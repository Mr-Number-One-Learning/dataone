# Replacing the Custom Orchestrator with Prefect (Local) — Implementation Plan

## Fit check — does Prefect actually fit this project?

**Yes, with one honest caveat below.** Three things about the current codebase make Prefect a low-friction fit rather than a forced one:

1. Orchestration here is already plain Python — `scheduler.py` is a `schedule`-library loop that shells out to `spark-submit` via `subprocess.run()`, and `retry.py` is a tenacity decorator. Both map almost directly onto Prefect's `@flow`/`@task` + `retries=` model. There's no DAG-authoring paradigm shift required, unlike adopting Airflow or Dagster.
2. Postgres is already a first-class citizen in this stack (`_pipeline_runs` metadata table, CDC watermark table), so Prefect's own requirement for a Postgres backend isn't new infrastructure philosophy — just a new database inside a service you already run.
3. The existing `x-spark-image` build (Iceberg + Postgres/ClickHouse JDBC jars already layered in) can be reused as-is for the Prefect worker, via the same YAML merge key every other Spark service uses. No new image to design from scratch.

**The caveat:** `bronze_to_silver.py` runs its entire nightly job — ingest, quality gate, SCD2 merge, gold marts, ClickHouse load — as **one Spark session inside one `spark-submit` process**. Swapping the orchestrator alone does not, by itself, give you step-level retry granularity; it gives you the same single-task granularity you have today, just on better infrastructure. Getting real step-level retries (e.g., retrying only the ClickHouse load without re-running the whole Spark job) requires also splitting `bronze_to_silver.py` into separately invocable stages — that's included below as an explicit optional phase, not bundled into the core migration, so scope stays honest.

---

## Phase 1 — Add Prefect infrastructure to `docker-compose.yml`

Prefect 3.x self-hosted needs: Postgres, Redis (new in 3.x, required for the events/messaging layer), the `prefect-server` API/UI, and a worker.

**Reuse the existing `dataone-postgres` container** rather than adding a second Postgres — it already has spare `max_connections` headroom worth checking (currently capped at 50; bump to ~80–100 once Prefect's pool is added), and this project already treats Postgres as the metadata home for exactly this kind of state (watermarks, pipeline runs).

```yaml
# infra/docker/postgres/init/02_prefect_db.sql  (new file, runs on container init)
CREATE DATABASE prefect;
```

```yaml
# docker-compose.yml additions

  prefect-redis:
    image: redis:7-alpine
    profiles: [ "core" ]
    container_name: dataone-prefect-redis
    networks: [ dataone-net ]
    mem_limit: "256m"

  prefect-server:
    image: prefecthq/prefect:3-latest
    profiles: [ "core" ]
    container_name: dataone-prefect-server
    networks: [ dataone-net ]
    depends_on:
      postgres:
        condition: service_healthy
      prefect-redis:
        condition: service_started
    ports:
      - "4200:4200"
    environment:
      PREFECT_API_DATABASE_CONNECTION_URL: "postgresql+asyncpg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/prefect"
      PREFECT_SERVER_API_HOST: "0.0.0.0"
      PREFECT_MESSAGING_BROKER: "prefect_redis.messaging"
      PREFECT_MESSAGING_CACHE: "prefect_redis.messaging"
      PREFECT_REDIS_MESSAGING_HOST: "dataone-prefect-redis"
    command: prefect server start --host 0.0.0.0
    mem_limit: "512m"

  prefect-worker:
    <<: *spark-image          # reuses the existing Iceberg/JDBC-enabled Spark client image
    profiles: [ "core" ]
    container_name: dataone-prefect-worker
    networks: [ dataone-net ]
    depends_on:
      prefect-server:
        condition: service_started
    environment:
      PREFECT_API_URL: "http://dataone-prefect-server:4200/api"
      PYTHONPATH: "/opt/dataone/src"
    volumes:
      - ./src:/opt/dataone/src
      - ./flows:/opt/dataone/flows
    entrypoint: ["bash", "-c"]
    command: >
      "pip install --no-cache-dir prefect &&
       prefect work-pool create dataone-process-pool --type process --overwrite &&
       prefect worker start --pool dataone-process-pool"
    mem_limit: "512m"
```

Using a **Process** work pool (not Docker) for the worker means flow runs execute directly inside this same container — no Docker-socket mounting, no per-deployment image builds. It's the closest match to what `scheduler.py` already does.

---

## Phase 2 — Convert the existing entry points into flows/tasks

Minimal-diff approach: wrap the *existing* `main()` functions, don't rewrite their internals yet.

```python
# flows/nightly_batch.py
import subprocess
from prefect import flow, task

from dataone.batch.backfill import BATCH_JOB_PATH


class BatchJobFailed(RuntimeError):
    pass


@task(retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_bronze_to_silver(start: str | None = None, end: str | None = None) -> None:
    cmd = ["spark-submit", BATCH_JOB_PATH]
    if start and end:
        cmd += ["--start", start, "--end", end]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise BatchJobFailed(f"spark-submit exited {result.returncode}")


@flow(name="nightly-batch")
def nightly_batch() -> None:
    run_bronze_to_silver()


@flow(name="backfill")
def backfill(start: str, end: str) -> None:
    """On-demand, parameterized — replaces running backfill.py by hand."""
    run_bronze_to_silver(start=start, end=end)


if __name__ == "__main__":
    nightly_batch.serve(name="nightly", cron="0 2 * * *")
```

```python
# flows/cdc_poll.py
import psycopg2
from prefect import flow, task

from dataone.ingestion.cdc_simulator import poll_once, get_watermark  # existing functions, unchanged

TABLES = [("customers", "customer_id"), ("orders", "order_id")]


@task(retries=3, retry_delay_seconds=10, retry_jitter_factor=0.3)
def poll_table(table: str, pk_column: str) -> int:
    conn = psycopg2.connect(...)  # same connection setup as cdc_simulator.py today
    try:
        return poll_once(conn, table, pk_column)
    finally:
        conn.close()


@flow(name="cdc-poll")
def cdc_poll() -> None:
    for table, pk in TABLES:
        poll_table(table, pk)


if __name__ == "__main__":
    cdc_poll.serve(name="cdc-poll", interval=30)  # matches today's POLL_INTERVAL_SECONDS
```

**Design note on the CDC poller:** today it's one long-running process with an internal `while True: poll_once(); sleep(30)`. The Prefect-native pattern above replaces that internal loop with Prefect's own interval scheduler firing a short flow every 30 seconds instead. This trades "one invisible long-running process" for "many short, individually visible flow runs in the UI" — genuinely better crash visibility (you can see exactly which poll cycle failed), at the cost of more run history to look at. Worth doing, but it's a real behavior change, not just a wrapper.

---

## Phase 3 — Deployments (replaces `schedule` + `scheduler.py` entirely)

| Deployment | Trigger | Replaces |
|---|---|---|
| `nightly` | `cron="0 2 * * *"` | `SCHEDULED_RUN_TIME = "02:00"` + the `schedule.every().day.at(...)` line |
| `cdc-poll` | `interval=30` (seconds) | `POLL_INTERVAL_SECONDS` while-loop in `cdc_simulator.run()` |
| `backfill` | none — manual trigger only | running `python -m dataone.batch.backfill --start ... --end ...` by hand |

Manual backfill runs become:
```bash
prefect deployment run 'backfill/backfill' --param start=2026-01-01 --param end=2026-01-31
```

---

## Phase 4 — Retire the old code

- [ ] Delete `src/dataone/orchestration/scheduler.py`
- [ ] Remove `schedule==1.2.2` from `requirements.txt`
- [ ] Add `prefect==3.*` to `requirements.txt`
- [ ] Decide on `retry.py`: `with_retry` is used in exactly three places today — `scheduler.py` (deleted), `cdc_simulator.poll_once` (replaced by the task-level `retries=` above), and `reviews_generator`'s HTTP call. If the reviews generator stays a standalone seed script outside Prefect's scope, keep `retry.py` + `tenacity` just for that one call site; otherwise fold it into a Prefect task too and drop the dependency entirely.
- [ ] Update any remaining docs that describe the custom scheduler as a deliberate stand-in "since none is in the allowed toolset"; that framing should be corrected once a real tool is in place.

---

## Phase 5 (optional / stretch) — Real step-level retry

Only pursue this if the coarse "retry the whole nightly job" granularity actually bites you. It means splitting `bronze_to_silver.py`'s `main()` into separately callable stages (e.g., a `--stage` CLI flag: `ingest`, `quality-and-scd2`, `gold-marts`, `clickhouse-load`), each its own `spark-submit` invocation and its own Prefect task with its own `retries=`. This is a real refactor of the batch job itself, not an orchestrator swap — worth its own separate plan if you want to go there, since it touches `bronze_to_silver.py`'s internals rather than just what calls it.

---

## Phase 6 — Cutover checklist

- [ ] Bring up `prefect-redis`, `prefect-server`, `prefect-worker` alongside the existing stack (`docker compose --profile core up -d`)
- [ ] Confirm Prefect UI reachable at `localhost:4200`
- [ ] Register the three deployments (`nightly`, `cdc-poll`, `backfill`)
- [ ] Run `nightly` manually once via the UI before trusting the cron schedule; confirm Iceberg tables and ClickHouse marts update exactly as they do today
- [ ] Let `cdc-poll` run for a full day alongside the old `cdc_simulator.run()` process **stopped** (don't double-poll — same watermark table, would race) and confirm watermark advances correctly
- [ ] Kill ClickHouse mid-load once, on purpose, and confirm the `run_bronze_to_silver` task actually retries per its `retries=3` setting
- [ ] Only after a few clean nightly cycles: remove the old manual `python -m dataone.orchestration.scheduler` invocation from wherever it's currently started (it isn't in `docker-compose.yml` today — check the `Makefile` / README / your own shell history for where it's actually launched from)
