# Airflow Orchestration — Implementation Plan

This plan replaces `src/dataone/orchestration/scheduler.py` (the `schedule` + `tenacity` daemon) with **Apache Airflow**, without changing what the pipeline computes, how it's partitioned, or any of its existing idempotency guarantees.

It's staged in three phases, each independently shippable and independently reversible:

- **Phase 0** — stand up Airflow alongside the existing scheduler, running in parallel, touching nothing production-critical.
- **Phase 1** — cut the *trigger* over to Airflow, at today's exact granularity (one task = the whole `make run-batch`), so the pipeline's internal behavior is provably unchanged.
- **Phase 2** — decompose the batch job into task-level stages at the pipeline's *existing* Iceberg checkpoints, gaining real per-stage retries, and fixing a genuine backfill-correctness bug as part of that decomposition (see 2.5).

A Phase 3 (lineage + alerting integration) is included at the end, scoped as optional/later.

> **Revision note:** Section 0.3/0.4 below were revised after comparing this plan against an equivalent Prefect-based proposal, which reuses the project's existing `x-spark-image` build directly for its worker rather than mounting the Docker socket. That's a better answer to the "how does the orchestrator invoke Spark without duplicating jar versions" question than this plan's original `DockerOperator` design — adopted here, with the one gap the Prefect plan left unaddressed (on-demand worker lifecycle) closed explicitly in 0.4.

---

## 0. Guiding constraints ("does not break the project")

- **The old scheduler is never deleted, only disabled.** `scheduler.py`, `retry.py`, and the `schedule` Makefile target stay in the repo through every phase. Cutover is a one-line change (which orchestrator is actually running); rollback is the same one-line change in reverse.
- **No business logic changes in Phase 0 or Phase 1.** Every function in `bronze_to_silver.py` keeps its current signature and behavior. Airflow is only changing *what process invokes* `make run-batch` — nothing about what that command does.
- **Phase 2's decomposition follows lines the pipeline already draws.** The task boundaries proposed below are the pipeline's existing Bronze → Silver → Gold → ClickHouse Iceberg checkpoints — not a redesign, a re-exposure of structure that's already there.
- **Minimal added footprint, matching this project's established pattern.** No new Postgres container (reuse the existing one, the same way Iceberg's JDBC catalog already does), no Celery/Redis (`LocalExecutor` is sufficient for a single-node batch DAG), and the CDC simulator / Structured Streaming job / NiFi flows are explicitly **out of scope** — they're long-running services, not batch tasks, and Airflow does not manage them.
- **The 68-test suite keeps passing unmodified through Phase 1.** Phase 2 adds new tests for the newly-introduced task boundaries; it does not need to rewrite existing ones, since the underlying functions are relocated, not rewritten.
- **Staged rollout, validated with real runs before cutover** — same discipline as the alerting-corrections plan: ship infrastructure, watch it run successfully in parallel, *then* flip the switch that makes it authoritative.

---

## Phase 0 — Stand up Airflow, in parallel, touching nothing

**Risk level: none.** This phase adds two containers and a database; it does not change what triggers today's batch job.

### 0.1 — Executor choice and why

Airflow's default multi-container footprint (`CeleryExecutor` + Redis + Flower) is exactly the kind of resource cost `docs/Prefect_Migration_Plan.md` already flagged as the reason a full DAG tool wasn't adopted originally — and that concern is still valid on this project's target hardware (Intel i7-6820HQ, 16GB RAM, already running Kafka + Spark + NiFi + ClickHouse + Grafana + Marquez concurrently).

**Use `LocalExecutor`, not `CeleryExecutor`.** `LocalExecutor` runs tasks as subprocesses of the scheduler itself — no Redis, no worker fleet, no Flower — while still giving real task-level parallelism (multiple tasks/DAG runs at once, bounded by `parallelism` and pool settings). For a single nightly DAG with a handful of sequential tasks, this is sufficient, and it keeps the total addition to **two containers** (`airflow-webserver`, `airflow-scheduler`) instead of four or five.

### 0.2 — Reuse the existing Postgres instance for Airflow's metadata DB

Following the exact pattern this project already uses for Iceberg's JDBC catalog (reusing `postgres` instead of standing up a Hive Metastore), Airflow's metadata database goes into a **second database on the same `postgres` container**, not a new container.

Add `infra/docker/postgres/init/003_airflow_db.sh` (a shell script, not `.sql`, since `CREATE DATABASE` can't run inside the single-transaction `.sql` init scripts Postgres already uses — this is the standard "multiple databases in one container" pattern):

```bash
#!/usr/bin/env bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflow_metadata;
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow;
EOSQL
```

`AIRFLOW_DB_PASSWORD` is a new `.env` variable (add to `.env.example` with a `changeme` placeholder, consistent with this project's existing credential-rotation convention). This runs once, automatically, on next fresh `postgres` volume init — **it does not touch or migrate the existing `customers`/`orders`/`products` schema or the Iceberg catalog tables**, since it's a separate `CREATE DATABASE`, not a change to the existing one. If the `postgres-data` volume already exists (i.e., this isn't a fresh environment), run the script's contents manually once via `docker exec -it dataone-postgres psql -U $POSTGRES_USER` instead — init scripts only run on first container start.

### 0.3 — New Docker Compose services

Add a new `airflow` profile (following the existing `core`/`batch` profile convention) so `make up` doesn't start Airflow by default until Phase 1's cutover.

**First, a new Dockerfile that layers Airflow onto the project's existing Spark image**, rather than the other way around — this is the corrected design (see the revision note above): `infra/docker/airflow/Dockerfile`

```dockerfile
# Reuses the exact, already-correct Spark + Iceberg + JDBC-jar image every
# other Spark service in this stack runs, instead of installing a second,
# independently-versioned Spark client inside an apache/airflow base image.
FROM dataone-spark:3.5.1-iceberg1.9.1

USER root
RUN pip install --no-cache-dir "apache-airflow==2.10.4" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
ENV AIRFLOW_HOME=/opt/airflow
RUN mkdir -p /opt/airflow && useradd -ms /bin/bash airflow_svc \
      && chown -R airflow_svc /opt/airflow
USER airflow_svc
```

**Validation flag before proceeding:** Airflow ships a strict, version-pinned constraints file specifically because it's known to conflict with other data-stack packages (protobuf, pydantic, and SQLAlchemy pin ranges are the usual culprits). Build this image and run `airflow db migrate` inside it as the very first step of Phase 0 — before writing any DAG — to confirm the constraint set resolves cleanly against whatever's already pinned in the base `dataone-spark` image's Python environment. If it doesn't, that's a Phase 0 blocker to resolve (typically a Python-version or venv-isolation fix), not something to discover mid-migration.

```yaml
  airflow-webserver:
    build:
      context: .
      dockerfile: infra/docker/airflow/Dockerfile
    profiles: [ "airflow" ]
    container_name: dataone-airflow-webserver
    networks: [ dataone-net ]
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "postgresql+psycopg2://airflow:${AIRFLOW_DB_PASSWORD}@postgres:5432/airflow_metadata"
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      PYTHONPATH: /opt/dataone/src
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/dataone/src:ro
    ports: [ "8081:8080" ]   # 8080 is already used elsewhere in this stack — confirm no collision at implementation time
    command: webserver
    mem_limit: "1g"
    cpus: "1.0"

  airflow-scheduler:
    build:
      context: .
      dockerfile: infra/docker/airflow/Dockerfile
    profiles: [ "airflow" ]
    container_name: dataone-airflow-scheduler
    networks: [ dataone-net ]
    depends_on:
      postgres:
        condition: service_healthy
      spark-master:
        condition: service_started
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "postgresql+psycopg2://airflow:${AIRFLOW_DB_PASSWORD}@postgres:5432/airflow_metadata"
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      PYTHONPATH: /opt/dataone/src
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/dataone/src:ro
      - lakehouse-data:/data/lakehouse
    command: scheduler
    mem_limit: "1.5g"
    cpus: "1.0"

  # See 0.4 — Option A (recommended default). Moved out of the on-demand
  # "batch" profile and into "airflow" so it's already registered with
  # spark-master by the time a task submits a job; no container lifecycle
  # management needed from inside Airflow at all.
  spark-worker-batch:
    profiles: [ "airflow" ]   # was: [ "batch" ] — see 0.4 for the trade-off this accepts
```

An `airflow-init` one-shot service (or a `make airflow-init` Makefile target running `airflow db migrate` + `airflow users create`) is needed once, before first start — add it as a third, `restart: "no"` service in the same profile, matching the existing `init-lakehouse` one-shot pattern already used elsewhere in this compose file. It should use the same `build:` block as the two services above, so it's exercising the exact image that'll actually run — this is also where the constraints-conflict validation from above gets run in practice, not just at first manual build time.

**Total added footprint, Option A (recommended): ~2.5GB for the two Airflow containers, plus `spark-worker-batch` moving from 0-when-idle to a steady 6GB (its existing `mem_limit`) since it's no longer stopped between runs.** That's a real, non-trivial cost on a 16GB laptop already running Kafka/NiFi/ClickHouse/Marquez/Grafana concurrently — stated plainly rather than glossed over; see 0.4 for the alternative that avoids it.

### 0.4 — How Airflow actually invokes Spark: native `spark-submit`, not `DockerOperator`

The original version of this plan proposed `DockerOperator` with the Docker socket mounted into the Airflow containers, to avoid `SparkSubmitOperator`'s problem: needing a second, independently-versioned Spark client + Iceberg/JDBC jars installed inside an `apache/airflow` base image, as a second place those versions could drift out of sync with the real `dataone-spark` image.

**The better fix, adopted here: don't add a second Spark client at all — build the Airflow container itself from the existing `dataone-spark` image** (0.3's Dockerfile). Since `LocalExecutor` runs tasks as subprocesses of the scheduler process, and the scheduler container *is* that Spark image with Airflow layered on top, a plain `BashOperator` running `spark-submit --master spark://spark-master:7077 ...` executes with the exact same binary and jars `spark-worker-batch` itself uses — no `docker exec`, no Docker socket, no second version to keep in sync. This mirrors the equivalent Prefect-based proposal's `<<: *spark-image` worker-reuse pattern, applied to Airflow's container instead of a Prefect work pool.

**The one thing that pattern alone doesn't solve — and the gap a from-scratch Prefect-style plan leaves open too — is `--deploy-mode client`'s need for at least one registered Spark *worker* with free executor slots.** Submitting the driver from inside the Airflow container doesn't make Airflow's container a worker; `spark-master` still needs somewhere to schedule executors. Today's `make run-batch` handles this by starting `spark-worker-batch` on demand and stopping it after — but doing that from inside Airflow would require exactly the sibling-container control (Docker socket or equivalent) this redesign is trying to remove. Two honest options, not a free lunch:

- **Option A (recommended, adopted in 0.3 above): move `spark-worker-batch` into the `airflow` profile as an always-up service.** Zero Docker-socket access anywhere in the Airflow deployment; every task is a plain subprocess call. Cost: `spark-worker-batch`'s 6GB `mem_limit` is now a steady-state cost instead of an on-demand one, since nothing stops it between nightly runs.
- **Option B (preserves the idle-resource savings, keeps a narrowly-scoped socket use): keep `spark-worker-batch` on the on-demand `batch` profile, and keep a Docker socket mount — used *only* for two `BashOperator` steps bracketing the actual work (`docker compose --profile batch up -d spark-worker-batch` before, `stop spark-worker-batch` after).** This is a materially narrower privilege than the original plan's `DockerOperator` design — it can start/stop two named services, not exec arbitrary commands into any container — but it isn't zero.

This plan defaults to **Option A** for the simplicity of "no Docker socket, full stop" being worth more than the RAM savings on a development machine; switch to Option B if the steady-state memory cost turns out to bite in practice. Either way, `spark-submit` itself now runs the same way for both options — natively, inside the Airflow-Spark image, never via `docker exec`.

### 0.5 — Validation

Bring up the `airflow` profile, log into the webserver UI, confirm the scheduler is healthy and the metadata DB migrated cleanly — **no DAGs exist yet**, so there is nothing to run. This phase is done when Airflow is observably healthy and completely inert with respect to the actual pipeline.


---

## Phase 1 — Cut the trigger over, at today's exact granularity

**Risk level: low.** One DAG, one task, calling the exact same command the scheduler calls today. If this task's behavior differs from today's scheduler in any way other than *what triggers it*, that's a bug in this phase, not an acceptable side effect.

### 1.1 — The DAG

`airflow/dags/dataone_nightly_batch.py`:

```python
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "dataone",
    "retries": 3,
    "retry_delay": timedelta(seconds=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=30),
}
# retries=3 + exponential backoff capped at 30s mirrors with_retry(max_attempts=3)
# in orchestration/retry.py exactly — same policy, now expressed as Airflow-native
# task config instead of a tenacity decorator.

SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
    "--deploy-mode client --driver-memory 512m --executor-memory 4000m "
    "--total-executor-cores 1"
)

@dag(
    dag_id="dataone_nightly_batch",
    schedule="0 2 * * *",           # matches SCHEDULED_RUN_TIME = "02:00" today
    start_date=...,                  # a fixed historical date, not datetime.now()
    catchup=False,                   # critical — see 1.2
    max_active_runs=1,               # the single Spark cluster can't usefully run two batches at once
    default_args=DEFAULT_ARGS,
    execution_timeout=timedelta(hours=2),   # matches BATCH_JOB_TIMEOUT_SECONDS exactly
    tags=["dataone", "batch", "phase-1"],
)
def dataone_nightly_batch():
    # Runs natively inside the airflow-scheduler container, which *is* the
    # dataone-spark image with Airflow layered on top (0.3/0.4) — the same
    # spark-submit binary and jars every other Spark service uses, no
    # docker exec, no second Spark client to keep in sync.
    run_batch = BashOperator(
        task_id="run_batch",
        bash_command=f"{SPARK_SUBMIT} /opt/dataone/src/dataone/batch/bronze_to_silver.py",
    )

dataone_nightly_batch()
```

### 1.2 — `catchup=False` is not optional

Airflow's default behavior — backfilling every missed scheduled interval between `start_date` and now on first deploy — would, if left on, retroactively fire the batch job for every day since `start_date`. `catchup=False` makes Airflow behave like the current scheduler (only future scheduled runs execute); this is a correctness requirement for a safe cutover, not a style preference.

### 1.3 — Why this task is provably equivalent to today's scheduler

- Same command (`spark-submit` against `bronze_to_silver.py`, no arguments — identical to `make run-batch` with no `START_DATE`/`END_DATE` set), now run natively instead of via `docker exec`, with identical driver/executor resource flags.
- Same retry semantics (3 attempts, exponential backoff capped at 30s — a direct translation of `with_retry(max_attempts=3, exceptions=(BatchJobFailed,))`).
- Same timeout (2 hours, matching `BATCH_JOB_TIMEOUT_SECONDS`), with Airflow marking the task `failed` on timeout — behaviorally equivalent to the current code's "log and abandon" (Airflow additionally *shows* this state in its UI, which is a strict improvement in observability, not a behavior change).
- Same schedule (02:00 daily).
- The underlying `bronze_to_silver.py` is untouched — same file, same single monolithic `main()`, same Iceberg writes, same idempotency.
- **One deliberate, stated difference**: `spark-worker-batch` is no longer started/stopped around each run (0.3/0.4, Option A) — it's already up as part of the `airflow` profile. The job itself behaves identically; only the worker's idle-vs-always-on lifecycle changed, and that trade-off is scoped and justified in 0.4, not incidental.

### 1.4 — Cutover sequence

1. Deploy Phase 0 + this DAG with the DAG **paused** (Airflow's default state for a newly-deployed DAG) — the old `scheduler` service keeps running unaffected.
2. Manually trigger the DAG once via the UI or CLI (`airflow dags trigger dataone_nightly_batch`) during a low-stakes window; confirm it produces byte-for-byte the same kind of run the scheduler produces (check Iceberg row counts, `gold.quarantine_summary`, ClickHouse sync — the same validation checklist already used for the alerting-corrections rollout).
3. Once confirmed, **stop the `schedule` Makefile target / scheduler process** (whatever currently keeps it running — a systemd unit, a `make schedule &` in a terminal, or a Docker service, depending on how it's deployed) and **unpause the Airflow DAG**. This is the actual cutover moment; do these two actions together, not separately, so there's never a window with both or neither running.
4. Leave `scheduler.py` in the repo, unused, for at least one full release cycle before considering removing it in a later cleanup PR.

### 1.5 — Rollback

Pause the Airflow DAG, restart the old scheduler process. Since neither phase touched `bronze_to_silver.py`, there is no data-layer rollback needed — this is purely "which process is calling `make run-batch` on a timer," and reverting that is immediate.

---

## Phase 2 — Decompose into stage-level tasks at existing Iceberg checkpoints

**Risk level: medium.** This is the phase that actually earns Airflow's value over the old scheduler — per-stage retries, per-stage visibility, and a DAG structure that documents the pipeline's real dependency graph instead of hiding it inside one opaque `spark-submit` call. It's scoped to follow boundaries the code already has, not invent new ones.

### 2.1 — Where the natural boundaries already are

`bronze_to_silver.py`'s `main()` already writes to Iceberg at these points, in this order — meaning the "hand-off" between stages is already a durable, re-readable checkpoint, not something this refactor has to invent:

1. **Ingest & Quality Gate**: Bronze read → CDC parse/dedupe → `run_quality_gate()` across all 7 datasets → writes `silver.*` (customers, orders, clickstream, reviews) and `quarantine.*`.
2. **SCD2 Merge**: reads `silver.customers` → `apply_scd2_merge()` → writes `gold.dim_customer`.
3. **Gold Marts**: reads `silver.*` + `gold.dim_customer` (+ NiFi-sourced `gold.dim_campaign`) → builds the star schema fact and the 9 marts → writes `gold.*`.
4. **ClickHouse Sync**: **already, today, re-reads every table from Iceberg** (`gold_from_iceberg = {name: spark.read... for name in gold}`) before calling `load_clickhouse_marts()` — this is the clearest pre-existing seam in the whole file; the code already treats this as a separate stage internally, this refactor just makes that boundary a real task boundary too.

### 2.2 — The refactor: one entrypoint, `--stage` argument, no logic changes

Add a `--stage` argument to `bronze_to_silver.py` (`{ingest, scd2, gold, clickhouse_sync}`, default: run all four in sequence — so the existing no-argument `make run-batch` behavior, and Phase 1's `BashOperator` call, keep working completely unchanged). Each stage's body is **the existing code, unmodified, sliced along the boundaries above** — this is a function-extraction refactor, not a rewrite:

```python
def run_stage_ingest(spark: SparkSession, batch_date: date) -> None:
    """Bronze -> parse/dedupe -> quality gate -> silver + quarantine writes.
    Exactly today's main() body up through the quarantine_summary write."""
    ...

def run_stage_scd2(spark: SparkSession, batch_date: date) -> None:
    """silver.customers -> SCD2 merge -> gold.dim_customer."""
    ...

def run_stage_gold(spark: SparkSession, batch_date: date) -> None:
    """silver.* + gold.dim_customer -> star schema + 9 marts -> gold.* writes."""
    ...

def run_stage_clickhouse_sync(spark: SparkSession, batch_date: date) -> None:
    """Re-read gold.* from Iceberg -> JDBC sync to ClickHouse. Already how
    today's code does this step — unchanged."""
    ...
```

Each stage gets **its own `SparkSession`** (via the existing `build_spark_session()` helper) rather than sharing one across the whole run. This is the honest cost of decomposition: four short-lived JVM driver startups instead of one long-lived one, adding roughly 10-20 seconds of cold-start overhead per stage. In exchange: a failure in `clickhouse_sync` no longer requires re-running the SCD2 merge and gold-marts computation to retry — it retries in place, against data that's already durably written to Iceberg. For a nightly batch job with a multi-hour timeout budget, this trade is worth it; it would not be worth it inside a tight, low-latency loop, which is exactly why the *streaming* job is correctly left outside this refactor entirely.

### 2.3 — The decomposed DAG

```python
@dag(
    dag_id="dataone_nightly_batch",
    schedule="0 2 * * *",
    start_date=...,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dataone", "batch", "phase-2"],
)
def dataone_nightly_batch():
    def spark_stage(stage_name: str, timeout: timedelta) -> BashOperator:
        # Native spark-submit, same as Phase 1 (0.4) — no docker exec, no socket.
        return BashOperator(
            task_id=f"stage_{stage_name}",
            bash_command=(
                f"{SPARK_SUBMIT} /opt/dataone/src/dataone/batch/bronze_to_silver.py "
                f"--stage {stage_name} "
                "--logical-date {{ ds }}"     # see 2.5 — passed explicitly, not derived from datetime.now()
            ),
            execution_timeout=timeout,
        )

    ingest = spark_stage("ingest", timedelta(minutes=45))
    scd2 = spark_stage("scd2", timedelta(minutes=20))
    gold = spark_stage("gold", timedelta(minutes=30))
    clickhouse_sync = spark_stage("clickhouse_sync", timedelta(minutes=15))

    ingest >> scd2 >> gold >> clickhouse_sync

dataone_nightly_batch()
```

Per-stage timeouts sum to comfortably under the existing 2-hour ceiling, while each stage now fails fast on its own budget instead of all sharing one undifferentiated 2-hour allowance.

### 2.4 — Retry semantics per stage

`ingest` (the quality gate + widest fan-in of sources) is the stage most likely to see a transient failure (e.g. a slow Postgres CDC read) — give it the full 3-attempt policy. `clickhouse_sync` is a pure JDBC write of already-computed data — also worth 3 attempts, since a transient ClickHouse connection blip shouldn't fail the whole night's run. `scd2` and `gold` are pure-Iceberg reads/writes with no external network dependency beyond the lakehouse itself — 2 attempts is enough headroom without masking a real bug behind repeated retries. All four keep the same exponential-backoff shape already established in Phase 1's `DEFAULT_ARGS`.

### 2.5 — A real bug this decomposition fixes: `date.today()` vs. the logical date

Several functions in the pipeline (and the `quality_gate_summary` addition from the alerting-corrections plan) compute "today" via `date.today()` at run time. That's silently correct for the nightly on-schedule run, but **wrong for a backfill**: a backfill re-running February 3rd's batch on July 13th would stamp every row with `batch_date = July 13`, not `February 3`, corrupting exactly the kind of historical analysis the backfill was meant to reproduce.

Airflow's `{{ ds }}` Jinja template (the DAG run's *logical date*, not wall-clock time) is the standard, idiomatic fix for this class of bug, and this decomposition is the natural point to apply it: every stage receives `--logical-date {{ ds }}` explicitly (as shown in 2.3), and every function currently calling `date.today()` for a `batch_date`/`sales_date` column is changed to accept that date as a parameter instead. This is flagged here as an explicit, scoped fix — not silently bundled in — because it changes behavior for backfilled dates specifically (nightly runs, where `{{ ds }}` and `date.today()` coincide, are unaffected).

### 2.6 — The backfill DAG

`backfill.py`'s existing `--start`/`--end`/`--chunk-days` CLI has its own chunking logic tailored to this pipeline and does not map cleanly onto Airflow's native catchup/backfill mechanism (which operates strictly per-schedule-interval). Rather than force-fitting it, add a **second, manually-triggered DAG** that wraps the existing CLI as-is:

```python
@dag(
    dag_id="dataone_backfill",
    schedule=None,          # manual-trigger only, via `airflow dags trigger --conf`
    start_date=...,
    catchup=False,
    tags=["dataone", "batch", "backfill"],
)
def dataone_backfill():
    BashOperator(
        task_id="run_backfill",
        bash_command=(
            "python -m dataone.batch.backfill "
            "--start {{ dag_run.conf['start'] }} --end {{ dag_run.conf['end'] }} "
            "--chunk-days {{ dag_run.conf.get('chunk_days', 7) }}"
        ),
        execution_timeout=timedelta(hours=4),   # matches BACKFILL_JOB_TIMEOUT_SECONDS
    )

dataone_backfill()
```

Triggered with `airflow dags trigger dataone_backfill --conf '{"start": "2026-02-01", "end": "2026-02-07"}'`. This keeps `backfill.py`'s already-correct chunking/idempotency logic completely intact (see 2.5's date-parameter fix applied inside it) while giving backfills the same Airflow-native visibility, retry, and audit trail as the nightly DAG.

### 2.7 — Tests to add

Following this project's existing testing philosophy (`docs/TESTING_GUIDE.md`) and the regression-test pattern already used in `tests/test_alerting_rules.py`:

- **`tests/test_dag_integrity.py`** (no marker needed — pure DAG-parsing, no Spark/Iceberg required) — the standard Airflow best-practice test: import every DAG file and assert zero import errors, assert no cycles, assert every task has `retries` and `execution_timeout` set (catches someone adding a new task without thinking about failure handling), assert `catchup=False` on every DAG (regression guard against the exact backfill-storm risk in 1.2).
- **`tests/test_bronze_to_silver_stages.py`** (`@pytest.mark.spark` / `@pytest.mark.iceberg`, mirroring existing markers) — for each of the four extracted `run_stage_*()` functions, assert it can run standalone against a small fixture Iceberg catalog and produces the same output a full `main()` run produces for the same input — the regression guard that the extraction didn't silently change behavior.
- **`tests/test_logical_date_propagation.py`** — a small, fast, marker-free test asserting that `run_stage_ingest()` (and friends) use the passed-in `logical_date` parameter for every `batch_date`/`sales_date` column, not `date.today()` — a regression guard for the 2.5 fix specifically, in the same spirit as the existing `test_kafka_lag_metric_uses_the_real_exporter_name` "guard a previously-fixed mistake" pattern.
- Existing tests in `tests/test_validators.py`, `tests/test_scd2_customer_dim.py`, etc. are untouched — they test the underlying functions directly and don't care which process calls them.

### 2.8 — Rollout & validation sequence

1. Ship the `--stage` refactor to `bronze_to_silver.py` alone first, with **no DAG changes yet** — confirm `make run-batch` (no `--stage` argument, i.e. Phase 1's existing behavior) still produces identical output, since the no-argument path runs all four stages in sequence exactly as `main()` does today.
2. Ship the decomposed DAG (2.3) *paused*; manually trigger it once, and compare its output against a same-day `make run-batch` run on a scratch environment — row counts, quarantine summary, ClickHouse marts should match exactly.
3. Ship the backfill DAG (2.6) *paused*; manually trigger it against a small already-known date range and diff its output against the existing `backfill.py` CLI's output for that same range run directly (outside Airflow) — this validates 2.5's date-propagation fix didn't change anything for a range where `date.today()` and the logical date would have coincided anyway... then re-run it against an *older* date range specifically to confirm the fix (`batch_date` in the output now matches the backfilled date, not today).
4. Unpause both DAGs, disable the old scheduler for good (if not already done in Phase 1).

### 2.9 — Rollback

Both DAGs can be paused independently. `bronze_to_silver.py --stage` defaults to running all four stages in sequence with no arguments, so `make run-batch` and Phase 1's `BashOperator` call both continue to work exactly as before even after this refactor ships — meaning Phase 1's cutover point remains a valid fallback throughout all of Phase 2, not just before it.

---

## Phase 3 — Optional follow-ups (not required for a working migration)

- **OpenLineage integration**: `apache-airflow-providers-openlineage` would emit DAG/task-level lineage into the *same* Marquez instance and Kafka transport (`openlineage-events` topic) the Spark jobs already use — giving job-orchestration-level lineage alongside the existing column-level Spark lineage, in one Marquez namespace rather than two competing ones. Scoped as follow-up because it requires confirming namespace-collision behavior between Airflow's and Spark's OpenLineage emitters before enabling both.
- **Grafana/Prometheus integration**: Airflow supports a StatsD metrics exporter; piped through `statsd_exporter` into the existing `prometheus` service, DAG success-rate and task-duration metrics could join the "DataOne Alerts" Grafana folder alongside the quarantine-rate and (once fixed) streaming-health alerts from the alerting-corrections plan — one more signal in the same place, not a new dashboard tool.
- **Removing `scheduler.py`/`retry.py` entirely**: only once Phase 2 has run unattended and successfully for a stated minimum period (recommend at least two full weeks of nightly runs, covering at least one real backfill), as a dedicated cleanup PR — not bundled into this migration.

---

## Data engineering best practices checklist

- [ ] **Idempotency preserved end-to-end** — every task's underlying write is still a partition-overwrite or Iceberg `MERGE INTO`; Airflow retries are safe to re-run for exactly this reason, at both the whole-job (Phase 1) and per-stage (Phase 2) granularity.
- [ ] **`catchup=False` on every DAG** — no accidental historical backfill storm on deploy (1.2), enforced by a regression test (2.7).
- [ ] **Explicit logical date, never wall-clock time, inside task logic** — `{{ ds }}` threaded through explicitly (2.5), fixing a real backfill-correctness bug rather than just carrying it forward into the new orchestrator.
- [ ] **Per-task retries and timeouts, not one undifferentiated job-level allowance** — Phase 2 gives each stage a retry count and timeout sized to its own risk profile (2.4), instead of Phase 1's coarser whole-job policy.
- [ ] **`max_active_runs=1`** — prevents two overlapping DAG runs from both trying to drive the single shared Spark cluster at once, a real resource-contention risk on this hardware that the old single-threaded `schedule` daemon avoided only by accident (it physically couldn't run two jobs at once).
- [ ] **No heavy imports or DB/network calls at DAG *parse* time** — every DAG file above only imports operators and defines the DAG/task graph; all Spark/Iceberg/Postgres work happens inside task callables (`BashOperator` commands), never at module import time. This is a standard Airflow correctness requirement (the scheduler re-parses every DAG file on a short interval) and is the reason none of the DAG files above import anything from `dataone.batch` or `dataone.utils` directly.
- [ ] **No Docker-socket access anywhere in the deployment** (Option A, 0.4) — every task is a plain subprocess of the scheduler process, running inside an image that already has the correct Spark/Iceberg/JDBC jars baked in; there is no sibling-container control surface to reason about at all.
- [ ] **Secrets via Airflow Connections/Variables, not hardcoded** — `AIRFLOW_DB_PASSWORD` and any ClickHouse/Postgres credentials the DAGs need are set as Airflow Connections at deploy time, mirroring this project's existing `.env`-based credential rotation discipline rather than embedding them in DAG files.
- [ ] **DAG-level regression tests in CI**, matching the project's existing pattern of guarding previously-fixed mistakes with a named test (2.7).
- [ ] **Additive, staged rollout with the old system left in place as a fallback at every step** — never a same-PR delete-and-replace, consistent with the alerting-corrections plan's rollout discipline.
