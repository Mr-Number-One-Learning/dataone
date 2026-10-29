# Implementation plan: DataOne hardening fixes

## Context
A project assessment found a strong medallion lakehouse with gaps in automation, reproducibility, secrets hygiene and one oversized module (`src/dataone/batch/bronze_to_silver.py`, ~1490 lines). This plan implements the fixes as small, independently reviewable PRs.

Verified while planning:
- **Dropped:** "ClickHouse append duplicates". `load_clickhouse_marts` (`bronze_to_silver.py:906-945`) already truncates a staging table and swaps it in via `EXCHANGE TABLES`, so retries are safe. No change needed.
- **Confirmed:**
  - `config.py:128,141,164,172` fall back to `"changeme"`.
  - `docker-compose.yml:416,437` fall back to `marquez_password`.
  - Compose images `vchernikh/tabix:latest` (:395) and `prefecthq/prefect:3-latest` (:488) are unpinned.
  - `pyproject.toml` declares no dependencies.
  - There is no `.github/` and no pre-commit config.
  - `bronze_to_silver.py` has 10 `except Exception` sites (lines 502, 901, 937, 943, 983, 1063, 1167, 1266, 1295, 1483).
- **Unverified (check first in PR 6):** duplicated `write_overwrite_partitions` blocks around lines 1089-1145 and 1329-1433.

## PR sequence (each is independent unless noted)

### PR 1: CI and pre-commit
- Add `.github/workflows/ci.yml`:
  - Job `lint-test`: Python 3.10/3.11, install `requirements*.txt` plus `pip install -e .`, then `make lint` and `make test`.
  - Job `spark-tests` (nightly cron plus manual dispatch): JDK 17, `make test-spark` and `make test-iceberg`.
- Add `.pre-commit-config.yaml` with ruff and black, reusing the existing `[tool.ruff]` and `[tool.black]` config in `pyproject.toml`.
- Reuse the Makefile `lint`/`test` targets so CI and local runs match.

### PR 2: Secrets and credentials
- `config.py`: replace the `os.getenv("X_PASSWORD", "changeme")` fallbacks (lines 128, 141, 164, 172) with a small `_require_env(name)` helper that raises a clear error when unset. Keep `.env.example` as the documented source of defaults. Check `tests/` for tests that rely on the fallbacks and set env vars in `conftest.py`.
- `docker-compose.yml`: use `${MARQUEZ_PASSWORD:?set MARQUEZ_PASSWORD in .env}` at lines 416 and 437.
- `src/nifi_flows/Reviews_API_Ingestion.json`: replace the inline Mongo URI password with a NiFi parameter, or sanitize the committed value to a placeholder and document the post-import step in the NiFi guide.
- Stop embedding passwords in URLs (`config.py` ~132 and 145, compose ~211, 387, 500) by passing user and password separately where the client supports it.
- Bind published ports to `127.0.0.1:` in `docker-compose.yml`.
- Rotate the real values in the local `.env`, which is currently identical to `.env.example`.

### PR 3: Dependency consolidation
- Move runtime dependencies from `requirements.txt` into `[project.dependencies]` in `pyproject.toml`. Add `[project.optional-dependencies] dev` from `requirements-dev.txt`.
- Compare against `infra/docker/spark/requirements-spark.txt`, then either generate it from the same source or keep it as an explicit subset with a comment.
- Generate a lockfile with `uv pip compile` (or `pip-tools`), commit it, and install from it in CI and the Spark Dockerfile.
- Remove the unused `prometheus-client` and pick one Kafka client after grepping imports for `kafka` / `confluent_kafka`.
- Pin `vchernikh/tabix` and `prefecthq/prefect` to explicit tags.
- Keep `requirements*.txt` as thin generated files, or delete them and update README, Makefile and CONTRIBUTING.

### PR 4: Repo hygiene
- `.gitignore`:
  - Add `data/lakehouse/**` (keeping `.gitkeep`), `.hadoop/` and `jdk.zip`.
  - Fix the `*.docs` typo.
  - Decide on the `*.csv` rule. Grep NiFi flows and docs for CSV inputs and either create `data/seed/` or add an explicit exception.
- Move `src/test_bronze.py` and `src/query_dlq.py` to `scripts/` (replacing `print` with structlog, and removing the hardcoded table count).
- `git rm` `.agents/workflows/...`.
- Rename the `Gemini_Generated_Image_...` file under `docs/images` to a slug and update references.
- Makefile:
  - Add a confirmation prompt to `make clean` (`down -v`).
  - Parametrize the container name and Spark memory settings via variables.
- Make `scripts/run_batch_job.sh` call `make run-batch`, or delete it.
- Prefect tasks: replace `log_prints=True` reliance with structlog calls.

### PR 5: Metadata and quality verification
- Add a test (or CI step) that runs `scripts/generate_metadata.py` into a temp dir and diffs against committed `metadata/`.
- Generate the missing `metadata/ownership/silver.campaigns.json` through that script.
- Optional, later: freshness and row-volume checks in `quality/validators.py`, surfaced in the existing `gold.quality_gate_summary`. Evaluating Great Expectations or Soda is a separate decision.

### PR 6: Refactor bronze_to_silver.py (last, after CI and tests are in place)
1. Verify the duplicated code paths and add characterization tests under the `spark`/`iceberg` markers before moving anything.
2. Narrow the `except Exception` blocks at 502 (sentiment), 983, 1063, 1167, 1266, 1295 and 1483 to specific exceptions, or log and re-raise. Lines 901, 937 and 943 already re-raise or are guarded cleanup, so leave them.
3. Split into modules under `src/dataone/batch/` (for example `silver_orders.py`, `silver_reviews.py`, `silver_clickstream.py`, `gold.py`, `clickhouse_sync.py`) with one shared `write_overwrite_partitions` path. Keep `bronze_to_silver.py` as a thin orchestrator so `nightly_batch.py`, `backfill.py` and tests keep their imports.
4. Do it in several commits, one module extracted per commit.

### PR 7: MinIO object storage for the Iceberg warehouse
Today the warehouse is the local Docker volume `lakehouse-data` mounted at `/data/lakehouse` (`docker-compose.yml`), addressed as `file://` + `ICEBERG_WAREHOUSE_PATH` (`config.py:185`, `utils/iceberg_helpers.py:49`). It is single-host and `make clean` deletes it. Move it to S3-compatible storage with MinIO.

Do this after PR 2 (secrets) and PR 3 (dependencies), so MinIO credentials are required env vars and the S3 jars are pinned.

1. **Compose:** add a `minio` service (pinned image tag, healthcheck, named volume `minio-data`, API on `127.0.0.1:9000`, console on `127.0.0.1:9001`) and a one-shot `minio-init` service using `mc` to create the `lakehouse` bucket. Credentials come from `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` in `.env` and `.env.example`, with no default fallback (see PR 2).
2. **Config:** add a `MinioConfig` in `config.py` (endpoint, access key, secret key, bucket) using the `_require_env` helper from PR 2. Change `ICEBERG_WAREHOUSE_PATH` to `s3a://lakehouse/warehouse`.
3. **Catalog:** in `utils/iceberg_helpers.py:49`, stop hardcoding the `file://` prefix and use the configured URI scheme. Add the S3A settings to the Spark session: `spark.hadoop.fs.s3a.endpoint`, `.access.key`, `.secret.key`, `.path.style.access=true`, `.connection.ssl.enabled=false` for local MinIO, and `fs.s3a.impl`.
4. **Jars:** add `hadoop-aws` and `aws-java-sdk-bundle` matching the Spark 3.5.1 Hadoop version to `infra/docker/spark/Dockerfile` (baked into the image, not fetched at runtime), pinned alongside the Iceberg 1.9.1 jar.
5. **Hardcoded local paths to migrate or keep:**
   - `streaming/structured_streaming_job.py:38` `CHECKPOINT_BASE = "/data/lakehouse/checkpoints"` should become `s3a://lakehouse/checkpoints` via config.
   - `batch/bronze_to_silver.py:49-50` `STAGING_CAMPAIGNS_DIR` and `ARCHIVED_CAMPAIGNS_DIR` are a landing zone, probably written by NiFi. Check how NiFi writes there first. Either keep them on a local volume, or move to a bucket prefix and change the NiFi processors (and the archive step, which becomes copy + delete on S3).
   - `init-lakehouse` service and the `lakehouse-data` volume can be removed once nothing mounts them.
6. **Migration of existing data:** this is a dev project, so the default is to re-seed and re-run (`make seed`, then batch). Optional: `mc mirror` the old volume into the bucket, but Iceberg metadata files embed absolute `file://` paths, so a plain copy will not work. Use Iceberg's `register_table` / `rewrite_table_path` procedures, or just rebuild.
7. **Observability and docs:** add MinIO to the Prometheus scrape config (it exposes `/minio/v2/metrics/cluster`), update the architecture diagram, `docs/SEQUENCE_DIAGRAMS.md`, README, the infrastructure guide, and `.env.example`.
8. **Tests:** keep unit tests on a local path. Parametrize the `iceberg` marker fixtures in `tests/conftest.py` so the warehouse stays local by default, and add one opt-in `minio` marker test that runs against the compose MinIO (nightly CI job with a MinIO service container).

Risks:
- S3A and Iceberg jar version mismatches cause cryptic `ClassNotFound` or `NoSuchMethod` errors. Pin and test them together.
- `EXCHANGE TABLES`, MERGE and `overwritePartitions` behave the same on S3, but atomic directory renames do not exist on S3. Iceberg's metadata-pointer commits avoid this, but any code that lists or renames files directly (the campaigns archive step) needs review.
- Table commit concurrency still relies on the JDBC catalog in Postgres, so that stays.

## Verification
- Every PR: `make lint` and `make test` pass locally, and the CI job passes once PR 1 lands.
- PR 2: start the stack with `.env` missing a password and confirm a clear failure. Start it with real values and confirm Postgres, Mongo, ClickHouse, NiFi and Marquez connect. Confirm `docker compose config` shows ports bound to 127.0.0.1.
- PR 3: fresh venv install from the lockfile, then `pytest`. Build the Spark image (`docker compose build`).
- PR 4: `git status` shows lakehouse output is ignored. `make clean` prompts.
- PR 5: the new metadata drift test passes, and fails when a metadata file is edited by hand.
- PR 6: `make test-spark` and `make test-iceberg` before and after. Run `make run-batch` end to end on seeded data and compare gold row counts and the ClickHouse marts against a pre-refactor run.

- PR 7: `docker compose up` brings up MinIO healthy and the `lakehouse` bucket exists. Run seed, `make run-batch` and the streaming job, then confirm Parquet and metadata objects appear under `s3a://lakehouse/warehouse` in the MinIO console and nothing is written to the local volume. Run `make clean` and confirm the bucket data survives if only `minio-data` is kept. Compare gold row counts against a local-filesystem run. Run the `minio`-marked test.

## Open questions
- MinIO landing zone for NiFi campaign CSVs: keep on a local volume or move into the bucket (see PR 7, step 5).
- Lockfile tool: `uv` (recommended) or pip-tools.
- How NiFi secrets should be handled: parameter context or placeholder with a documented manual step.
