# DataOne Command Catalog

This document serves as the official command catalog for the DataOne lakehouse platform. Every command is presented inside a copyable code block for ease of use.

---

## 🚀 Environment Control

### Starts Core Services
```bash
make up
```
- **Description:** Starts core infrastructure services in background daemon mode.
- **Underlying Command:** `docker compose --profile core up -d`
- **Use Case:** Run this first to spin up the `core` profile: Kafka, PostgreSQL, MongoDB, ClickHouse, MinIO, NiFi, the Spark master and streaming worker, Prometheus, Grafana, Marquez and Prefect. The batch Spark worker is in the `batch` profile and is started on demand by `make run-batch`.

### Stops Core Services
```bash
make down
```
- **Description:** Stops core and batch pipeline containers without deleting data.
- **Underlying Command:** `docker compose --profile core --profile batch down`
- **Use Case:** Stop the local sandbox when finished.

### Hard Reset / Clean Volumes
```bash
make clean
```
- **Description:** Tears down all containers, networks, and **completely deletes** persistent docker volumes (databases, the MinIO-hosted Iceberg warehouse, checkpoints). Asks for confirmation first; `make clean FORCE=1` skips the prompt.
- **Underlying Command:** `docker compose --profile core --profile batch down -v`
- **Use Case:** Perform a hard reset to start with a fresh database and Iceberg catalog.

### List Container Status
```bash
make ps
```
- **Description:** Lists the current status of all running Docker containers.
- **Underlying Command:** `docker compose ps`

### Follow Real-time Logs
```bash
make logs
```
- **Description:** Attaches to the stdout/stderr streams of the docker services to view real-time log outputs.
- **Underlying Command:** `docker compose logs -f --tail=200`

---

## 📦 Batch & Medallion Pipeline Execution

### Execute Medallion Pipeline
```bash
make run-batch
```
- **Description:** Spins up the on-demand batch Spark executor, runs the main medallion pipeline script `bronze_to_silver.py`, and stops the batch worker once complete.
- **Underlying Command:** `docker compose ... up -d spark-worker-batch && docker exec ... spark-submit ... && docker compose ... stop spark-worker-batch`
- **Arguments / Options:**
  - `START_DATE` (e.g. `2026-07-01`): The start date of the date range to backfill.
  - `END_DATE` (e.g. `2026-07-14`): The end date of the date range to backfill (requires `START_DATE`).
  - `STAGE` (e.g. `standardize_silver`): Medallion stage to run.
  - `BATCH_CONTAINER`, `BATCH_DRIVER_MEMORY`, `BATCH_EXECUTOR_MEMORY` (defaults `dataone-spark-worker-batch`, `512m`, `4000m`): Override the batch container name and Spark memory, e.g. `make run-batch BATCH_EXECUTOR_MEMORY=2000m`.
- **Allowed `STAGE` choices:**
  - `ingest_bronze` - Ingests NiFi-staged campaign files, MongoDB reviews, and Postgres `products` (full snapshot) and `order_items` (incremental) into Bronze.
  - `standardize_silver` - Standardizes bronze tables, runs quality gates, and merges records into Silver Iceberg tables.
  - `model_gold` - Curates and transforms Silver data into Gold dimensions and fact metrics.
  - `sync_clickhouse` - Synchronizes computed business marts to ClickHouse.
- **Examples:**
  - *Run complete pipeline:*
    ```bash
    make run-batch
    ```
  - *Run narrow backfill:*
    ```bash
    make run-batch START_DATE=2026-07-01 END_DATE=2026-07-05
    ```
  - *Run a single stage:*
    ```bash
    make run-batch STAGE=standardize_silver
    ```
  - *Run backfill for a single stage:*
    ```bash
    make run-batch STAGE=model_gold START_DATE=2026-07-01 END_DATE=2026-07-05
    ```

### Run the Batch Pipeline (alias)
```bash
make batch
```
- **Description:** Alias for `make run-batch` (same options).

### Backfill a Date Range
```bash
python -m dataone.batch.backfill --start 2026-07-01 --end 2026-07-31 --chunk-days 7
```
- **Description:** Re-runs the batch job over a historical date range, one `spark-submit` per window. `--chunk-days N` (default 0 = one job for the whole range) splits the range so a failed window can be resumed. The Prefect `backfill(start, end)` flow in `nightly_batch.py` does the same stage by stage.

### Stop Batch Worker
```bash
make batch-stop
```
- **Description:** Stops the batch executor Spark container manually if it hangs.
- **Underlying Command:** `docker compose --profile core --profile batch stop spark-worker-batch`

---

## ⚡ Streaming & Real-Time Simulation

### Provision Kafka Topics
```bash
make kafka-topics
```
- **Description:** Provisions required Kafka topics (`orders-cdc`, `clickstream`) with default parameters on the Kafka broker.
- **Underlying Command:** `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ...`

### Execute Streaming Job
```bash
make stream-job
```
- **Description:** Deploys the PySpark structured streaming consumer/processor (`structured_streaming_job.py`) to ingest and parse real-time events.
- **Dependencies:** Implicitly provisions topics first via `make kafka-topics`.

### Run CDC Polling Loop
```bash
make stream-cdc
```
- **Description:** Serves the Prefect `cdc-poll` flow from your machine (`python src/dataone/orchestration/cdc_poll.py`). Every 5 seconds it polls the Postgres `customers` and `orders` tables for rows newer than their `updated_at` watermark and publishes change events to the `orders-cdc` topic. Needs the Prefect server (`make up`).

### Run Live Orders Generator
```bash
make stream-live-orders
```
- **Description:** Simulates active orders streaming into Kafka by running a live orders transaction generator locally.

### Run Clickstream Generator
```bash
make stream-clickstream
```
- **Description:** Simulates active user activity streaming into Kafka by running a clickstream event generator locally.

### Query DLQ Dead Letters
```bash
make query-dead-letters
```
- **Description:** Submits a PySpark helper script to scan and print the contents of the streaming Dead Letter Queue (DLQ).

---

## 🗓️ Scheduling & Orchestration

### Run Nightly Batch Scheduler
```bash
make schedule
```
- **Description:** Serves the Prefect `nightly-batch` flow (cron `0 2 * * *`) from a foreground process on your machine; keep it running. Each run executes `ingest_bronze`, `standardize_silver`, `model_gold` and `sync_clickhouse` as separate Prefect tasks (each a `make run-batch STAGE=...`). The `backfill` flow is not served by this command.
- **Underlying Command:** `python src/dataone/orchestration/nightly_batch.py`

---

## 🧪 Seeding & Test Suite

### Seed Source Databases
```bash
make seed
```
- **Description:** Runs generators locally to populate source files and transactional database states (Orders, Campaigns, Reviews).

### Run Test Suite
```bash
make test
```
- **Description:** Runs the offline/unit tests with pytest (non-Spark/non-Docker tests).
- **Underlying Command:** `pytest -v`

### Run Spark Marked Tests
```bash
make test-spark
```
- **Description:** Runs unit tests marked with `@pytest.mark.spark`.
- **Underlying Command:** `pytest -v -m spark`
- **Pre-requisites:** Requires a local Java install.

### Run Iceberg Marked Tests
```bash
make test-iceberg
```
- **Description:** Runs tests marked with `@pytest.mark.iceberg`.
- **Underlying Command:** `pytest -v -m iceberg`
- **Pre-requisites:** Requires a local Java install and network access on the first run (to fetch the Iceberg jar). No Docker needed: the tests use a throwaway local Hadoop catalog (see `tests/conftest.py`).

### Run MinIO Marked Tests
```bash
make test-minio
```
- **Description:** Runs tests marked with `@pytest.mark.minio` (an Iceberg table round-trip through S3FileIO).
- **Underlying Command:** `pytest -v -m minio`
- **Pre-requisites:** A local Java install, MinIO running (`docker compose --profile core up -d minio minio-init`), and `MINIO_TEST_ENDPOINT` (e.g. `http://localhost:9100`), `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` set.

### Check Coverage Report
```bash
make coverage
```
- **Description:** Runs test coverage and prints missing/untested lines.

---

## 🧼 Code Quality & Linting

### Lint Check Codebase
```bash
make lint
```
- **Description:** Runs `ruff` static analysis checks and `black` code format checks.

### Format Codebase
```bash
make fmt
```
- **Description:** Automatically applies `black` code formatting and runs `ruff` lint auto-fixes.
