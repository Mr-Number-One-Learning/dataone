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
- **Use Case:** Run this first to spin up Kafka, PostgreSQL, ClickHouse, Prometheus, Grafana, and the Spark master/workers.

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
- **Description:** Tears down all containers, networks, and **completely deletes** persistent docker volumes.
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
- **Allowed `STAGE` choices:**
  - `ingest_bronze` - Ingests external source file campaigns and reviews.
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
- **Description:** Launches the database CDC polling loops locally. Emulates a CDC agent by periodically reading postgres WAL/CDC changes and writing them to the `orders-cdc` topic.

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
- **Description:** Runs the Prefect orchestration agent script to register flows, tasks, and begin executing schedules.
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
- **Pre-requisites:** Requires a running docker-compose container environment.

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
