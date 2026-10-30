# Infrastructure & Deployment Guide

This directory contains the Docker configurations, schemas, Grafana dashboards, and metrics exporters that orchestrate the DataOne lakehouse environment.

## 🛠 Prerequisites
- **Docker** & **Docker Compose** (Ensure at least 6GB of RAM is allocated to Docker Desktop if running on Mac/Windows).
- **Make** (for orchestration shortcuts).
- **Python 3.10+** (for local data generators).

## ⚙️ Environment Configuration (`.env`)

Copy `.env.example` to `.env` before starting the cluster. 

| Variable | Description | Default Example |
|---|---|---|
| `POSTGRES_*` | OLTP Source & Iceberg Catalog credentials | `dataone` / `changeme` |
| `MONGO_*` | Document storage for Raw Reviews | `dataone` / `changeme` |
| `KAFKA_BOOTSTRAP_SERVERS` | Network address for the Kafka broker | `kafka:29092` |
| `CLICKHOUSE_*` | Analytics Database credentials | `default` / `changeme` |
| `NIFI_HOST` / `NIFI_PORT` | Apache NiFi Address | `nifi` / `8443` |
| `NIFI_REVIEWS_LISTEN_PORT` | Plain HTTP endpoint for ingestion | `9900` |
| `ICEBERG_WAREHOUSE` | Iceberg warehouse location (`s3://...` = MinIO; unset = local `file://`) | `s3://lakehouse/warehouse` |
| `ICEBERG_WAREHOUSE_PATH` | Local volume path for streaming checkpoints and NiFi staging | `/data/lakehouse` |
| `MINIO_IMAGE` / `MINIO_MC_IMAGE` | MinIO server / `mc` client image tags (required, no default; see `.env.example`) | `quay.io/minio/minio:RELEASE...` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | MinIO admin (server + init job only) | `minioadmin` / `changeme1234` |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Least-privilege app user the Spark jobs use | `dataone-app` / `changeme1234` |
| `MINIO_BUCKET` / `MINIO_ENDPOINT` / `MINIO_REGION` | Warehouse bucket, in-network endpoint, signing region | `lakehouse` / `http://minio:9000` / `us-east-1` |
| `GRAFANA_ADMIN_PASSWORD` | Access password for the dashboard UI | `changeme` |
| `GEN_*` | Data generator constraints (seed, row counts) | *See .env.example* |

## 🪣 Object storage (MinIO)

The Iceberg warehouse (Parquet data files plus table metadata) lives in the MinIO bucket `lakehouse`, written through Iceberg's `S3FileIO` (the `iceberg-aws-bundle` jar baked into the Spark image). The `minio-init` job creates the bucket and an application user limited to it, so Spark never sees the root credentials.

- S3 API on the host: `http://localhost:9100` (host port 9000 is ClickHouse's). Console: `http://localhost:9001`.
- The `lakehouse-data` volume is still mounted for **streaming checkpoints** and **NiFi's campaign staging files**; those are local by design.
- After pulling this change, rebuild the Spark image (`docker compose build`, the jar list changed) and add the MinIO variables from `.env.example` to your `.env`.
- Existing local-filesystem tables are not migrated (Iceberg metadata embeds absolute `file://` paths). Re-seed and re-run the batch, or unset `ICEBERG_WAREHOUSE` to keep using the local warehouse.
- Opt-in integration test: `make test-minio` (needs MinIO running).

## 🚢 Deployment Commands

The infrastructure is split into Docker Compose profiles:
1. **`core`**: Always-on services (Databases, Kafka, Spark Streaming, UIs, Monitoring).
2. **`batch`**: Ephemeral PySpark worker for nightly pipeline execution.

**Start the Cluster:**
```bash
make up
```
*(Under the hood: `docker compose --profile core up -d`)*

**Run the Batch ETL Manually:**
```bash
make batch
```
This starts the on-demand batch worker, submits the ETL job, and stops the worker again when it finishes. If a worker is ever left running, stop it with:
```bash
make batch-stop
```

**Run the Batch ETL Automatically (Daemon):**
```bash
make schedule
```
This launches a lightweight Python orchestrator that automatically triggers the batch pipeline at 02:00 AM daily.

You can also pass an optional `INTERVAL` parameter:
```bash
make schedule INTERVAL=30m  # Runs every 30 minutes
make schedule INTERVAL=2h   # Runs every 2 hours
```

## 🕸️ Data Lineage (OpenLineage & Marquez)

DataOne tracks PySpark job data lineage using **OpenLineage**. Instead of sending lineage HTTP requests directly from PySpark to the Marquez API (which can cause bottlenecks), the PySpark jobs are configured to use the **Kafka transport**, pushing lineage events to the internal `openlineage-events` Kafka topic. A lightweight background service (`marquez-kafka-consumer`) listens to this topic and pushes the events into the Marquez API asynchronously.

## 🌐 Exposed Interfaces & Ports

| Service | Port | Local URL |
|---|---|---|
| **Grafana** (Dashboards) | `3000` | [http://localhost:3000](http://localhost:3000) |
| **Kafka UI** | `8085` | [http://localhost:8085](http://localhost:8085) |
| **NiFi UI** | `8443` | [https://localhost:8443](https://localhost:8443) |
| **Tabix** (ClickHouse UI) | `8088` | [http://localhost:8088](http://localhost:8088) |
| **Marquez UI** (Lineage) | `3001` | [http://localhost:3001](http://localhost:3001) |
| **Mongo Express** | `8081` | [http://localhost:8081](http://localhost:8081) |
| **Spark Master UI** | `8099` | [http://localhost:8099](http://localhost:8099) |
| **Prometheus** | `9090` | [http://localhost:9090](http://localhost:9090) |
| **MinIO Console** | `9001` | [http://localhost:9001](http://localhost:9001) |
| **MinIO S3 API** | `9100` | `http://localhost:9100` |
| **Postgres** (JDBC) | `5445` | `jdbc:postgresql://localhost:5445/dataone` |
| **ClickHouse** (HTTP) | `8123` | `http://localhost:8123` |
| **Kafka** (External) | `9092` | `localhost:9092` |

> **Security**: every published port is bound to `127.0.0.1`, so these URLs work from the host only. Passwords have no in-code defaults; `docker compose` and the Python config both fail fast if a required value in `.env` is missing.

## 📊 Grafana Provisioning & JSON Architecture

Grafana is provisioned automatically with its datasources and dashboards via config files in `infra/grafana/provisioning/` and JSON dashboard files located in `infra/grafana/dashboards/`. 

- `business_kpis.json` — daily sales (7d/30d rolling avg), top 5 products per category, customer segments, conversion rate, campaign effectiveness, and the streaming job's real-time `live_activity` pulse. ClickHouse datasource.
- `ops_overview.json` — container CPU/RAM (cAdvisor), Kafka broker/partition counts, Postgres active connections, ClickHouse query rate, Postgres/Spark up-status. Prometheus datasource.
- `data_quality.json` — quarantine metrics and alerts. ClickHouse datasource.
- `realtime_operations.json` — real-time order velocity, estimated revenue over the last hour, live order status breakdowns, and batch vs. real-time projections. ClickHouse datasource.

**Not included:** NiFi flow health. NiFi's `/nifi-api/flow/metrics/prometheus` scrape target likely 401s under single-user auth (see `infra/prometheus/prometheus.yml`'s comment on the `nifi` job) — adding a panel for a target that's probably down isn't worth it until that's resolved.

**Validation:** Dashboards are auto-loaded by Grafana via `dashboards.yml`. Column/metric names are cross-checked automatically by `tests/test_grafana_dashboards.py` and `tests/test_clickhouse_ddl.py` — they are continuously validated in CI, not just eyeballed.

## 🩺 Troubleshooting

### Grafana/Prometheus Clock Drift (WSL2 / Docker Desktop)
If you see an error in Grafana stating:
> *Warning: Error fetching server time: Detected X seconds time difference between your browser and the server. Prometheus relies on accurate time and time drift might cause unexpected query results.*

**Cause:** When your Windows host machine goes to sleep or hibernates, the clock inside the Linux VM (WSL2) running Docker stops. Upon waking up, the VM's clock does not always sync back with the host, causing metrics to be recorded with timestamps from the past.

**Fix:** 
1. **Windows (WSL2):** Open PowerShell and run `wsl --shutdown`. This will fully shut down the WSL VM. Wait a few seconds, then restart Docker Desktop. It will boot a fresh VM with the correctly synced time.
2. **Alternative:** Simply restarting Docker Desktop (Quit and re-open) may also work, but `wsl --shutdown` guarantees the underlying VM clock resets.

## 🧨 Teardown & Destroy

To prevent accidental CPU usage or persistent resource overallocation on your host machine, ensure you tear down the environment when finished.

**Stop all services (keeps data intact):**
```bash
make down
```

**Destroy all services AND wipe all data volumes:**
```bash
make clean
```
> **Warning**: `make clean` asks for confirmation (`make clean FORCE=1` skips it). It destroys all Postgres data, Iceberg tables (the MinIO volume), ClickHouse marts, Kafka topics, and Grafana history. You will need to run `make seed` upon next boot.
