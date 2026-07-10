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
| `ICEBERG_WAREHOUSE_PATH` | Lakehouse storage path | `/data/lakehouse` |
| `GRAFANA_ADMIN_PASSWORD` | Access password for the dashboard UI | `changeme` |
| `GEN_*` | Data generator constraints (seed, row counts) | *See .env.example* |

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
| **Postgres** (JDBC) | `5445` | `jdbc:postgresql://localhost:5445/dataone` |
| **ClickHouse** (HTTP) | `8123` | `http://localhost:8123` |
| **Kafka** (External) | `9092` | `localhost:9092` |

## 📊 Grafana Provisioning & JSON Architecture

Grafana is provisioned automatically with its datasources and dashboards via config files in `infra/grafana/provisioning/` and JSON dashboard files located in `infra/grafana/dashboards/`. 

- `business_kpis.json` — daily sales (7d/30d rolling avg), top 5 products per category, customer segments, conversion rate, campaign effectiveness, and the streaming job's real-time `live_activity` pulse. ClickHouse datasource.
- `ops_overview.json` — container CPU/RAM (cAdvisor), Kafka broker/partition counts, Postgres active connections, ClickHouse query rate, Postgres/Spark up-status. Prometheus datasource.
- `data_quality.json` — quarantine metrics and alerts. ClickHouse datasource.
- `realtime_operations.json` — real-time order velocity, estimated revenue over the last hour, live order status breakdowns, and batch vs. real-time projections. ClickHouse datasource.

**Not included:** NiFi flow health. NiFi's `/nifi-api/flow/metrics/prometheus` scrape target likely 401s under single-user auth (see `infra/prometheus/prometheus.yml`'s comment on the `nifi` job) — adding a panel for a target that's probably down isn't worth it until that's resolved.

**Validation:** Dashboards are auto-loaded by Grafana via `dashboards.yml`. Column/metric names are cross-checked automatically by `tests/test_grafana_dashboards.py` and `tests/test_clickhouse_ddl.py` — they are continuously validated in CI, not just eyeballed.

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
> **Warning**: This destroys all Postgres data, Iceberg tables, ClickHouse marts, Kafka topics, and Grafana history. You will need to run `make seed` upon next boot.
