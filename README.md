![DataOne Banner](docs/images/dataone-project-banner.png)

# DataOne Lakehouse & Streaming Analytics
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Spark](https://img.shields.io/badge/apache_spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/apache_kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Apache NiFi](https://img.shields.io/badge/Apache_NiFi-72BAC5?style=for-the-badge&logo=apache&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-00d1e0?style=for-the-badge&logo=apache&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/postgresql-4169e1?style=for-the-badge&logo=postgresql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white)
![Grafana](https://img.shields.io/badge/grafana-%23F46800.svg?style=for-the-badge&logo=grafana&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-ffffff?style=for-the-badge&logo=prefect&logoColor=070E82)
![Marquez](https://img.shields.io/badge/Marquez-000000?style=for-the-badge&logoColor=white)
![OpenLineage](https://img.shields.io/badge/OpenLineage-272A30?style=for-the-badge&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

An end-to-end modern data engineering project demonstrating Change Data Capture (CDC), streaming analytics, and batch processing built on a local **Lakehouse** architecture using Apache Iceberg, PySpark, ClickHouse, and Grafana.

---

## 🛠️ Tool Stack Selection & Justification

Every tool in this stack was selected deliberately to maximize performance while respecting strict hardware constraints (local Docker environment). We prioritize technical fit over familiarity:

| Layer | Tool | Role | Why this, not the alternative? |
|---|---|---|---|
| **Streaming Ingestion** | **Kafka** (KRaft mode) | Event backbone for clickstream + CDC events | KRaft mode removes the separate ZooKeeper dependency, providing the same durability guarantees but saving ~700MB of RAM. |
| **Batch Ingestion** | **Apache NiFi** | Pulls marketing CSVs + reviews API, schema-checks at the edge | Visual flow control, built-in back-pressure, and provenance beat hand-rolled file watchers for heterogeneous batch sources. |
| **OLTP & Catalog** | **PostgreSQL** | Simulated app DB (orders/customers) AND Iceberg's JDBC catalog | Reusing Postgres as the Iceberg catalog avoids deploying a heavy Hive Metastore just to track table metadata. |
| **Document Store** | **MongoDB** | Raw product reviews (variable schema JSON) | Genuinely schema-flexible source—demonstrating a real NoSQL use-case, not a forced one. |
| **Lakehouse Format** | **Apache Iceberg** | Bronze/Silver/Gold tables, schema evolution, time travel, SCD2 | Provides ACID transactions, schema evolution, and partition pruning without needing a Hive Metastore or live HDFS cluster. |
| **OLAP Serving** | **ClickHouse** | Pre-aggregated marts for the dashboard | Achieves sub-second aggregations on a single node with a fraction of the RAM footprint required by Hive or Presto. |
| **Processing Engine** | **PySpark** | Both Batch ETL (SCD2, dedup) and Structured Streaming | Reuses the same codebase/skills for batch and real-time paradigms. Industry-standard for heavy data transformations. |
| **Orchestration** | **Prefect** | Lightweight orchestration via Prefect flows and tasks | Prefect provides robust task-level retries and visibility with a minimal footprint, replacing the legacy custom scheduler. |
| **Dashboards** | **Grafana** | Ops dashboards (Prometheus) and Business KPIs (ClickHouse) | One service, two purposes. RAM-conscious reuse instead of spinning up a separate BI tool like Metabase or Superset. |
| **Data Lineage** | **Marquez & OpenLineage** | Visualizes PySpark job data lineage | Provides full observability into dataset lineage and job execution without heavy manual instrumentation. Uses Kafka transport for efficiency. |
| **Metrics** | **Prometheus** | Scrapes Kafka/Spark/NiFi/Postgres exporters | Standard pull-based metrics with extremely low overhead. |

*Deliberately excluded: Hive (its metastore role is covered by Postgres + Iceberg) and HDFS (we use HadoopFileIO on local disk to get Parquet I/O benefits at near-zero cost).*

---

## 🏗️ Architecture & Data Flow

![Visual Architecture](docs/images/Architecture%20&%20Data%20Flow.png)

---

## 🚀 5-Minute Local Quick-Start

To get the full pipeline running locally on your laptop, ensure you have **Docker**, **Docker Compose**, and **Make** installed. 

### 1. Start Infrastructure
Bring up the `core` profile, which includes Postgres, ClickHouse, Kafka, MongoDB, NiFi, Grafana, and **Prefect**:
```bash
make up
```

Wait 30-60 seconds for the containers to fully initialize. You can monitor the health with `make ps`.

### 2. Generate Seed Data
Generate initial synthetic data into Postgres (customers, orders, products) and MongoDB (reviews):
```bash
# Ensure you are using Python 3.10+ and have installed dependencies via pip install -r requirements.txt
make seed
```

### 3. Start Streaming Jobs
Initialize the Kafka topics and start the PySpark Structured Streaming worker, which continuously ingests CDC logs and clickstream data into the Bronze Iceberg layer:
```bash
make stream-job
```

Simulate live CDC events and clickstream activity in a separate terminal. The CDC poller is now orchestrated by Prefect:
```bash
make stream-cdc
make stream-clickstream
```

### 4. Run the Nightly Batch ETL
Process the Bronze records through the Data Quality Gate, merge into Silver (applying SCD Type 2 logic for customers), aggregate into Gold business marts, and export them to ClickHouse for serving:

**Manual Run:**
```bash
make batch
```
> *Note: `make batch` starts the on-demand Spark batch worker, submits the full ETL job (`bronze_to_silver.py`), and stops the worker again when it finishes (if the worker is left running, `make batch-stop` stops it).*

**Automated Schedule (Prefect):**
The Prefect server and worker run automatically via the `core` profile. To deploy and run the nightly batch scheduler via Prefect:
```bash
make schedule
```
Monitor the execution, retries, and logs in the Prefect UI at `http://localhost:4200`.

### 5. View Dashboards
Navigate to `http://localhost:3000` to access Grafana (Default credentials are configured in `.env`). The predefined dashboards pull directly from the ClickHouse layer.

You can preview the visual layouts in the **[Dashboards Guide](docs/DASHBOARDS.md)**.

To tear down all resources and clear the persistent volumes, run:
```bash
make clean
```

---

## ✅ Testing & CI

Run the offline test suite locally with `make test` (Spark/Iceberg-dependent tests are opt-in via `make test-spark` / `make test-iceberg`), and get a line-by-line coverage report with `make coverage`. Every push and pull request also runs linting (Ruff + Black) and the full offline suite with coverage in GitHub Actions — see [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

---

## 📚 Documentation References

For more detailed information, please refer to the specific documentation guides located throughout the repository:

- **[Architecture & Design Decisions](docs/DECISION.md)**: The single source of truth for the platform's architectural paradigm, tool selection justifications, and structural implementations.
- **[Orchestration Guide (Prefect)](docs/ORCHESTRATOR.md)**: Describes the Prefect 3.x infrastructure, deployment details, and flow configurations replacing the legacy scheduler.

- **[Data Dictionary & Schema Contract](docs/DATA_DICTIONARY.md)**: Details the exact lineage paths and precise schema mapping from Bronze through Gold layers.
- **[Infrastructure Guide](docs/INFRASTRUCTURE_GUIDE.md)**: Maps the Docker profiles, environment variables, mapped ports, and teardown commands.
- **[NiFi Flows Guide](docs/NIFI_FLOWS_GUIDE.md)**: Visual architecture of the Apache NiFi ingestion processes.
- **[Dashboards Guide](docs/DASHBOARDS.md)**: Previews of the Grafana dashboards for Business KPIs, Operations, Realtime Operations, and Data Quality.
- **[Data Modeling Architecture](docs/DATA_MODELING.md)**: Details the Medallion & Kimball dimensional modeling choices, performance optimizations, and SCD Type 2 logic.
- **[Prefect Orchestration Migration Plan](docs/future/Prefect_Migration_Plan.md)**: Details the migration roadmap for replacing Makefiles with Prefect for advanced pipeline orchestration and observability.
- **[Quality & Validation Guide](docs/TESTING_GUIDE.md)**: Outlines Pytest usage, the Spark/Iceberg dependency markers, and the internal Data Quality validation gate.
- **[Quarantine Layer Architecture](docs/QUARANTINE_LAYER.md)**: Illustrates the DLQ mechanism, how bad rows are routed to quarantine instead of being dropped, and how they are monitored.
- **[Future Improvements Roadmap](docs/future/Future_Improvements.md)**: Strategic recommendations for elevating the project to a Lead/Senior Data Engineering tier (e.g., dbt, DataHub, CI/CD).
- **[Cloud Deployment Plan](docs/future/Cloud_Deployment_Plan.md)**: Architectural roadmap and migration strategy for moving the platform to AWS or Azure.
- **[Stage 1: Future Plan - Predictive Modeling](docs/future/ML_Model_Implementation_Plan.md)**: Details the roadmap for integrating MLflow, Prophet forecasting, and XGBoost churn models into the platform.
- **[Contributing Guide](CONTRIBUTING.md)**: Formalizes the local `venv` setup, formatting/linting using Black and Ruff, and PR documentation rules.

---

## 📂 Repository Tree Map

```text
.
├── Makefile                # Primary entrypoint for running jobs and managing Docker
├── README.md               # This file
├── CONTRIBUTING.md         # Developer setup and PR guide
├── docker-compose.yml      # Multi-container orchestration definitions
├── data/
│   ├── lakehouse/          # Iceberg warehouse directory (mounted to Spark containers)
│   └── raw/                # Local data generation outputs
├── docs/                   # Extended project documentation
├── infra/
│   ├── docker/             # Dockerfiles and DB Initialization scripts (SQL)
│   ├── grafana/            # Provisioned dashboards and datasources
│   └── prometheus/         # Metrics configurations
├── src/
│   └── dataone/
│       ├── batch/          # Batch ETL pipeline scripts (bronze_to_silver.py, scd2)
│       ├── config.py       # Centralized env/credential management
│       ├── generators/     # Synthetic data generators (orders, reviews, clickstream)
│       ├── ingestion/      # CDC Simulator to pull WAL/Updated columns from Postgres
│       ├── orchestration/  # Lightweight daemon scheduler (scheduler.py)
│       ├── quality/        # Custom PySpark Data Quality validators
│       ├── streaming/      # Spark Structured Streaming job (Kafka -> Bronze)
│       └── utils/          # Spark session builders, Iceberg DDL helpers, schemas
└── tests/
    ├── conftest.py         # Pytest fixtures for Spark and Iceberg
    └── test_*.py           # Test suites for ETL, SCD2, Quality, and Generators
```

---

*Created by **Eng. Ahmed Maher Al-Maqtari***  
*Copyright © Mr.NumberOne*
