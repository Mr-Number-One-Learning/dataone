# Architecture & Design Decisions (DECISION.md)

*Single source of truth for the DataOne Lakehouse & Streaming Analytics platform. Cross-referenced against `src/dataone/`, `infra/`, `docs/`, `docker-compose.yml`, and the test suite in `tests/`.*

---

## 1. Executive Summary & Project Overview

**DataOne** is a fully local, Docker-Composed **e-commerce sales analytics lakehouse** that simulates the data estate of a mid-sized online retailer and builds the full pipeline needed to turn raw operational data into business-ready analytics — end to end, with no managed cloud services in the loop.

### The business problem

An e-commerce operation generates data in at least four structurally incompatible shapes at once:

1. **Transactional state** (customers, orders, products) living in an OLTP system (PostgreSQL) that changes constantly and must be captured without hammering the source database.
2. **High-volume behavioral telemetry** (clickstream: page views, add-to-cart, checkout funnel events) arriving as a continuous, unbounded stream.
3. **Semi-structured, schema-variable content** (product reviews) that doesn't fit a rigid relational shape.
4. **Periodic external/batch extracts** (marketing campaign performance CSVs) that show up on no particular schedule and need light validation before they can be trusted.

DataOne's job is to land all four shapes into one coherent, queryable warehouse — enforcing correctness, tracking history, and surfacing business marts (daily sales, conversion, campaign ROAS, customer lifetime value) — through a single **Medallion (Bronze → Silver → Gold) lakehouse** built on **Apache Iceberg**.

### Volume & velocity

The environment is intentionally sized for a **single developer laptop**, not a production cluster, but the pipeline logic is written as if it weren't:

- **Batch volume:** synthetic generators default to `GEN_ORDERS_TOTAL_ROWS=100000`, `GEN_PRODUCTS_TOTAL=2000`, `GEN_REVIEWS_TOTAL=5000` (see `docs/DATA_DICTIONARY.md`, tunable via `.env`).
- **Streaming velocity:** the clickstream generator defaults to `GEN_CLICKSTREAM_EVENTS_PER_SEC=200`, continuously produced into Kafka and consumed by a Spark Structured Streaming job on a 30-second micro-batch trigger (`structured_streaming_job.py`).
- **CDC velocity:** the CDC simulator (`ingestion/cdc_simulator.py`) polls Postgres every 5 seconds (`POLL_INTERVAL_SECONDS = 5`) with a `BATCH_LIMIT = 5_000` rows/poll ceiling, emitting change events to the `orders-cdc` Kafka topic.
- **Batch cadence:** the nightly ETL (`batch/bronze_to_silver.py`, run stage by stage) is scheduled for 02:00 daily by the Prefect `nightly-batch` flow (`orchestration/nightly_batch.py`, served with `make schedule`); any stage can also be run by hand with `make run-batch STAGE=<stage>`.

The explicit design constraint threading through every decision below is: **hardware-conscious, dependency-light, and production-representative** — i.e., every tool substitution favors a lighter-weight but semantically equivalent option over the "default" heavyweight industry tool, rather than skipping the capability entirely.

---

## 2. Technology Stack & Tool Selection Matrix

| Component | Tool Chosen | Alternatives Considered | Core Reasons for Selection |
|---|---|---|---|
| **Streaming Ingestion (Event Backbone)** | **Apache Kafka (KRaft mode)** | Kafka + ZooKeeper, Redpanda, AWS Kinesis | KRaft mode eliminates the ZooKeeper quorum entirely, saving ~700MB RAM on a laptop-constrained Docker host while keeping the same durability/ordering guarantees. Kinesis/managed brokers were rejected outright — this project runs with zero cloud dependency. |
| **Batch/Semi-Structured Ingestion** | **Apache NiFi** | Custom Python file-watchers, Airbyte | NiFi's visual flow canvas gives built-in back-pressure, provenance tracking, and schema-checking at the edge for two structurally different sources (CSV campaign exports, REST→MongoDB review ingestion) without hand-rolling retry/backoff logic per source. |
| **OLTP Source + Iceberg Catalog** | **PostgreSQL** (dual role) | Hive Metastore, AWS Glue Catalog, MySQL | Reusing the same Postgres instance as both the simulated app database *and* Iceberg's **JDBC catalog** (`org.apache.iceberg.jdbc.JdbcCatalog`, see `utils/iceberg_helpers.py`) avoids standing up a full Hive Metastore just to track Iceberg table metadata — one fewer JVM service to run and tune. |
| **Document Store (Semi-Structured)** | **MongoDB** | Storing reviews as Postgres JSONB | Reviews are a genuinely variable-schema source (optional fields, free-text, nested metadata) — this is a real NoSQL use case rather than a forced one, demonstrating polyglot persistence intentionally. |
| **Lakehouse Table Format** | **Apache Iceberg** (`iceberg-spark-runtime`, Spark 3.5.1) | Delta Lake, Apache Hudi, raw partitioned Parquet | Iceberg gives ACID transactions, safe concurrent writers, schema evolution, hidden partitioning, and native `MERGE INTO` (used for SCD2) — all without requiring a live HDFS cluster or Databricks-specific runtime, unlike Delta Lake's tightest integrations. |
| **Processing Engine (Batch + Streaming)** | **PySpark / Spark Structured Streaming** | Pandas + cron scripts, Flink, dbt-only | One engine covers both the continuous Kafka→Bronze ingestion path and the heavy nightly Bronze→Gold batch transform (SCD2 merges, dedup, aggregation) — same DataFrame API, same skillset, same cluster (`spark-master`, `spark-worker-streaming`, `spark-worker-batch` in `docker-compose.yml`). |
| **OLAP Serving Layer** | **ClickHouse** | Presto/Trino, Druid, plain Postgres marts | Delivers sub-second aggregate queries against `gold.*` marts on a single node with a fraction of the RAM footprint Presto/Trino would need for a comparable local cluster. |
| **Orchestration** | **Prefect 3** (`orchestration/nightly_batch.py`, `orchestration/cdc_poll.py`) | Apache Airflow, Dagster, the original custom `schedule` + `tenacity` scheduler | Python-native `@flow`/`@task` decorators over the existing code, task-level retries and a UI, while reusing the existing Postgres (a `prefect` database) instead of Airflow's webserver + scheduler + Celery/Redis footprint. Replaced the earlier custom scheduler; see `docs/future/Prefect_Migration_Plan.md` for the migration record and `docs/ORCHESTRATOR.md` for how it runs. |
| **Dashboards / BI** | **Grafana** (dual-purpose) | Metabase, Apache Superset | One Grafana instance serves *both* infra/ops metrics (Prometheus datasource: container CPU/RAM, Kafka broker health) and business KPIs (ClickHouse datasource: `daily_sales`, `conversion_rate`, `roas`) — avoiding a second BI tool purely to separate audiences. |
| **Data Lineage** | **OpenLineage → Kafka transport → Marquez** | Direct HTTP OpenLineage-to-Marquez calls, DataHub | PySpark jobs are configured with `spark.openlineage.transport.type=kafka` (see `utils/spark_session.py`) rather than firing synchronous HTTP lineage events at the Marquez API — this decouples job execution from Marquez availability/latency. A dedicated `marquez-kafka-consumer` service asynchronously drains the `openlineage-events` topic into Marquez. |
| **Metrics** | **Prometheus** (+ cAdvisor, postgres-exporter, kafka-exporter) | Datadog, New Relic | Standard pull-based scraping with near-zero overhead; fully self-hosted, matching the no-cloud-dependency constraint. |
| **Data Quality / Governance** | **Custom PySpark Quality Gate** (`quality/validators.py`) + Quarantine Layer | Great Expectations, Soda Core, dbt tests | A purpose-built null/range-check gate integrated directly into the Bronze→Silver Spark transform avoids the JVM/Python interop overhead and separate-config-file model of Great Expectations, while still enforcing the two things this pipeline actually needs: required-column presence and numeric range bounds — see Section 6 for the honest limits of this approach. |

*Deliberately excluded: Hive (its catalog role is fully covered by Postgres + Iceberg's JDBC catalog) and HDFS (Iceberg writes Parquet and table metadata to MinIO, an S3-compatible object store, through `S3FileIO` and `ICEBERG_WAREHOUSE=s3://...`, getting columnar I/O without a live HDFS NameNode/DataNode cluster; unsetting `ICEBERG_WAREHOUSE` falls back to a local `file://` warehouse under `ICEBERG_WAREHOUSE_PATH`). Streaming checkpoints and NiFi's campaign staging files remain on the local `lakehouse-data` volume.*

### 2.1 Deep-Dive: Why These Tools Over Their Competitors

**Iceberg vs. Delta Lake vs. Hudi.** Delta Lake's richest features (e.g., Unity Catalog integration, deep OPTIMIZE/Z-ORDER tuning) are strongest inside Databricks; running it purely open-source on vanilla Spark loses some of that advantage while still carrying comparable operational weight. Hudi's copy-on-write/merge-on-read distinction adds real complexity for a project whose actual requirement is simpler: ACID batch writes plus one `MERGE INTO`-driven SCD2 job. Iceberg's catalog abstraction is also what let this project reuse Postgres as the catalog (via `JdbcCatalog`) instead of standing up a Hive Metastore — a concrete, measurable win for a memory-constrained Docker host.

**Prefect vs. Airflow/Dagster (and the original custom scheduler).** The project started with a `schedule`-library daemon plus a `tenacity` retry decorator, chosen because Airflow's minimum viable local footprint (webserver + scheduler + Postgres metadata DB + Redis/Celery for anything beyond `SequentialExecutor`) competes directly with Spark and Kafka for the same constrained RAM budget. It was replaced by Prefect 3 (see `docs/future/Prefect_Migration_Plan.md`), which keeps the footprint small (server + Redis, metadata in the existing Postgres) while adding a UI, run history and task-level retries. The migration also removed the old limitation that the whole nightly job ran as **one Spark session inside one `spark-submit` process**: `bronze_to_silver.py` now takes `--stage {ingest_bronze, standardize_silver, model_gold, sync_clickhouse}`, and `nightly-batch` runs each stage as its own Prefect task, so a failed ClickHouse sync is retried without re-running ingest, the quality gate or the SCD2 merge.

**ClickHouse vs. Presto/Trino/Druid.** The Gold marts (`daily_sales`, `roas`, `conversion_rate`, etc.) are pre-aggregated, low-cardinality-dimension tables meant for dashboard-speed reads, not ad-hoc federated queries across many sources — ClickHouse's single-node columnar engine is a closer match to that access pattern than Presto/Trino's distributed-query design, at a much lower memory cost for a laptop deployment. `fact_order_items` is *also* synced to ClickHouse (not just the aggregates), which recovers some of the ad-hoc drill-down flexibility a pure OBT-mart-only design would sacrifice (see Section 4).

**PySpark for both batch and streaming vs. splitting engines (e.g., Flink for streaming).** Using one engine for both paradigms means the ingestion team needs one API surface, not two — `structured_streaming_job.py` and `batch/bronze_to_silver.py` share the same `utils/iceberg_helpers.py`, `utils/schemas.py`, and `utils/spark_session.py` modules. The trade-off, made explicit in the streaming job's own docstring, is that streaming-only sources (Kafka CDC, clickstream) can't materialize revenue-level business metrics in real time because `order_items`/`products` are batch-JDBC-only sources — so the "live" ClickHouse feed is activity counts (events, sessions, checkout completions), not revenue, and that gap is closed on the ClickHouse side instead: a ClickHouse `Kafka`-engine table reads the `orders-cdc` topic directly and materialized views compute per-minute order counts, a revenue estimate and live status counts (`infra/docker/clickhouse/init/002_marts.sql`, shown on the `realtime_operations.json` dashboard; see `docs/future/Future_Improvements.md` §2).

**Custom Quality Gate vs. Great Expectations.** Great Expectations' expectation-suite model is powerful but introduces its own config/versioning layer and a heavier Python dependency footprint for what this project needs: null checks and numeric bounds enforced inline inside an already-running Spark job, with zero additional service to deploy. The trade-off — no built-in expectation library, no auto-generated data docs, no anomaly-detection expectations — is real and is the reason a future migration to Great Expectations or Soda Core is worth revisiting once the rule set outgrows simple null/range checks (see Section 6).

---

## 3. Data Architecture & Ingestion Strategy

### 3.1 Architectural paradigm

DataOne implements a **Medallion Architecture** (Bronze → Silver → Gold) with a **Kimball dimensional model fully consolidated at the Gold layer**, plus a dedicated **Quarantine layer** that runs parallel to Silver/Gold as a permanent Dead Letter Queue rather than a temporary error path. It is also a **Lambda architecture**: a real-time "speed layer" runs alongside the authoritative nightly "batch layer" (Bronze → Silver → Gold, revenue-bearing marts). The speed layer has two parts: Structured Streaming → `live_activity` in ClickHouse (clickstream activity counts), and a ClickHouse `Kafka`-engine table on the `orders-cdc` topic with materialized views (`mv_orders_cdc_parser`, `mv_orders_per_minute`, `mv_revenue_estimate`, `mv_order_status_counts`) for order-level real-time visibility — see `docs/future/Future_Improvements.md` §2.

### 3.2 Ingestion patterns

Ingestion is split into two physically decoupled pipelines rather than one unified DAG:

**Streaming (real-time landing).** The `orders-cdc` and `clickstream` Kafka topics are consumed continuously by `structured_streaming_job.py` on a `processingTime="30 seconds"` micro-batch trigger, in `outputMode("append")`, writing straight into `bronze.orders_cdc` / `bronze.clickstream` with **zero transformation or joins** — the design goal here is throughput and latency, not correctness, which is deferred entirely to the batch layer. A second, `outputMode("update")` streaming query (`.trigger(processingTime="1 minute")`) uses `foreachBatch` to push a lightweight `live_activity` aggregate directly into ClickHouse.

**Batch (curation & transformation).** `batch/bronze_to_silver.py` (the stage orchestration and silver logic, with the gold marts in `batch/gold_marts.py` and the ClickHouse load in `batch/clickhouse_sync.py`), scheduled nightly at 02:00 by the Prefect `nightly-batch` flow (one Prefect task per stage), does all the heavy lifting: parsing the CDC JSON envelope, deduplicating, running the Data Quality Gate, applying SCD Type 2 to the customer dimension, computing the Gold-layer aggregates, and syncing to ClickHouse over JDBC. Apache NiFi feeds this batch path from two additional sources — marketing campaign CSVs and a REST `ListenHTTP` endpoint that lands reviews into MongoDB.

### 3.3 Schema evolution

The **Bronze CDC table (`bronze.orders_cdc`)** takes a deliberate **schema-on-read** approach: because a single CDC simulator captures two structurally different source tables (`customers`, `orders`) into one Kafka topic, the outer envelope is kept flat and uniform — `{table_name, op, pk_column, pk_value, data_json: STRING, captured_at}` — with the actual row payload pre-serialized to a JSON string in the `data_json` column (see `cdc_simulator.py`'s `emit_change_event()` and the envelope schema in `structured_streaming_job.py`). This means upstream schema changes in Postgres (a new column added to `orders`) don't break Bronze ingestion at all — the JSON payload just carries the new field, and only the batch job's `parse_*_from_cdc()` step needs updating to surface it into Silver. The explicit trade-off, stated in `docs/DATA_MODELING.md`, is higher compute cost at batch time (JSON parsing at scale) in exchange for ingestion-time resilience.

Iceberg's own native schema evolution (`ALTER TABLE ... ADD COLUMN`, safe type widening) is available for every typed Bronze/Silver/Gold table (`bronze.clickstream`, `bronze.reviews`, `bronze.campaigns`, and all Silver/Gold tables), since Iceberg tracks columns by ID rather than position — the standard advantage over plain partitioned Parquet.

### 3.4 Backfills

`batch/backfill.py` provides a dedicated CLI (`python -m dataone.batch.backfill --start YYYY-MM-DD --end YYYY-MM-DD`) that re-invokes `bronze_to_silver.py` via a fresh `spark-submit` process, deliberately kept separate from the nightly schedule — matching how backfills are normally triggered as a distinct, manually-initiated operation. The Prefect `backfill(start, end)` flow in `orchestration/nightly_batch.py` offers the same thing stage by stage. Two properties make this safe:

1. **Idempotent by construction.** Every downstream write in the batch job is either a partition-overwrite (`write_overwrite_partitions`, used for Silver/Gold) or a true upsert-with-history (the SCD2 `MERGE INTO`) — never a blind append. The one exception is the Quarantine layer, which is *meant* to accumulate across runs as a permanent audit trail.
2. **Chunkable and resumable.** `--chunk-days N` splits a wide date range into consecutive `spark-submit` windows; if one window's job fails, `backfill.py` exits at that window (`sys.exit(result.returncode)`) instead of silently continuing, so a re-run resumes from the broken window rather than reprocessing the entire range.

### 3.5 Deduplication

Deduplication happens at two points:
- **CDC watermarking** (`ingestion/cdc_simulator.py`): a persistent `_cdc_watermarks` bookkeeping table in Postgres (not in-memory state) tracks the last-seen `updated_at` per watched table, so a simulator restart resumes from where it left off instead of re-emitting the entire table or silently skipping a gap. This gives **at-least-once** delivery — the watermark only advances after an entire poll batch is confirmed flushed to Kafka (`flush_producer()`), so a crash mid-batch causes the next poll to re-emit some rows rather than lose them. A documented known limitation: rows sharing an identical `updated_at` timestamp that get split across a `LIMIT`-bounded poll can have siblings skipped — accepted as an explicit, scoped-out edge case rather than solved with a compound `(timestamp, pk)` cursor.
- **Silver-layer dedup** (`batch/bronze_to_silver.py`): CDC events are collapsed to one row per business key (e.g., `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY captured_at DESC) = 1`) before being written to `silver.customers` / `silver.orders`, guaranteeing Silver's "one row per entity" grain regardless of how many duplicate/at-least-once CDC events arrived upstream.

---

## 4. Data Modeling & Schema Design

### 4.1 Methodology per layer

| Layer | Methodology | Rationale |
|---|---|---|
| **Bronze** | Schema-on-read (CDC) / typed flat schemas (clickstream, reviews, campaigns) | Preserve raw source fidelity; absorb upstream schema drift without breaking ingestion. |
| **Silver** | Cleansed 3NF-style curated entities (`silver.customers`, `silver.orders`, `silver.order_items`, `silver.products`, `silver.reviews`, `silver.clickstream`, `silver.campaigns`) | One row per entity, deduplicated, typed, validated — deliberately **not** a star schema. |
| **Gold** | **Kimball dimensional modeling** (star schema) + wide-table (OBT) business marts | The star schema (`gold.fact_order_items` + conformed dimensions) is the single analytical presentation layer; the OBT marts (`daily_sales`, `roas`, etc.) are pre-computed for dashboard-speed reads. |

The star schema lives **entirely in Gold**, not spread across Silver — this was a deliberate refactor decision (see project history: the silver layer was audited and the dimensional model consolidated into Gold specifically to keep Silver's contract simple: "cleansed, one row per entity," with no join/grain ambiguity).

### 4.2 Fact table: `gold.fact_order_items`

- **Grain:** one row per order line item sold.
- **Primary Key:** `sk_order_id` — a deterministic hash of `(source_system, order_id, product_id)` ensuring a true line-item primary key natively resolving 1:M relationships.
- **Foreign Keys:** `sk_product_id` → `gold.dim_product`, `sk_campaign_id` → `gold.dim_campaign`, `date_key` (INT, `YYYYMMDD`) → `gold.dim_date`, and `sk_customer_id` → `gold.dim_customer` for strict point-in-time mapping to the SCD2 state.
- **Partitioning:** `days(order_date)` — Iceberg hidden partitioning, so query engines prune entire files/directories for date-range queries without requiring a `to_date()`-cast predicate rewrite.
- **Bucketing:** `bucket(16, customer_id)` — co-locates rows for the same `customer_id` into the same physical bucket files, avoiding expensive shuffle/broadcast joins when this fact table is joined against `dim_customer` (also bucketed identically) during Gold-layer aggregation.

### 4.3 Conformed dimensions

| Dimension | Key Strategy | Grain | Notes |
|---|---|---|---|
| `gold.dim_customer` | `sk_customer_id` (Composite MD5 Hash of `customer_id` + `valid_from`); Business Key `customer_id` | One row per customer per validity period | **SCD Type 2** — see 4.4 |
| `gold.dim_product` | `sk_product_id` (SHA-256 SK); Business Key `product_id` | One row per product | Type 1 (no history tracked) |
| `gold.dim_campaign` | `sk_campaign_id` (SHA-256 SK); Business Key `campaign_id` | One row per campaign | Built from `silver.campaigns` (NiFi-ingested CSVs that passed the quality gate), deduplicated to the latest row per `campaign_id`. |
| `gold.dim_date` | `date_key` **INT** (`YYYYMMDD`), plus a human-readable `calendar_date` DATE column | One row per calendar day | Integer key chosen specifically to enable fast integer-hash joins against `fact_order_items.date_key`, avoiding runtime date-casting during joins. |

**Why deterministic surrogate keys everywhere, rather than auto-increment integers?** A deterministic hash of `(source_system, natural_key)` means the same customer/product always maps to the same surrogate key across pipeline re-runs, without needing a stateful key-generation sequence — and it isolates the analytics layer from upstream primary-key recycling or type changes, explicitly designed to support future multi-source-system integration (e.g., a second OLTP system) without key collisions.

### 4.4 Slowly Changing Dimensions

`gold.dim_customer` implements **SCD Type 2** via `batch/scd2_customer_dim.py`, using Iceberg's native `MERGE INTO` SQL rather than a manual delete-then-insert:

- **Tracked columns:** `full_name`, `email`, `segment`, `address` (`SCD_TRACKED_COLUMNS` in `scd2_customer_dim.py`).
- **Mechanics:** when any tracked column changes for a given `customer_id`, the existing "current" row is expired (`valid_to = current_timestamp()`, `is_current = False`) and a new row is inserted (`valid_from = current_timestamp()`, `is_current = True`). The surrogate key `sk_customer_id` uniquely identifies this specific historical version.
- **Contract:** the incoming DataFrame passed to `apply_scd2_merge()` must already be deduplicated to exactly one row per `customer_id` representing the latest known state for the run (enforced upstream via `ROW_NUMBER()` windowing in the batch job) — the SCD2 merge function itself does not do that deduplication.
- **Why this matters analytically:** historical `fact_order_items` rows join to the dimension state *as it existed at the time the order was placed* using time-based join logic to resolve the correct `sk_customer_id` — e.g., a customer's segment upgrade from `standard` to `vip` doesn't retroactively rewrite past orders' segment attribution, which is the entire point of Type 2 tracking.

All other dimensions (`dim_product`, `dim_campaign`, `dim_date`) are effectively **SCD Type 1** — no history, latest-state overwrite — a deliberate scoping decision reflecting that product/campaign attribute changes aren't currently a business-critical historical question for this dataset, unlike customer segment/address.

### 4.5 The OBT / mart layer

Eleven pre-aggregated Gold marts sit alongside the star schema: `daily_sales` (with 7-day/30-day rolling revenue averages), `top_products`, `customer_segments`, `conversion_rate`, `campaign_effectiveness`/`roas`, `product_sentiment`, `customer_clv`, `funnel_conversion`, `quarantine_summary`, and `quality_gate_summary`. This is a deliberate **pre-aggregation trade-off**: rather than making ClickHouse/Grafana compute rolling averages and funnel ratios live against the full fact table on every dashboard refresh, the batch job computes them once nightly. The documented cost is reduced ad-hoc drill-down flexibility — mitigated by also syncing the full `fact_order_items` star schema to ClickHouse, so an analyst can still run an ad-hoc "revenue by product category for campaign X" query without waiting on new pipeline work.

---

## 5. Pipeline Orchestration & Data Transformation

### 5.1 End-to-end lineage and dependency management

Lineage is captured automatically, not hand-documented, via **OpenLineage** instrumentation on every PySpark job (`spark.extraListeners = io.openlineage.spark.agent.OpenLineageSparkListener`, configured in `utils/spark_session.py`). Rather than each Spark job firing synchronous HTTP calls at the Marquez API — which would couple job execution latency to Marquez's availability — lineage events are routed through the **`spark.openlineage.transport.type=kafka`** transport into a dedicated `openlineage-events` Kafka topic. A standalone `marquez-kafka-consumer` service asynchronously drains that topic into Marquez's HTTP API, decoupling the Spark job's runtime from lineage-backend latency or downtime entirely. The dependency graph itself (Bronze → Silver → Gold → ClickHouse) is therefore observable in the Marquez UI (`localhost:3001`) without any manual lineage annotation in the pipeline code.

### 5.2 Failures, retries, alerting, and idempotency

- **Retries:** every stage task in the `nightly-batch` and `backfill` flows has `retries=3` with fixed delays of 10, 30 and 60 seconds. A stage fails by raising `BatchJobFailed` when its `make run-batch STAGE=...` exits non-zero, and only that stage is retried. The `cdc-poll` flow's `poll_table` task has `retries=3` (10 s delay with jitter); inside it, the CDC simulator handles a dropped Postgres connection with an explicit `_reconnect()` path (retrying the same call against an already-dead socket would never succeed). The ClickHouse loader retries individual ClickHouse queries.
- **Timeouts:** the standalone backfill CLI enforces `BACKFILL_JOB_TIMEOUT_SECONDS = 4 * 60 * 60` per window, so a hung `spark-submit` does not wedge a wide backfill. The Prefect stage tasks set no timeout.
- **Alerting:** Grafana alert rules are provisioned in `infra/grafana/provisioning/alerting/rules.yaml` ("High Quarantine Rate" on `gold.quality_gate_summary`, and "High Kafka Consumer Lag"), alongside the `data_quality.json` dashboard built on `gold.quarantine_summary` and `gold.quality_gate_summary`.
- **Idempotency:** every batch write is either a **partition-overwrite** (Silver/Gold via `write_overwrite_partitions`, safe to replay any number of times for the same partition) or a **stateful upsert-with-history** (the SCD2 `MERGE INTO`) — with the single deliberate exception of the Quarantine layer, which append-accumulates by design since it functions as a permanent audit log, not a queryable current-state table.
- **Run visibility:** a failed stage fails its Prefect task and flow run, visible in the Prefect UI, without stopping future scheduled runs. The flow's `publish-run-summary` task runs in a `finally` block and publishes the `_pipeline_runs` rows for that flow run as a Markdown artifact, so partial failures are summarised too.

### 5.3 Transformation logic division (Spark vs. batch orchestration)

Everything computationally heavy — CDC JSON parsing, deduplication, the Data Quality Gate, SCD2 merges, Gold aggregation, and the ClickHouse JDBC sync — lives in Spark code under `batch/`: `bronze_to_silver.py` (the `--stage` entrypoint, Bronze ingest and Silver logic), `gold_marts.py` (the Gold builders), `clickhouse_sync.py` (the staging-table + `EXCHANGE TABLES` load) and `scd2_customer_dim.py`. Each stage is its own `spark-submit`, reading its inputs back from Iceberg, so stages are independently re-runnable. The orchestration layer (Prefect) stays thin: it decides *when* to run, runs the four stages in order as separate tasks, retries a failed stage on its own, and links runs to lineage via `PARENT_RUN_ID` — it holds no transformation logic. Running `bronze_to_silver.py` without `--stage` still runs all stages in one process.

---

## 6. Data Quality, Governance, and Security

### 6.1 Data Quality framework

DataOne implements a custom **PySpark Quality Gate** (`src/dataone/quality/validators.py`) rather than an external framework like Great Expectations, applied at the Bronze → Silver/Gold boundary via `run_quality_gate()`:

1. **Null checks** — a configurable list of `required_columns` that must not be null (e.g., for `fact_order_items`: `order_id`, `customer_id`, `product_id`).
2. **Range checks** — a `column_bounds` dict of `(low, high)` tuples per column (e.g., `unit_price ≥ 0`, `quantity ≥ 1`, review `rating` between 1 and 5).
3. **Fail-fast validation of the rule set itself** — if a caller's rule references a column absent from the DataFrame (typically a typo), `run_quality_gate()` raises a clear `ValueError` immediately rather than letting Spark fail later with an opaque `AnalysisException` mid-query-plan.

**What happens when a check fails:** a row is never dropped and never null-inserted. It is tagged with a human-readable `_quarantine_reason` (e.g., `"null_check_failed"`, `"range_check_failed"`, or both concatenated if it fails multiple rules) and routed to a `quarantine.<table_name>` Iceberg table that mirrors the target table's schema plus the reason column. Eight datasets are currently covered end-to-end: `campaigns`, `customers`, `orders`, `products`, `order_items`, `clickstream`, `fact_order_items`, and `reviews` (full rule table in `docs/QUARANTINE_LAYER.md`). The rules are declared per dataset in `metadata/quality/*.json` and loaded by `run_quality_gate(df, dataset_name=...)`.

**Row-count reconciliation:** `reconcile_row_counts(source_count, landed_count)` provides an exact-match check between source and landed row counts, logging a loud warning (not silently tolerating) any mismatch — though this reconciliation check is currently invoked ad hoc rather than being wired as a hard gate that blocks a Gold-layer publish on mismatch. This is a known, acknowledged gap (surfaced in this project's own capstone code review): row-count reconciliation exists as a utility function but is not yet *enforced* as a blocking quality gate across every pipeline stage.

**Monitoring:** quarantined volume is aggregated nightly into `gold.quarantine_summary` by `batch_date`, `table_name`, and `failure_reason`, while overall passed vs quarantined rates are captured in `gold.quality_gate_summary` — feeding the `data_quality.json` Grafana dashboard directly, so DQ monitoring is dashboard-native rather than requiring log-scraping.

### 6.2 Access control, masking, and compliance — current state and known gaps

This section is written to the same standard of honesty as the rest of this document, including where the platform currently falls short:

- **No column-level PII masking is implemented in the codebase today.** A full-repository search for masking/encryption logic (`mask`, `encrypt`, `pii`, `gdpr`, `ccpa`) turns up no active masking of customer PII (`full_name`, `email`, `address` in `gold.dim_customer`) anywhere in the Silver→Gold transform or in the ClickHouse sync. This was flagged as a **major finding** in this project's own capstone code review as one of the two most significant outstanding issues (alongside the row-count reconciliation gap in 6.1), and remains open. The concrete remediation path — not yet implemented — is to hash or tokenize `email` and truncate/generalize `address` before the ClickHouse JDBC sync, since ClickHouse is the layer BI dashboards query directly and therefore the layer with the broadest exposure surface.
- **No encryption-at-rest or in-transit is configured** beyond what each service's default Docker image provides; this is consistent with the project's explicit scope as a local development/portfolio environment rather than a production deployment, but is called out here rather than left implicit, since a reader evaluating this as a production reference architecture needs to know it plainly.
- **Credentials** are centralized through `.env` / `src/dataone/config.py` rather than hardcoded in pipeline code, which is good practice. Passwords have no in-code defaults: `config.py` raises if a required one is unset, and `docker-compose.yml` requires `MARQUEZ_PASSWORD` and the MinIO credentials. `.env.example` still ships placeholder values (`changeme`) that must be rotated before any non-local use, all published ports except the NiFi reviews listener (`NIFI_REVIEWS_LISTEN_PORT`) are bound to `127.0.0.1`, and the Spark jobs reach MinIO as a bucket-scoped application user rather than the root account.
- **Access control** is effectively single-tenant: there is no row-level security, no per-role Grafana permission scheme, and no column-level grants in ClickHouse restricting which marts a given consumer can query. Every service in `docker-compose.yml`'s `core` profile is reachable by anything on the `dataone-net` Docker network.
- **GDPR/CCPA applicability:** because this project uses entirely synthetic, generator-produced data (`generators/*.py`, `Faker`-based), there is no real personal data at risk today — but the architecture itself (unmasked PII flowing all the way to the BI-facing ClickHouse layer) would not, as currently built, satisfy a real "right to erasure" or data-minimization requirement if pointed at real customer data. Treat the PII-masking gap above as the blocking item before any such migration.

### 6.3 Recommended remediation (not yet built)

For completeness and to keep this document forward-looking: the two most impactful next steps, in priority order, are (1) enforcing `reconcile_row_counts()` as a hard-failing gate rather than a logged warning, wired into `bronze_to_silver.py`'s write path for every Silver/Gold target, and (2) introducing a masking transform (deterministic hash for `email`, generalization for `address`) applied specifically at the Gold → ClickHouse sync boundary in `clickhouse_sync.py` (`load_clickhouse_marts()`), so raw PII never leaves the Iceberg lakehouse for the BI-facing serving layer. Both are scoped narrowly enough to land without restructuring the medallion architecture itself.

---

*Compiled from a direct audit of the `dataone-master` repository — source code, Docker/infra configuration, and existing project documentation (`docs/DATA_MODELING.md`, `docs/QUARANTINE_LAYER.md`, `docs/INFRASTRUCTURE_GUIDE.md`, `docs/TESTING_GUIDE.md`, `docs/ORCHESTRATOR.md`, `docs/future/Prefect_Migration_Plan.md`, `docs/future/Future_Improvements.md`). Original project by Eng. Ahmed Maher Al-Maqtari.*
