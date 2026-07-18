# DATA ENGINEERING ARCHITECTURE

This technical manual details the end-to-end architectural design decisions, data flow mechanics, table optimizations, and structural integrity of the DataOne platform. 

---

## 1. System Architecture & High-Level Data Flow

The DataOne platform is built entirely on a self-hosted, local-first Docker architecture without relying on managed cloud services. Data moves through the system utilizing decoupled ingestion patterns to support varying data shapes and velocities.

### Core Infrastructure & Transport
- **Message Broker (Streaming):** **Apache Kafka (KRaft mode)**. Used to ingest continuous, unbounded streams of CDC data and clickstream telemetry. KRaft mode is used to eliminate ZooKeeper, reducing the RAM footprint.
- **Batch Data Mover:** **Apache NiFi**. Manages back-pressure, schema-checking, and HTTP listeners for semi-structured documents (reviews) and external file drops (campaign CSVs).
- **OLTP & Catalog:** **PostgreSQL**. Acts dual-purpose as the primary source database for operational data AND as the Iceberg JDBC Catalog (`org.apache.iceberg.jdbc.JdbcCatalog`).
- **Processing Engine:** **PySpark / Spark Structured Streaming**. Unified engine for both continuous Kafka micro-batching and heavy nightly batch ETL (SCD2 merges, aggregations).
- **Serving Layer:** **ClickHouse**. High-performance OLAP engine receiving synced data via JDBC from the Gold layer to serve sub-second aggregations.
- **Orchestration:** **Prefect**. Lightweight DAG orchestration executing flows like `nightly_batch.py` and `cdc_poll.py` via an integrated worker pool.

---

## 2. The Medallion Architecture Deep Dive

The platform strictly enforces the **Medallion Architecture (Bronze → Silver → Gold)** within **Apache Iceberg**, augmented by a parallel **Quarantine** layer.

### Bronze Layer (Raw)
- **Format & Enforcement:** Append-only Iceberg tables. No destructive operations.
- **Strategy:** Employs **Schema-on-Read** for complex or changing upstream systems (e.g., CDC events are encapsulated in a flat JSON payload column `data_json` wrapped in a uniform envelope `{table, op, pk_column, captured_at}`). This absorbs upstream DDL changes without pipeline failure.
- **Ingestion:** High-frequency Spark Structured Streaming (`outputMode("append")`) or NiFi batch drops. 

### Silver Layer (Cleaned / Conformed)
- **Format & Enforcement:** Cleansed, 3NF-style curated entities. 
- **Strategy:** Applies strict **Deduplication** (using `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY captured_at DESC) = 1`). Data undergoes PySpark Data Quality Gates (null checks, range bounds).
- **Harmonization:** JSON payloads are flattened, data types are cast properly, and any row failing validation is discarded from Silver and routed to `quarantine.*`.

### Gold Layer (Business / Analytics)
- **Format & Enforcement:** Kimball Dimensional Model (Star Schema) paired with Wide-Table (OBT) business marts.
- **Strategy:** All fact tables and conformed dimensions are generated here. Contains complex business logic, Slowly Changing Dimensions (SCD Type 2), and pre-aggregated dashboards marts (e.g., `daily_sales`, `conversion_rate`) tailored specifically for ClickHouse consumption.

### Master Table Mapping

| Table Name | Layer | Source Table(s) | Update Frequency | Key Transformations |
|---|---|---|---|---|
| `bronze.orders_cdc` | Bronze | Postgres `orders`, `customers` | Micro-batch (30s) | Flat JSON envelope landing. No transformation. |
| `bronze.clickstream` | Bronze | Kafka `clickstream` | Micro-batch (30s) | Direct event append. |
| `bronze.reviews` | Bronze | MongoDB `reviews` (NiFi) | Nightly Batch | Document ingestion. |
| `silver.orders` | Silver | `bronze.orders_cdc` | Nightly Batch | JSON parsing, deduplication, DQ null/range checks. |
| `silver.customers` | Silver | `bronze.orders_cdc` | Nightly Batch | JSON parsing, deduplication, DQ validation. |
| `silver.reviews` | Silver | `bronze.reviews` | Nightly Batch | NLP sentiment analysis scoring, flattening. |
| `gold.fact_order_items`| Gold | `silver.orders`, `products` | Nightly Batch | Hash SK generation, dimensional foreign key binding. |
| `gold.dim_customer` | Gold | `silver.customers` | Nightly Batch | SCD Type 2 `MERGE INTO`, tracked column expirations. |
| `gold.daily_sales` | Gold | `gold.fact_order_items` | Nightly Batch | Date grouping, 7-day/30-day rolling revenue window functions. |

---

## 3. Data Integration & Joining Strategy

### Where Joins Occur
Data sources are joined **strictly at the Gold Layer**.
- **Silver** is restricted to 3NF entities representing single concepts (a customer, an order, a review).
- **Gold** executes the heavy integration: `gold.fact_order_items` joins transaction data with product metadata, campaigns, and customer dimension keys.

### Join Keys and Strategies
- **Deterministic Surrogate Keys (SKs):** All dimension-to-fact joins utilize deterministic SHA-256 hash surrogate keys (e.g., `sk_customer_id = MD5(customer_id + valid_from)`). This isolates analytics from upstream key recycling.
- **Integer Date Keys:** Dates are joined using `INT` representations (`YYYYMMDD` like `20260712`) avoiding runtime casting overhead.
- **Late-Arriving Data:** Handled seamlessly because CDC payloads maintain their `updated_at` watermarks, and the nightly job deduplicates based on the latest valid timestamp before merging into Gold.

---

## 4. Storage Engine & Table Design 

### Apache Iceberg (Lakehouse)
Iceberg is chosen for ACID transactions on local disk (via `HadoopFileIO`) bypassing the need for a live HDFS cluster.
- **Partitioning Strategy:** Uses **Hidden Partitioning**. For example, `gold.fact_order_items` is partitioned by `days(order_date)`. The engine handles the extraction automatically, so analysts do not need to rewrite queries with `to_date()` predicates to trigger partition pruning.
- **Clustering/Bucketing:** Heavily joined tables (`gold.fact_order_items` and `gold.dim_customer`) use `bucket(16, customer_id)`. This co-locates identical customers into identical buckets, completely eliminating expensive network shuffles (Broadcast/Shuffle-Hash) during the nightly ETL run.
- **TTL/Lifecycle:** Managed via Iceberg snapshot expiration. Old snapshots are periodically expired to clear unreferenced Parquet files from disk, keeping the metadata tree lean.

### ClickHouse (Serving)
ClickHouse `MergeTree` engines host the final serving copies of Gold marts.
- **Primary & Sorting Keys:** ClickHouse does not use traditional B-Trees. Data is physically sorted on disk by the `ORDER BY` clause in the table DDL (e.g., `ORDER BY (sales_date)` or `ORDER BY (category, product_id)`). This enables sparse primary indexes to skip millions of rows per block instantly.

---

## 5. Indexing & Performance Tuning

1. **Min/Max Column Statistics:** Iceberg automatically stores upper/lower bounds for every column within its manifest files. During querying, Spark evaluates `WHERE unit_price > 100` against the manifests first, skipping entire Parquet files that don't contain matching bounds without opening them.
2. **Bucketed Joins:** Mentioned above, the 16-bucket setup on `customer_id` is the primary performance tuning mechanic for the heaviest pipeline stage (SCD2 merges and Fact table generation).
3. **Pre-Aggregation (Marts):** By materializing rolling averages and funnel metrics inside `batch_to_silver.py`, the ClickHouse server acts simply as a lightning-fast data retrieval layer for Grafana, requiring zero complex runtime analytical calculations.
