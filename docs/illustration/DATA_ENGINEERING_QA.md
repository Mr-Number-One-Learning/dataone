# DATA ENGINEERING QA FAQ

This companion document acts as a technical FAQ for onboarding engineers and addressing architecture reviews, focusing on the "Whys", edge cases, scaling, and governance.

---

### Architectural "Whys"

**Q: Why did we choose a MergeTree engine (ClickHouse) over standard storage for the serving layer?**  
**A:** Iceberg/Parquet are optimized for massive batch throughput, but reading directly from Iceberg using a query engine (like Presto) requires significant RAM and distributed computing power. ClickHouse's `MergeTree` engine physically sorts data on disk by the primary key, utilizing sparse indexing. This allows a single, memory-constrained laptop container to execute sub-second aggregations on millions of rows for live Grafana dashboard refreshes.

**Q: Why are data sources joined at the Gold layer instead of the Silver layer?**  
**A:** The Silver layer's strict contract is "Cleansed, 3NF Entities" (e.g., one row per customer, one row per order). If we joined order data with customer demographic data in Silver, we would create a massive, denormalized table with duplicated customer fields. By pushing joins to the Gold layer (building a Star Schema), we maintain data integrity in Silver and handle complex business logic (like Slowly Changing Dimensions) exactly when we map facts to dimensions.

**Q: Why do we use Prefect instead of Apache Airflow for orchestration?**  
**A:** Given our constraint of running locally via Docker Compose, Airflow's footprint (Scheduler, Webserver, PostgreSQL, Celery/Redis) is too heavy. Prefect provides native Python execution, a lightweight footprint operating from our existing Postgres instance, and robust task-level retries without spinning up multiple heavy JVMs or UI services.

---

### Edge Cases & Failure Modes

**Q: How does the pipeline handle a schema evolution change from an upstream API/DB?**  
**A:** For heavy operational data (CDC), we employ a Schema-on-Read approach in Bronze. The changing data is serialized as a JSON string (`data_json`) within a static envelope `{table_name, pk, data_json}`. If an upstream DBA adds a column to the Postgres `orders` table, the pipeline does not break. The new column simply rides along in the JSON payload until an engineer updates the PySpark batch job to parse it out into the Silver layer. Furthermore, Apache Iceberg supports native schema evolution without rewriting data files.

**Q: What happens if duplicate data enters the Silver layer?**  
**A:** Duplicates cannot logically persist in Silver. During the PySpark Bronze-to-Silver ETL, a deduplication window function is applied: `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY captured_at DESC)`. This guarantees that only the single latest state of an entity makes it into the curated Silver table, regardless of how many duplicate micro-batch events arrived in Bronze.

**Q: What happens if an ingested row is completely invalid?**  
**A:** The pipeline features a PySpark Data Quality Gate. If a row violates range checks (e.g., negative price) or null constraints, it is not dropped silently. It is tagged with a `_quarantine_reason` and pushed to a persistent `quarantine.*` table to maintain a permanent audit log of failures for engineering triage.

---

### Performance & Scaling

**Q: How does our indexing strategy prevent full table scans on daily queries?**  
**A:** 
1. **Hidden Partitioning:** Apache Iceberg partitions fact data by `days(order_date)`. Query engines automatically prune irrelevant directories based on timestamps.
2. **Min/Max Manifest Statistics:** Iceberg tracks column metrics at the file level. Queries filtering by `customer_id` or `price` skip entire Parquet files that don't contain matching bounds.
3. **Bucketing:** `gold.dim_customer` and `gold.fact_order_items` are both clustered using `bucket(16, customer_id)`. During heavy ETL joins, Spark reads identically bucketed data concurrently without needing to perform a full cluster shuffle over the network.

---

### Data Governance

**Q: How is PII handled or masked across the Medallion layers?**  
**A:** *Known System Gap.* Currently, there is **no column-level PII masking implemented**. Unmasked customer demographics (`email`, `full_name`, `address`) flow directly from Bronze into the BI-facing ClickHouse Gold tables. This is explicitly documented in the architectural decision logs (`DECISION.md`) as the primary outstanding security debt. The planned remediation is to enforce cryptographic hashing (tokenization) on the `email` column and truncation on the `address` column at the Silver → Gold PySpark transform boundary before it syncs to ClickHouse.
