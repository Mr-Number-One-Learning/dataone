# Future Improvements & Roadmap

While the DataOne project successfully implements a robust Medallion Architecture and scores an "Outstanding" evaluation, the following recommendations represent a roadmap to elevate the platform to a Senior/Lead Data Engineering level.

These improvements focus on democratizing data access, improving observability, reducing latency, and enforcing strict software engineering best practices.

---

## 1. Analytics Engineering with `dbt` (Data Build Tool)
Currently, all business marts (e.g., `daily_sales`, `roas`, `funnel_conversion`) are built using PySpark DataFrame operations in the batch job. 

**The Improvement:** 
- Use PySpark strictly for infrastructure-level heavy lifting: parsing CDC JSON, deduplication, and Iceberg layer management (Bronze $\rightarrow$ Silver).
- Offload the Gold layer business logic to **dbt** using SQL.
- Implement `dbt-spark` or `dbt-clickhouse` to manage the final aggregations.

**Why:** It demonstrates a modern separation of concerns. Data Engineers handle the ingestion and infrastructure, while Analytics Engineers and Data Analysts use dbt to define and test business metrics using native SQL.



---

## 2. Real-Time Streaming Marts (Lambda Architecture) ✅ IMPLEMENTED
The project already implements a partial Lambda Architecture: the `live_activity` table in ClickHouse (fed by the Structured Streaming job) provides real-time operational visibility for sessions, events, and checkout completions. However, the CDC stream (orders, customers) is ingested into Bronze in real-time but its business metrics are only aggregated during the nightly batch run.

**The Improvement:**
- Connect ClickHouse *directly* to the Kafka CDC topic using the `Kafka` table engine.
- Create ClickHouse Materialized Views to compute live, real-time operational dashboards (e.g., tracking orders placed in the last 5 minutes, real-time revenue).

**Why:** This would extend the existing streaming capability from clickstream-only metrics to full order-level visibility, completing the **Lambda Architecture** — rigorous batch historical layer (Iceberg) combined with a fast, real-time streaming layer for immediate operational visibility.


