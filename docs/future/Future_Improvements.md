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
Originally only the `live_activity` table in ClickHouse (fed by the Structured Streaming job) gave real-time visibility, for sessions, events, and checkout completions; order metrics waited for the nightly batch.

**What was built:**
- ClickHouse reads the Kafka CDC topic *directly* through a `Kafka`-engine table (`_kafka_orders_cdc`).
- Materialized views (`mv_orders_cdc_parser`, `mv_orders_per_minute`, `mv_revenue_estimate`, `mv_order_status_counts`) feed the Real-Time Operations dashboard: orders in the last 5 minutes, order velocity, revenue estimate, status breakdown.

**Why:** This extends the existing streaming capability from clickstream-only metrics to full order-level visibility, completing the **Lambda Architecture** — rigorous batch historical layer (Iceberg) combined with a fast, real-time streaming layer for immediate operational visibility.


