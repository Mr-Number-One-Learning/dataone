# DATA FLOW STEPS

This document provides a strict, step-by-step linear trace of how data propagates through the DataOne system, broken down explicitly by Data Source.

---

## Source 1: PostgreSQL CDC (Customers & Orders)

- **Step 1: Origin & Ingestion:** 
  - **Origin:** Operational OLTP PostgreSQL database.
  - **Trigger:** Interval-based polling orchestrated by Prefect (`cdc_poll.py` running every 5 seconds).
  - **Extraction:** A Python simulator queries `updated_at` watermarks, serializes the row into JSON, wraps it in a unified envelope (`{table_name, op, pk_column, pk_value, data_json, captured_at}`), and pushes it to the `orders-cdc` Kafka topic.
- **Step 2: Landing in Bronze (Raw):** 
  - **Landing Zone:** `bronze.orders_cdc`
  - **Metadata:** PySpark Structured Streaming appends the parsed envelope (including `captured_at`) every 30 seconds; malformed events go to `bronze.dead_letters`.
- **Step 3: Processing into Silver (Cleaned):** 
  - **Cleaning:** The nightly batch job parses the JSON payload. `customers` and `orders` are split into separate paths.
  - **Deduplication:** A `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY captured_at DESC)` window function deduplicates rows.
  - **Validation:** Passes through the PySpark Quality Gate. Rows with null PKs are routed to `quarantine.orders`.
  - **Target:** `silver.orders` and `silver.customers`.
- **Step 4: Integration & Joins:** 
  - **Joins:** Joined at the Gold layer in `batch/gold_marts.py` (`build_fact_order_items`). `silver.orders` joins with `silver.order_items` and `silver.products`, then with `gold.dim_customer` (the SCD2 version valid at `order_date`); `date_key` (`YYYYMMDD`) links to `gold.dim_date`.
- **Step 5: Consolidation into Gold (Analytics):** 
  - **Business Logic:** Surrogate keys are hashed. `silver.customers` undergoes a `MERGE INTO` operation to maintain SCD Type 2 history.
  - **Final Targets:** `gold.fact_order_items`, `gold.dim_customer`, and aggregates like `gold.daily_sales`.

---

## Source 2: Clickstream Activity

- **Step 1: Origin & Ingestion:** 
  - **Origin:** Web/App front-end simulator.
  - **Trigger:** Continuous high-velocity event streaming.
  - **Extraction:** Python `faker` generator pushing raw JSON event payloads (page views, checkout funnels) directly into the `clickstream` Kafka topic.
- **Step 2: Landing in Bronze (Raw):** 
  - **Landing Zone:** `bronze.clickstream`
  - **Metadata:** Written via Spark Structured Streaming with `processingTime="30 seconds"` micro-batches.
- **Step 3: Processing into Silver (Cleaned):** 
  - **Cleaning:** The nightly batch job enforces the Quality Gate (ensuring `session_id` and `event_type` are not null).
  - **Target:** `silver.clickstream`
- **Step 4: Integration & Joins:** 
  - **Joins:** None. The Gold conversion marts are aggregated from `silver.clickstream` alone (sessions and funnel events per day).
- **Step 5: Consolidation into Gold (Analytics):** 
  - **Business Logic:** Evaluates full checkout funnels (PageView → AddToCart → CheckoutStart → Purchase).
  - **Final Targets:** `gold.conversion_rate` and `gold.funnel_conversion`.

---

## Source 3: Product Reviews

- **Step 1: Origin & Ingestion:** 
  - **Origin:** The reviews generator POSTs each review to NiFi's plain-HTTP `ListenHTTP` endpoint (`/reviews`, port 9900).
  - **Trigger:** Push, per review (or direct MongoDB writes when `GEN_REVIEWS_INGEST_MODE=direct`).
  - **Extraction:** NiFi's `PutMongo` processor lands the documents in MongoDB; the nightly `ingest_bronze` stage reads new reviews from MongoDB.
- **Step 2: Landing in Bronze (Raw):** 
  - **Landing Zone:** `bronze.reviews`
  - **Metadata:** Schema is strictly typed, but accounts for missing fields inherent in NoSQL datasets.
- **Step 3: Processing into Silver (Cleaned):** 
  - **Cleaning:** Text processing and verification flags.
  - **Enrichment:** NLP processing derives a `sentiment_score` (-1.0 to 1.0 polarity) from the review body text.
  - **Target:** `silver.reviews`
- **Step 4: Integration & Joins:** 
  - **Joins:** Joined at the Gold layer with `silver.products` on `product_id`.
- **Step 5: Consolidation into Gold (Analytics):** 
  - **Business Logic:** Averages product ratings and sentiment scores, grouping by product category to derive brand health.
  - **Final Targets:** `gold.product_sentiment`.

---

## Source 4: Marketing Campaigns

- **Step 1: Origin & Ingestion:** 
  - **Origin:** Third-party marketing tool exports (CSV files).
  - **Trigger:** File drop watchers in Apache NiFi.
  - **Extraction:** NiFi ingests the CSV, standardizes the headers, and prepares it for the lakehouse.
- **Step 2: Landing in Bronze (Raw):** 
  - **Landing Zone:** `bronze.campaigns`
- **Step 3: Processing into Silver (Cleaned):** 
  - **Cleaning:** The quality gate checks required fields and non-negative budget/spend/clicks/conversions; passing rows go to `silver.campaigns`, failures to `quarantine.campaigns`.
  - **Target:** `silver.campaigns`
- **Step 4: Integration & Joins:** 
  - **Joins:** Joined directly into the Gold dimensional model. Fact records lookup their `campaign_id` to retrieve the associated surrogate key `sk_campaign_id`.
- **Step 5: Consolidation into Gold (Analytics):** 
  - **Business Logic:** Costs from campaigns are weighed against the revenue generated in `fact_order_items` to calculate Return on Ad Spend (ROAS).
  - **Final Targets:** `gold.dim_campaign` and `gold.roas` (and `gold.campaign_effectiveness`).
