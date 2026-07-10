-- Runs automatically on first ClickHouse container start, right after
-- 001_init_db.sql creates the database.
--
-- Column NAMES here must exactly match utils/schemas.py's GOLD_* definitions
-- — Spark's JDBC writer (batch/bronze_to_silver.py's load_clickhouse_marts(),
-- streaming/structured_streaming_job.py's foreachBatch sink) inserts by
-- column name, not position. If you change a column name in schemas.py,
-- change it here too.
--
-- MergeTree (not ReplacingMergeTree) for the batch-loaded marts: every load
-- is a full TRUNCATE + reload from the complete current Iceberg gold table (see
-- bronze_to_silver.py's main() for why it reloads from Iceberg rather than
-- pushing the in-memory, possibly backfill-narrowed DataFrame), so there's
-- never a need for merge-time deduplication — the table is just fully
-- replaced every run. The exception is the streaming-fed live_activity
-- table below, which IS a ReplacingMergeTree — see its comment.

CREATE TABLE IF NOT EXISTS dataone_marts.daily_sales
(
    -- Delta+ZSTD: monotonically increasing dates compress to near-nothing as deltas.
    sales_date      Date CODEC(Delta, ZSTD),
    order_count     Int64,
    total_revenue   Float64,
    revenue_7d_avg  Float64,
    revenue_30d_avg Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(sales_date)
ORDER BY sales_date;

CREATE TABLE IF NOT EXISTS dataone_marts.top_products
(
    product_id       Int64,
    product_name     String,
    sku              String,
    category         String,
    units_sold       Int64,
    revenue          Float64,
    rank_in_category Int32
)
ENGINE = MergeTree
ORDER BY (category, rank_in_category);

CREATE TABLE IF NOT EXISTS dataone_marts.customer_segments
(
    segment         String,
    customer_count  Int64,
    total_revenue   Float64,
    avg_order_value Float64
)
ENGINE = MergeTree
ORDER BY segment;

CREATE TABLE IF NOT EXISTS dataone_marts.conversion_rate
(
    activity_date         Date,
    sessions              Int64,
    checkout_completions  Int64,
    conversion_rate       Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY activity_date;

-- cost_per_conversion is Nullable: build_campaign_effectiveness() in
-- bronze_to_silver.py emits NULL when conversions = 0 (avoids a division by
-- zero) rather than substituting a fake 0/0 value.
CREATE TABLE IF NOT EXISTS dataone_marts.campaign_effectiveness
(
    campaign_id          Int64,
    name                 String,
    channel              String,
    spend                Float64,
    clicks               Int64,
    conversions          Int64,
    cost_per_conversion  Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY campaign_id;

-- Written by the STREAMING job (structured_streaming_job.py's foreachBatch
-- sink), not the batch job — appended every ~1 minute, never truncated.
-- This is the real-time dashboard pulse: activity counts, not revenue
-- (order_items/products needed for revenue aren't available from streaming
-- sources alone — see utils/schemas.py module docstring).
--
-- ReplacingMergeTree(window_end), not plain MergeTree: the streaming sink is
-- at-least-once, so the same 1-minute window can be inserted more than once
-- after a checkpoint replay. Windows are uniquely identified by their start,
-- so ORDER BY window_start dedupes replays at merge time (keeping the row
-- with the latest window_end as the version tiebreaker).
-- TTL: this is a real-time pulse table that would otherwise grow unbounded;
-- 7 days is more than any dashboard panel looks back.
CREATE TABLE IF NOT EXISTS dataone_marts.live_activity
(
    window_start          DateTime64(3),
    window_end            DateTime64(3),
    event_count           Int64,
    active_sessions       Int64,
    checkout_completions  Int64
)
ENGINE = ReplacingMergeTree(window_end)
PARTITION BY toYYYYMM(window_start)
ORDER BY window_start
TTL toDateTime(window_start) + INTERVAL 7 DAY DELETE;

-- Base fact table: lowest-grain transactional data (one row per order line item).
-- Enables ad-hoc Star Schema queries by joining with dim_product, dim_customer,
-- dim_campaign, and dim_date. Aggregate tables (daily_sales, top_products, etc.)
-- remain the primary source for operational dashboards; this table is for
-- analyst-driven exploration.
CREATE TABLE IF NOT EXISTS dataone_marts.fact_order_items
(
    sk_order_id    String,
    order_id       Int64,
    sk_customer_id String,
    customer_id    Int64,
    order_date     DateTime64(3),
    date_key       Int32,
    status         String,
    product_id     Int64,
    sk_product_id  String,
    quantity       Int64,
    unit_price     Float64,
    line_total     Float64,
    campaign_id    Int64,
    sk_campaign_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, product_id, customer_id);

-- Stage 3.1: Staging tables for atomic ClickHouse TRUNCATE + load via table swap
CREATE TABLE IF NOT EXISTS dataone_marts.fact_order_items_staging AS dataone_marts.fact_order_items;
CREATE TABLE IF NOT EXISTS dataone_marts.daily_sales_staging AS dataone_marts.daily_sales;
CREATE TABLE IF NOT EXISTS dataone_marts.top_products_staging AS dataone_marts.top_products;
CREATE TABLE IF NOT EXISTS dataone_marts.customer_segments_staging AS dataone_marts.customer_segments;
CREATE TABLE IF NOT EXISTS dataone_marts.conversion_rate_staging AS dataone_marts.conversion_rate;
CREATE TABLE IF NOT EXISTS dataone_marts.campaign_effectiveness_staging AS dataone_marts.campaign_effectiveness;

CREATE TABLE IF NOT EXISTS dataone_marts.product_sentiment
(
    product_id       Int64,
    review_count     Int64,
    avg_rating       Float64,
    avg_sentiment    Float64,
    pct_verified     Float64,
    product_name     String,
    category         String
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS dataone_marts.product_sentiment_staging AS dataone_marts.product_sentiment;

CREATE TABLE IF NOT EXISTS dataone_marts.dim_date
(
    date_key      Int32,
    calendar_date Date,
    day_of_week   Int32,
    day_name      String,
    week_of_year  Int32,
    month         Int32,
    month_name    String,
    quarter       Int32,
    year          Int32,
    is_weekend    UInt8
)
ENGINE = MergeTree
ORDER BY date_key;
CREATE TABLE IF NOT EXISTS dataone_marts.dim_date_staging AS dataone_marts.dim_date;

CREATE TABLE IF NOT EXISTS dataone_marts.dim_product
(
    sk_product_id String,
    product_id    Int64,
    sku           String,
    product_name  String,
    category      String,
    current_price Float64
)
ENGINE = MergeTree
ORDER BY product_id;
CREATE TABLE IF NOT EXISTS dataone_marts.dim_product_staging AS dataone_marts.dim_product;

CREATE TABLE IF NOT EXISTS dataone_marts.dim_campaign
(
    sk_campaign_id String,
    campaign_id    Int64,
    name           String,
    channel        String,
    start_date     Date,
    end_date       Date
)
ENGINE = MergeTree
ORDER BY campaign_id;
CREATE TABLE IF NOT EXISTS dataone_marts.dim_campaign_staging AS dataone_marts.dim_campaign;

CREATE TABLE IF NOT EXISTS dataone_marts.customer_clv
(
    customer_id           Int64,
    full_name             String,
    total_orders          Int64,
    total_spend           Float64,
    first_order_date      Date,
    last_order_date       Date,
    avg_order_value       Float64,
    days_since_last_order Int32,
    segment               String
)
ENGINE = MergeTree
ORDER BY customer_id;
CREATE TABLE IF NOT EXISTS dataone_marts.customer_clv_staging AS dataone_marts.customer_clv;

CREATE TABLE IF NOT EXISTS dataone_marts.funnel_conversion
(
    activity_date             Date,
    page_view                 Int64,
    add_to_cart               Int64,
    checkout_start            Int64,
    checkout_complete         Int64,
    cart_to_checkout_rate     Nullable(Float64),
    checkout_to_purchase_rate Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY activity_date;
CREATE TABLE IF NOT EXISTS dataone_marts.funnel_conversion_staging AS dataone_marts.funnel_conversion;

CREATE TABLE IF NOT EXISTS dataone_marts.roas
(
    campaign_id        Int64,
    name               String,
    channel            String,
    start_date         Date,
    end_date           Date,
    spend              Float64,
    attributed_revenue Float64,
    roas               Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY campaign_id;
CREATE TABLE IF NOT EXISTS dataone_marts.roas_staging AS dataone_marts.roas;

-- TTL: quarantine rows are triage material, not history — 90 days is plenty
-- to investigate a bad batch, and keeps this diagnostics table from growing
-- forever alongside the pipeline.
CREATE TABLE IF NOT EXISTS dataone_marts.quarantine_summary
(
    -- Delta+ZSTD: monotonically increasing dates compress to near-nothing as deltas.
    batch_date       Date CODEC(Delta, ZSTD),
    table_name       String,
    -- ZSTD(3): free-text failure reasons are wide and highly repetitive.
    failure_reason   String CODEC(ZSTD(3)),
    row_count        Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(batch_date)
ORDER BY (batch_date, table_name, failure_reason)
TTL batch_date + INTERVAL 90 DAY DELETE;
CREATE TABLE IF NOT EXISTS dataone_marts.quarantine_summary_staging AS dataone_marts.quarantine_summary;

CREATE TABLE IF NOT EXISTS dataone_marts.quality_gate_summary (
    batch_date Date,
    table_name String,
    passed_count Int64,
    quarantined_count Int64
) ENGINE = MergeTree()
ORDER BY (batch_date, table_name);

CREATE TABLE IF NOT EXISTS dataone_marts.quality_gate_summary_staging AS dataone_marts.quality_gate_summary;


-- -----------------------------------------------------------------------------
-- Real-Time Streaming Marts (Lambda Architecture)
-- -----------------------------------------------------------------------------

-- 1. Kafka Source Table — Raw CDC event consumer
CREATE TABLE IF NOT EXISTS dataone_marts._kafka_orders_cdc (
    message String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'orders-cdc',
    kafka_group_name = 'clickhouse_realtime_consumer_7',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 1000;

-- 2. Parsed Orders Landing Table — Stores denormalized order events
CREATE TABLE IF NOT EXISTS dataone_marts.rt_orders_raw (
    order_id      Int64,
    customer_id   Nullable(Int64),
    order_date    DateTime64(3),
    status        String,
    campaign_id   Nullable(Int64),
    op            String,
    captured_at   DateTime64(3),
    _inserted_at  DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(captured_at)
ORDER BY order_id
PARTITION BY toYYYYMM(order_date)
TTL toDateTime(order_date) + INTERVAL 30 DAY DELETE;

-- 3. Materialized View — CDC Parser
CREATE MATERIALIZED VIEW IF NOT EXISTS dataone_marts.mv_orders_cdc_parser
TO dataone_marts.rt_orders_raw AS
SELECT
    JSONExtractInt(JSONExtractString(message, 'data'), 'order_id')    AS order_id,
    JSONExtractInt(JSONExtractString(message, 'data'), 'customer_id') AS customer_id,
    parseDateTimeBestEffortOrNull(JSONExtractString(JSONExtractString(message, 'data'), 'order_date')) AS order_date,
    JSONExtractString(JSONExtractString(message, 'data'), 'status')   AS status,
    JSONExtractInt(JSONExtractString(message, 'data'), 'campaign_id') AS campaign_id,
    JSONExtractString(message, 'op') AS op,
    parseDateTimeBestEffortOrNull(JSONExtractString(message, 'captured_at')) AS captured_at
FROM dataone_marts._kafka_orders_cdc
WHERE JSONExtractString(message, 'table') = 'orders'
  AND JSONExtractInt(JSONExtractString(message, 'data'), 'order_id') > 0;

-- 4a. Order Velocity Tracker
CREATE TABLE IF NOT EXISTS dataone_marts.rt_orders_per_minute (
    window_start   DateTime,
    order_count    AggregateFunction(count, UInt64),
    _inserted_at   DateTime DEFAULT now()
) ENGINE = AggregatingMergeTree
ORDER BY window_start
TTL window_start + INTERVAL 7 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS dataone_marts.mv_orders_per_minute
TO dataone_marts.rt_orders_per_minute AS
SELECT
    toStartOfMinute(parseDateTimeBestEffortOrNull(JSONExtractString(JSONExtractString(message, 'data'), 'order_date'))) AS window_start,
    countState() AS order_count
FROM dataone_marts._kafka_orders_cdc
WHERE JSONExtractString(message, 'table') = 'orders' AND JSONExtractString(message, 'op') = 'insert'
GROUP BY window_start;

CREATE TABLE IF NOT EXISTS dataone_marts.rt_revenue_estimate (
    window_start     DateTime,
    new_order_count  UInt64,
    est_revenue      Float64
) ENGINE = SummingMergeTree()
ORDER BY window_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS dataone_marts.mv_revenue_estimate
TO dataone_marts.rt_revenue_estimate AS
SELECT
    toStartOfMinute(parseDateTimeBestEffortOrNull(JSONExtractString(JSONExtractString(message, 'data'), 'order_date'))) AS window_start,
    toUInt64(count()) AS new_order_count,
    toFloat64(count() * 55.5) AS est_revenue
FROM dataone_marts._kafka_orders_cdc
WHERE JSONExtractString(message, 'table') = 'orders' 
  AND JSONExtractString(message, 'op') = 'insert'
GROUP BY window_start;

-- 4c. Live Status Distribution
CREATE TABLE IF NOT EXISTS dataone_marts.rt_order_status_counts (
    status         String,
    order_count    UInt64
) ENGINE = SummingMergeTree()
ORDER BY status;

CREATE MATERIALIZED VIEW IF NOT EXISTS dataone_marts.mv_order_status_counts
TO dataone_marts.rt_order_status_counts AS
SELECT
    JSONExtractString(JSONExtractString(message, 'data'), 'status') AS status,
    toUInt64(count()) AS order_count
FROM dataone_marts._kafka_orders_cdc
WHERE JSONExtractString(message, 'table') = 'orders'
  AND JSONExtractString(message, 'op') = 'insert'
GROUP BY status;
