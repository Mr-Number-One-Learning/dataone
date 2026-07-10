"""
Single source of truth for the Iceberg table schemas used across the
streaming job, batch job, and SCD2 merge. Exists so the bronze *writer*
(streaming job) and bronze *reader* (batch job) can't silently drift out of
sync on column names/types — both import from here instead of each hardcoding
their own copy.

Design note on bronze.orders_cdc: the CDC simulator watches BOTH `customers`
and `orders` Postgres tables (see ingestion/cdc_simulator.py) onto the same
Kafka topic, and those two source tables have entirely different columns.
Rather than force a wide, mostly-null union schema, bronze.orders_cdc stores
the event envelope with the row payload as a raw JSON string
(`data_json`) — schema-on-read, parsed downstream in the batch transform once
we know which table a given row came from. bronze.clickstream doesn't have
this problem (one consistent event shape, see generators/clickstream_generator.py)
so it gets a normal flat schema.

Design note on order_items/products: these have no CDC or streaming path at
all currently — orders_generator.py bulk-loads them straight into Postgres
and nothing watches them for changes. The batch job reads them directly via
Spark JDBC (a full extract each run) rather than through the lakehouse bronze
layer. This is a deliberate, flagged scope limit: incremental/CDC extraction
of order_items would need its own watermark mechanism, not built here.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# BRONZE — raw, append-only, as close to the source shape as practical
# ---------------------------------------------------------------------------

BRONZE_ORDERS_CDC = {
    "layer": "bronze",
    "table": "orders_cdc",
    "columns": [
        ("table_name", "STRING"),
        ("op", "STRING"),
        ("pk_column", "STRING"),
        ("pk_value", "STRING"),
        ("data_json", "STRING"),
        ("captured_at", "TIMESTAMP"),
    ],
    "partition_by": ["days(captured_at)"],
}

BRONZE_CLICKSTREAM = {
    "layer": "bronze",
    "table": "clickstream",
    "columns": [
        ("event_id", "STRING"),
        ("session_id", "STRING"),
        ("event_type", "STRING"),
        ("product_id", "BIGINT"),
        ("customer_id", "BIGINT"),  # nullable — absent in the JSON for anonymous events
        ("ts", "TIMESTAMP"),
    ],
    "partition_by": ["days(ts)"],
}

BRONZE_DEAD_LETTERS = {
    "layer": "bronze",
    "table": "dead_letters",
    "columns": [
        ("value", "STRING"),
        ("source_topic", "STRING"),
        ("failed_at", "TIMESTAMP"),
    ],
    "partition_by": ["days(failed_at)"],
}

BRONZE_CAMPAIGNS = {
    "layer": "bronze",
    "table": "campaigns",
    "columns": [
        ("campaign_id", "BIGINT"),
        ("name", "STRING"),
        ("channel", "STRING"),
        ("start_date", "DATE"),
        ("end_date", "DATE"),
        ("budget", "DOUBLE"),
        ("spend", "DOUBLE"),
        ("clicks", "BIGINT"),
        ("conversions", "BIGINT"),
        ("ingested_at", "TIMESTAMP"),
    ],
    "partition_by": None,  # small reference-sized table, partitioning would be overkill
}

BRONZE_REVIEWS = {
    "layer": "bronze",
    "table": "reviews",
    "columns": [
        ("review_id", "STRING"),
        ("product_id", "BIGINT"),
        ("customer_id", "BIGINT"),    # nullable — anonymous reviews
        ("rating", "INT"),
        ("title", "STRING"),
        ("body", "STRING"),
        ("verified_purchase", "BOOLEAN"),
        ("submitted_at", "TIMESTAMP"),
        ("ingested_at", "TIMESTAMP"),
    ],
    "partition_by": ["months(submitted_at)"],
}

# ---------------------------------------------------------------------------
# SILVER — curated: deduped, joined, typed (cleansed 3NF entities only;
# the Kimball Star Schema lives entirely in the Gold layer below)
# ---------------------------------------------------------------------------

SILVER_REVIEWS = {
    "layer": "silver",
    "table": "reviews",
    "columns": [
        ("review_id", "STRING"),
        ("product_id", "BIGINT"),
        ("customer_id", "BIGINT"),
        ("rating", "INT"),
        ("title", "STRING"),
        ("body", "STRING"),
        ("verified_purchase", "BOOLEAN"),
        ("submitted_at", "TIMESTAMP"),
        ("ingested_at", "TIMESTAMP"),
        ("sentiment_score", "DOUBLE"),
    ],
    "partition_by": ["months(submitted_at)"],
}

# ---------------------------------------------------------------------------
# GOLD — business-ready marts, also loaded into ClickHouse for the dashboard
# ---------------------------------------------------------------------------

SILVER_CUSTOMERS = {
    "layer": "silver",
    "table": "customers",
    "columns": [
        ("customer_id", "BIGINT"),
        ("full_name", "STRING"),
        ("email", "STRING"),
        ("segment", "STRING"),
        ("address", "STRING"),
        ("updated_at", "TIMESTAMP"),
        ("captured_at", "TIMESTAMP"),
    ],
    "partition_by": ["bucket(16, customer_id)"],
}

SILVER_ORDERS = {
    "layer": "silver",
    "table": "orders",
    "columns": [
        ("order_id", "BIGINT"),
        ("customer_id", "BIGINT"),
        ("order_date", "TIMESTAMP"),
        ("status", "STRING"),
        ("campaign_id", "BIGINT"),
        ("captured_at", "TIMESTAMP"),
    ],
    "partition_by": ["days(order_date)"],
}

GOLD_DAILY_SALES = {
    "layer": "gold",
    "table": "daily_sales",
    "columns": [
        ("sales_date", "DATE"),
        ("order_count", "BIGINT"),
        ("total_revenue", "DOUBLE"),
        ("revenue_7d_avg", "DOUBLE"),
        ("revenue_30d_avg", "DOUBLE"),
    ],
    "partition_by": ["months(sales_date)"],
}

GOLD_TOP_PRODUCTS = {
    "layer": "gold",
    "table": "top_products",
    "columns": [
        ("product_id", "BIGINT"),
        ("product_name", "STRING"),
        ("sku", "STRING"),
        ("category", "STRING"),
        ("units_sold", "BIGINT"),
        ("revenue", "DOUBLE"),
        ("rank_in_category", "INT"),
    ],
    "partition_by": None,
}

GOLD_CUSTOMER_SEGMENTS = {
    "layer": "gold",
    "table": "customer_segments",
    "columns": [
        ("segment", "STRING"),
        ("customer_count", "BIGINT"),
        ("total_revenue", "DOUBLE"),
        ("avg_order_value", "DOUBLE"),
    ],
    "partition_by": None,
}

GOLD_CONVERSION_RATE = {
    "layer": "gold",
    "table": "conversion_rate",
    "columns": [
        ("activity_date", "DATE"),
        ("sessions", "BIGINT"),
        ("checkout_completions", "BIGINT"),
        ("conversion_rate", "DOUBLE"),
    ],
    "partition_by": ["months(activity_date)"],
}

GOLD_CAMPAIGN_EFFECTIVENESS = {
    "layer": "gold",
    "table": "campaign_effectiveness",
    "columns": [
        ("campaign_id", "BIGINT"),
        ("name", "STRING"),
        ("channel", "STRING"),
        ("spend", "DOUBLE"),
        ("clicks", "BIGINT"),
        ("conversions", "BIGINT"),
        ("cost_per_conversion", "DOUBLE"),
    ],
    "partition_by": None,
}

GOLD_PRODUCT_SENTIMENT = {
    "layer": "gold",
    "table": "product_sentiment",
    "columns": [
        ("product_id", "BIGINT"),
        ("review_count", "BIGINT"),
        ("avg_rating", "DOUBLE"),
        ("avg_sentiment", "DOUBLE"),
        ("pct_verified", "DOUBLE"),
        ("product_name", "STRING"),
        ("category", "STRING"),
    ],
    "partition_by": None,
}

GOLD_DIM_CUSTOMER = {
    "layer": "gold",
    "table": "dim_customer",
    "columns": [
        ("sk_customer_id", "STRING"),
        ("customer_id", "BIGINT"),
        ("full_name", "STRING"),
        ("email", "STRING"),
        ("segment", "STRING"),
        ("address", "STRING"),
        ("valid_from", "TIMESTAMP"),
        ("valid_to", "TIMESTAMP"),
        ("is_current", "BOOLEAN"),
    ],
    "partition_by": ["bucket(16, customer_id)"],
}

GOLD_FACT_ORDER_ITEMS = {
    "layer": "gold",
    "table": "fact_order_items",
    "columns": [
        ("sk_order_id", "STRING"),
        ("order_id", "BIGINT"),
        ("sk_customer_id", "STRING"),
        ("customer_id", "BIGINT"),
        ("order_date", "TIMESTAMP"),
        ("date_key", "INT"),
        ("status", "STRING"),
        ("product_id", "BIGINT"),
        ("sk_product_id", "STRING"),
        ("quantity", "BIGINT"),
        ("unit_price", "DOUBLE"),
        ("line_total", "DOUBLE"),
        ("campaign_id", "BIGINT"),
        ("sk_campaign_id", "STRING"),
    ],
    "partition_by": ["days(order_date)", "bucket(16, customer_id)"],
}

GOLD_DIM_DATE = {
    "layer": "gold",
    "table": "dim_date",
    "columns": [
        ("date_key", "INT"),
        ("calendar_date", "DATE"),
        ("day_of_week", "INT"),
        ("day_name", "STRING"),
        ("week_of_year", "INT"),
        ("month", "INT"),
        ("month_name", "STRING"),
        ("quarter", "INT"),
        ("year", "INT"),
        ("is_weekend", "BOOLEAN"),
    ],
    "partition_by": ["year"],
}

GOLD_DIM_PRODUCT = {
    "layer": "gold",
    "table": "dim_product",
    "columns": [
        ("sk_product_id", "STRING"),
        ("product_id", "BIGINT"),
        ("sku", "STRING"),
        ("product_name", "STRING"),
        ("category", "STRING"),
        ("current_price", "DOUBLE"),
    ],
    "partition_by": None,
}

GOLD_DIM_CAMPAIGN = {
    "layer": "gold",
    "table": "dim_campaign",
    "columns": [
        ("sk_campaign_id", "STRING"),
        ("campaign_id", "BIGINT"),
        ("name", "STRING"),
        ("channel", "STRING"),
        ("start_date", "DATE"),
        ("end_date", "DATE"),
    ],
    "partition_by": None,
}

GOLD_CUSTOMER_CLV = {
    "layer": "gold",
    "table": "customer_clv",
    "columns": [
        ("customer_id", "BIGINT"),
        ("full_name", "STRING"),
        ("total_orders", "BIGINT"),
        ("total_spend", "DOUBLE"),
        ("first_order_date", "DATE"),
        ("last_order_date", "DATE"),
        ("avg_order_value", "DOUBLE"),
        ("days_since_last_order", "INT"),
        ("segment", "STRING"),
    ],
    "partition_by": None,
}

GOLD_FUNNEL_CONVERSION = {
    "layer": "gold",
    "table": "funnel_conversion",
    "columns": [
        ("activity_date", "DATE"),
        ("page_view", "BIGINT"),
        ("add_to_cart", "BIGINT"),
        ("checkout_start", "BIGINT"),
        ("checkout_complete", "BIGINT"),
        ("cart_to_checkout_rate", "DOUBLE"),
        ("checkout_to_purchase_rate", "DOUBLE"),
    ],
    "partition_by": ["months(activity_date)"],
}

GOLD_ROAS = {
    "layer": "gold",
    "table": "roas",
    "columns": [
        ("campaign_id", "BIGINT"),
        ("name", "STRING"),
        ("channel", "STRING"),
        ("start_date", "DATE"),
        ("end_date", "DATE"),
        ("spend", "DOUBLE"),
        ("attributed_revenue", "DOUBLE"),
        ("roas", "DOUBLE"),
    ],
    "partition_by": None,
}

GOLD_QUARANTINE_SUMMARY = {
    "layer": "gold",
    "table": "quarantine_summary",
    "columns": [
        ("batch_date", "DATE"),
        ("table_name", "STRING"),
        ("failure_reason", "STRING"),
        ("row_count", "BIGINT"),
    ],
    "partition_by": ["months(batch_date)"],
}

# ---------------------------------------------------------------------------
# QUARANTINE — same shape as the silver table it failed validation for, plus
# the reason. One per silver table that runs through run_quality_gate().
# ---------------------------------------------------------------------------

QUARANTINE_FACT_ORDER_ITEMS = {
    "layer": "quarantine",
    "table": "fact_order_items",
    "columns": GOLD_FACT_ORDER_ITEMS["columns"] + [("_quarantine_reason", "STRING")],
    "partition_by": None,
}

QUARANTINE_CAMPAIGNS = {
    "layer": "quarantine",
    "table": "campaigns",
    "columns": BRONZE_CAMPAIGNS["columns"] + [("_quarantine_reason", "STRING")],
    "partition_by": None,
}

QUARANTINE_REVIEWS = {
    "layer": "quarantine",
    "table": "reviews",
    "columns": SILVER_REVIEWS["columns"] + [("_quarantine_reason", "STRING")],
    "partition_by": None,
}

QUARANTINE_ORDERS = {
    "layer": "quarantine",
    "table": "orders",
    "columns": SILVER_ORDERS["columns"] + [("_quarantine_reason", "STRING")],
    "partition_by": None,
}

QUARANTINE_CUSTOMERS = {
    "layer": "quarantine",
    "table": "customers",
    "columns": [
        ("customer_id", "BIGINT"),
        ("full_name", "STRING"),
        ("email", "STRING"),
        ("segment", "STRING"),
        ("address", "STRING"),
        ("updated_at", "STRING"),
        ("captured_at", "TIMESTAMP"),
        ("_quarantine_reason", "STRING"),
    ],
    "partition_by": None,
}

QUARANTINE_PRODUCTS = {
    "layer": "quarantine",
    "table": "products",
    "columns": [
        ("product_id", "BIGINT"),
        ("sku", "STRING"),
        ("name", "STRING"),
        ("category", "STRING"),
        ("unit_price", "DECIMAL(10,2)"),
        ("created_at", "TIMESTAMP"),
        ("updated_at", "TIMESTAMP"),
        ("_quarantine_reason", "STRING"),
    ],
    "partition_by": None,
}

SILVER_CLICKSTREAM = {
    "layer": "silver",
    "table": "clickstream",
    "columns": BRONZE_CLICKSTREAM["columns"],
    "partition_by": BRONZE_CLICKSTREAM["partition_by"],
}

QUARANTINE_CLICKSTREAM = {
    "layer": "quarantine",
    "table": "clickstream",
    "columns": BRONZE_CLICKSTREAM["columns"] + [("_quarantine_reason", "STRING")],
    "partition_by": None,
}

ALL_TABLES = [
    BRONZE_ORDERS_CDC,
    BRONZE_CLICKSTREAM,
    BRONZE_CAMPAIGNS,
    BRONZE_REVIEWS,
    BRONZE_DEAD_LETTERS,
    SILVER_REVIEWS,
    SILVER_CLICKSTREAM,
    SILVER_CUSTOMERS,
    SILVER_ORDERS,
    GOLD_DIM_CUSTOMER,
    GOLD_FACT_ORDER_ITEMS,
    GOLD_DAILY_SALES,
    GOLD_TOP_PRODUCTS,
    GOLD_CUSTOMER_SEGMENTS,
    GOLD_CONVERSION_RATE,
    GOLD_CAMPAIGN_EFFECTIVENESS,
    GOLD_PRODUCT_SENTIMENT,
    GOLD_DIM_DATE,
    GOLD_DIM_PRODUCT,
    GOLD_DIM_CAMPAIGN,
    GOLD_CUSTOMER_CLV,
    GOLD_FUNNEL_CONVERSION,
    GOLD_ROAS,
    GOLD_QUARANTINE_SUMMARY,
    QUARANTINE_FACT_ORDER_ITEMS,
    QUARANTINE_CAMPAIGNS,
    QUARANTINE_REVIEWS,
    QUARANTINE_ORDERS,
    QUARANTINE_CUSTOMERS,
    QUARANTINE_PRODUCTS,
    QUARANTINE_CLICKSTREAM,
]


def create_all_tables_sql() -> list[str]:
    """Generates CREATE TABLE SQL statements for all defined schemas.

    One CREATE TABLE IF NOT EXISTS statement per table defined above.
    Designed to be run once at job startup (idempotent), so a fresh environment
    bootstraps itself.

    Returns:
        list[str]: A list of generated SQL statements.
    """
    from dataone.utils.iceberg_helpers import create_table_sql

    return [
        create_table_sql(t["layer"], t["table"], t["columns"], t["partition_by"])
        for t in ALL_TABLES
    ]
