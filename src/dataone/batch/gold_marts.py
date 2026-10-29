"""
Gold-layer mart builders: pure DataFrame -> DataFrame transformations (plus the
two summary builders that read quarantine/silver tables), split out of
bronze_to_silver.py. bronze_to_silver re-exports every name here so existing
imports (`from dataone.batch.bronze_to_silver import build_daily_sales`) keep working.
"""

from __future__ import annotations

import os
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dataone.quality.validators import QualityResult
from dataone.utils.iceberg_helpers import make_surrogate_key, table_identifier
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)


def build_fact_order_items(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    customer_dim_full_df: DataFrame,
) -> DataFrame:
    """
    One row per order line item — joining orders (Kafka CDC via the
    lakehouse) with order_items and products (direct JDBC) is the
    "joins/merge from multiple data sources" requirement in practice, not
    just in name.

    Enriches the fact row with surrogate keys for every conformed dimension
    (date_key, sk_product_id, sk_campaign_id) so the Gold Star Schema
    joins exclusively on deterministic hash keys.
    """
    return (
        order_items_df.alias("oi")
        .join(orders_df.alias("o"), on="order_id", how="inner")
        .join(F.broadcast(products_df).alias("p"), on="product_id", how="inner")
        .join(
            F.broadcast(customer_dim_full_df).alias("c"),
            on=(F.col("o.customer_id") == F.col("c.customer_id"))
            & (F.col("o.order_date") >= F.col("c.valid_from"))
            & (F.col("c.valid_to").isNull() | (F.col("o.order_date") < F.col("c.valid_to"))),
            how="inner",
        )
        .select(
            F.col("o.order_id"),
            F.col("c.sk_customer_id"),
            F.col("o.customer_id"),
            F.col("o.order_date"),
            F.col("o.status"),
            F.col("oi.product_id"),
            F.col("oi.quantity").cast("long"),
            F.col("oi.unit_price").cast("double"),
            (F.col("oi.quantity") * F.col("oi.unit_price")).cast("double").alias("line_total"),
            F.col("o.campaign_id"),
        )
        .withColumn("date_key", F.date_format(F.to_date("order_date"), "yyyyMMdd").cast("int"))
        .withColumn("sk_product_id", make_surrogate_key("product_id"))
        .withColumn(
            "sk_campaign_id",
            F.when(F.col("campaign_id").isNotNull(), make_surrogate_key("campaign_id")),
        )
    )


def build_product_sentiment(silver_reviews_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """Builds the product sentiment summary mart.

    Args:
        silver_reviews_df (DataFrame): The silver reviews dataframe.
        products_df (DataFrame): The products dataframe.

    Returns:
        DataFrame: A dataframe containing sentiment metrics aggregated by product.
    """
    agg = silver_reviews_df.groupBy("product_id").agg(
        F.count("*").alias("review_count"),
        F.avg("rating").alias("avg_rating"),
        F.avg("sentiment_score").alias("avg_sentiment"),
        (F.sum(F.col("verified_purchase").cast("int")) / F.count("*")).alias("pct_verified"),
    )
    return agg.join(
        products_df.select("product_id", F.col("name").alias("product_name"), "category"),
        on="product_id",
        how="left",
    )


def build_daily_sales(fact_order_items_df: DataFrame) -> DataFrame:
    """
    True calendar-day rolling 7/30-day revenue averages — window functions
    over an explicit day-ordinal column (via datediff), not row count, so
    days with zero orders don't silently shift the window.
    """
    daily = (
        fact_order_items_df.groupBy(F.to_date("order_date").alias("sales_date"))
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("line_total").alias("total_revenue"),
        )
        .withColumn(
            "_day_ordinal", F.datediff(F.col("sales_date"), F.lit("1970-01-01").cast("date"))
        )
    )
    window_7d = Window.orderBy("_day_ordinal").rangeBetween(-6, 0)
    window_30d = Window.orderBy("_day_ordinal").rangeBetween(-29, 0)
    return (
        daily.withColumn("revenue_7d_avg", F.avg("total_revenue").over(window_7d))
        .withColumn("revenue_30d_avg", F.avg("total_revenue").over(window_30d))
        .select("sales_date", "order_count", "total_revenue", "revenue_7d_avg", "revenue_30d_avg")
    )


def build_top_products(fact_order_items_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """RANK() per category by revenue — the other named window-function
    requirement (pivoting/ranking)."""
    joined = fact_order_items_df.join(F.broadcast(products_df), on="product_id", how="inner")
    agg = joined.groupBy("product_id", "category", F.col("name").alias("product_name"), "sku").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_total").alias("revenue"),
    )
    rank_window = Window.partitionBy("category").orderBy(F.col("revenue").desc())
    return agg.withColumn("rank_in_category", F.rank().over(rank_window)).select(
        "product_id", "product_name", "sku", "category", "units_sold", "revenue", "rank_in_category"
    )


def build_customer_segments(
    fact_order_items_df: DataFrame, customer_dim_full_df: DataFrame
) -> DataFrame:
    """
    Point-in-time join against the FULL SCD2 dimension (all versions, not
    just is_current): each order matches the customer version whose
    [valid_from, valid_to) interval contains the order_date, so revenue is
    attributed to the segment the customer actually held when they ordered —
    correct historical analysis for customers who changed tiers. Orders
    predating a customer's first captured version have no matching interval
    and are excluded — same behavior as an inner join on an unknown customer.
    """
    joined = fact_order_items_df.alias("f").join(
        customer_dim_full_df.alias("c"),
        on=(
            (F.col("f.customer_id") == F.col("c.customer_id"))
            & (F.col("f.order_date") >= F.col("c.valid_from"))
            & (F.col("c.valid_to").isNull() | (F.col("f.order_date") < F.col("c.valid_to")))
        ),
        how="inner",
    )
    return (
        joined.groupBy(F.col("c.segment").alias("segment"))
        .agg(
            F.countDistinct("f.customer_id").alias("customer_count"),
            F.countDistinct("f.order_id").alias("_order_count"),
            F.sum("f.line_total").alias("total_revenue"),
        )
        .withColumn("avg_order_value", F.col("total_revenue") / F.col("_order_count"))
        .select("segment", "customer_count", "total_revenue", "avg_order_value")
    )


def build_conversion_rate(clickstream_df: DataFrame) -> DataFrame:
    """Builds the daily conversion rate mart based on clickstream sessions.

    Args:
        clickstream_df (DataFrame): The silver clickstream dataframe.

    Returns:
        DataFrame: A dataframe containing daily session and checkout completion metrics.
    """
    daily = clickstream_df.withColumn("activity_date", F.to_date("ts"))
    sessions = daily.groupBy("activity_date").agg(F.countDistinct("session_id").alias("sessions"))
    completions = (
        daily.filter(F.col("event_type") == "checkout_complete")
        .groupBy("activity_date")
        .agg(F.countDistinct("session_id").alias("checkout_completions"))
    )
    return (
        sessions.join(completions, on="activity_date", how="left")
        .fillna(0, subset=["checkout_completions"])
        .withColumn("conversion_rate", F.col("checkout_completions") / F.col("sessions"))
    )


def build_campaign_effectiveness(campaigns_df: DataFrame) -> DataFrame:
    """Builds the campaign effectiveness mart.

    Args:
        campaigns_df (DataFrame): The campaigns dataframe.

    Returns:
        DataFrame: A dataframe containing campaign performance metrics, including cost per conversion.
    """
    return campaigns_df.select(
        "campaign_id", "name", "channel", "spend", "clicks", "conversions"
    ).withColumn(
        "cost_per_conversion",
        F.when(F.col("conversions") > 0, F.col("spend") / F.col("conversions")).otherwise(
            F.lit(None)
        ),
    )


def build_dim_date(
    spark: SparkSession, start: str | None = None, end: str | None = None
) -> DataFrame:
    """
    Calendar dimension. Range comes from DIM_DATE_START/DIM_DATE_END env
    vars (defaults below) — and main() rebuilds this table every run
    (createOrReplace, it's only ~1k rows), so widening the env range is
    picked up immediately instead of being frozen at first bootstrap.
    """
    start = start or os.getenv("DIM_DATE_START", "2025-01-01")
    end = end or os.getenv("DIM_DATE_END", "2027-12-31")
    date_range = spark.sql(
        f"SELECT sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY) AS dates"
    )
    return (
        date_range.select(F.explode("dates").alias("_date"))
        .withColumn("date_key", F.date_format("_date", "yyyyMMdd").cast("int"))
        .withColumn("calendar_date", F.col("_date").cast("date"))
        .withColumn("day_of_week", F.dayofweek("_date").cast("int"))
        .withColumn("day_name", F.date_format("_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("_date").cast("int"))
        .withColumn("month", F.month("_date").cast("int"))
        .withColumn("month_name", F.date_format("_date", "MMMM"))
        .withColumn("quarter", F.quarter("_date").cast("int"))
        .withColumn("year", F.year("_date").cast("int"))
        .withColumn("is_weekend", F.dayofweek("_date").isin([1, 7]).cast("boolean"))
        .drop("_date")
    )


def build_dim_product(products_df: DataFrame) -> DataFrame:
    """Builds the product dimension table.

    Args:
        products_df (DataFrame): The raw products dataframe.

    Returns:
        DataFrame: A dataframe containing the product dimension attributes and surrogate keys.
    """
    return products_df.select(
        "product_id",
        "sku",
        F.col("name").alias("product_name"),
        "category",
        F.col("unit_price").alias("current_price"),
    ).withColumn("sk_product_id", make_surrogate_key("product_id"))


def build_dim_campaign(campaigns_df: DataFrame) -> DataFrame:
    """Builds the campaign dimension table.

    Args:
        campaigns_df (DataFrame): The raw campaigns dataframe.

    Returns:
        DataFrame: A dataframe containing the campaign dimension attributes and surrogate keys.
    """
    return campaigns_df.select(
        "campaign_id", "name", "channel", "start_date", "end_date"
    ).withColumn("sk_campaign_id", make_surrogate_key("campaign_id"))


def build_customer_clv(
    fact_order_items_df: DataFrame, customer_dim_current_df: DataFrame
) -> DataFrame:
    """Builds the Customer Lifetime Value (CLV) mart.

    Args:
        fact_order_items_df (DataFrame): The fact order items dataframe.
        customer_dim_current_df (DataFrame): The current customer dimension dataframe.

    Returns:
        DataFrame: A dataframe containing CLV metrics per customer.
    """
    clv = (
        fact_order_items_df.groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("line_total").alias("total_spend"),
            F.min(F.to_date("order_date")).alias("first_order_date"),
            F.max(F.to_date("order_date")).alias("last_order_date"),
        )
        .withColumn("avg_order_value", F.col("total_spend") / F.col("total_orders"))
        .withColumn("days_since_last_order", F.datediff(F.current_date(), F.col("last_order_date")))
    )
    return clv.join(
        customer_dim_current_df.select("customer_id", "full_name", "segment"),
        on="customer_id",
        how="left",
    ).select(
        "customer_id",
        "full_name",
        "total_orders",
        "total_spend",
        "first_order_date",
        "last_order_date",
        "avg_order_value",
        "days_since_last_order",
        "segment",
    )


def build_funnel_conversion(clickstream_df: DataFrame) -> DataFrame:
    """Builds the funnel conversion mart.

    Args:
        clickstream_df (DataFrame): The silver clickstream dataframe.

    Returns:
        DataFrame: A dataframe containing daily funnel conversion rates.
    """
    EVENT_TYPES = ["page_view", "add_to_cart", "checkout_start", "checkout_complete"]
    daily = clickstream_df.withColumn("activity_date", F.to_date("ts"))
    pivot = (
        daily.groupBy("activity_date")
        .pivot("event_type", EVENT_TYPES)
        .agg(F.countDistinct("session_id"))
        .fillna(0)
    )
    return pivot.withColumn(
        "cart_to_checkout_rate",
        F.when(F.col("add_to_cart") > 0, F.col("checkout_start") / F.col("add_to_cart")).otherwise(
            F.lit(None)
        ),
    ).withColumn(
        "checkout_to_purchase_rate",
        F.when(
            F.col("checkout_start") > 0, F.col("checkout_complete") / F.col("checkout_start")
        ).otherwise(F.lit(None)),
    )


def build_roas(fact_order_items_df: DataFrame, campaigns_df: DataFrame) -> DataFrame:
    """Builds the Return on Ad Spend (ROAS) mart.

    Args:
        fact_order_items_df (DataFrame): The fact order items dataframe.
        campaigns_df (DataFrame): The campaigns dataframe.

    Returns:
        DataFrame: A dataframe containing ROAS metrics per campaign.
    """
    attributed_revenue = (
        fact_order_items_df.filter(F.col("campaign_id").isNotNull())
        .groupBy("campaign_id")
        .agg(F.sum("line_total").alias("attributed_revenue"))
    )
    return (
        campaigns_df.join(attributed_revenue, on="campaign_id", how="left")
        .withColumn(
            "roas",
            F.when(F.col("spend") > 0, F.col("attributed_revenue") / F.col("spend")).otherwise(
                F.lit(None)
            ),
        )
        .select(
            "campaign_id",
            "name",
            "channel",
            "start_date",
            "end_date",
            "spend",
            "attributed_revenue",
            "roas",
        )
    )


def build_quality_gate_summary(
    spark: SparkSession,
    batch_date: date,
    results: dict[str, QualityResult | None],
) -> DataFrame:
    """One row per dataset for this run: (batch_date, table_name, passed_count,
    quarantined_count). Built from counts run_quality_gate() already computed —
    falls back to querying Iceberg tables directly when running in modular stage mode."""
    rows = []
    for table_name, result in results.items():
        if result is not None:
            rows.append((batch_date, table_name, result.passed_count, result.quarantined_count))
        else:
            try:
                if table_name == "campaigns":
                    passed_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("silver", "campaigns"))
                        .count()
                    )
                    quar_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("quarantine", "campaigns"))
                        .count()
                    )
                elif table_name == "customers":
                    passed_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("silver", "customers"))
                        .count()
                    )
                    quar_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("quarantine", "customers"))
                        .count()
                    )
                elif table_name == "orders":
                    passed_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("silver", "orders"))
                        .count()
                    )
                    quar_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("quarantine", "orders"))
                        .count()
                    )
                elif table_name == "reviews":
                    passed_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("silver", "reviews"))
                        .count()
                    )
                    quar_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("quarantine", "reviews"))
                        .count()
                    )
                elif table_name == "clickstream":
                    passed_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("silver", "clickstream"))
                        .count()
                    )
                    quar_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("quarantine", "clickstream"))
                        .count()
                    )
                elif table_name == "products":
                    passed_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("gold", "dim_product"))
                        .count()
                    )
                    quar_cnt = (
                        spark.read.format("iceberg")
                        .load(table_identifier("quarantine", "products"))
                        .count()
                    )
                else:
                    passed_cnt, quar_cnt = 0, 0
                rows.append((batch_date, table_name, passed_cnt, quar_cnt))
            except Exception as e:
                log.warning(
                    "failed_to_query_quality_counts_for_summary", table=table_name, error=str(e)
                )
                rows.append((batch_date, table_name, 0, 0))
    return spark.createDataFrame(
        rows, schema="batch_date date, table_name string, passed_count long, quarantined_count long"
    )


def build_quarantine_summary(spark: SparkSession) -> DataFrame:
    """Builds a daily summary of quarantined rows across all pipelines.

    Args:
        spark (SparkSession): The active Spark session.

    Returns:
        DataFrame: A dataframe containing daily quarantine counts aggregated by reason.
    """

    def _agg(table_name: str, date_col: str) -> DataFrame:
        q_df = spark.read.format("iceberg").load(table_identifier("quarantine", table_name))
        return (
            q_df.withColumn("batch_date", F.to_date(F.col(date_col)))
            .withColumn("table_name", F.lit(table_name))
            .groupBy(
                "batch_date", "table_name", F.col("_quarantine_reason").alias("failure_reason")
            )
            .agg(F.count("*").alias("row_count"))
        )

    orders_fact_agg = _agg("fact_order_items", "order_date")
    orders_raw_agg = _agg("orders", "captured_at")
    campaigns_agg = _agg("campaigns", "start_date")
    reviews_agg = _agg("reviews", "submitted_at")
    customers_agg = _agg("customers", "captured_at")
    products_agg = _agg("products", "updated_at")
    clickstream_agg = _agg("clickstream", "ts")

    # We aggregate again in case multiple tables have the exact same failure reason on the same date
    return (
        orders_fact_agg.unionByName(orders_raw_agg)
        .unionByName(campaigns_agg)
        .unionByName(reviews_agg)
        .unionByName(customers_agg)
        .unionByName(products_agg)
        .unionByName(clickstream_agg)
        .groupBy("batch_date", "table_name", "failure_reason")
        .agg(F.sum("row_count").alias("row_count"))
    )
