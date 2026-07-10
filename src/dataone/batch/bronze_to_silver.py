"""
Nightly Spark batch job: bronze -> silver/gold. Ingests NiFi-staged campaign
files into bronze, joins orders/customers (from Kafka CDC via the lakehouse)
with order_items/products (direct JDBC from Postgres — see schemas.py for
why those two have no CDC path), applies window functions, runs the SCD2
customer-dimension merge, runs the data-quality gate, writes curated Iceberg
tables, and loads the business marts into ClickHouse.

Run (inside the spark-worker-batch container, started via `make batch`):
    spark-submit bronze_to_silver.py [--start DATE --end DATE]

TESTABILITY NOTE: this is real PySpark DataFrame logic (joins, window
functions, MERGE INTO via the SCD2 module) — written carefully against the
documented APIs, but this project's dev sandbox has no way to test it for
real (no Java, no Kafka, no Iceberg/ClickHouse cluster). First real run is
on your laptop.
"""
from __future__ import annotations

import argparse
import contextlib
import glob
import os
import shutil
import time
import typing
import urllib.error
import urllib.request
from base64 import b64encode

import psycopg2

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from dataone.batch.scd2_customer_dim import apply_scd2_merge
from dataone.config import clickhouse, postgres
from dataone.quality.validators import reconcile_row_counts, run_quality_gate, QualityResult
from dataone.utils.iceberg_helpers import bootstrap_namespaces_sql, table_identifier, make_surrogate_key
from dataone.utils.logging_config import get_logger
from dataone.utils.schemas import ALL_TABLES, create_all_tables_sql
from dataone.utils.spark_session import build_spark_session

log = get_logger(__name__)

STAGING_CAMPAIGNS_DIR = "/data/lakehouse/staging/campaigns"
ARCHIVED_CAMPAIGNS_DIR = "/data/lakehouse/staging/campaigns/_archived"

# Schema for the JSON payload inside bronze.orders_cdc.data_json when
# table_name = "customers" — mirrors infra/docker/postgres/init/001_schema.sql.
_CDC_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("full_name", StringType()),
        StructField("email", StringType()),
        StructField("segment", StringType()),
        StructField("address", StringType()),
        StructField("updated_at", StringType()),
    ]
)

# Same, for table_name = "orders".
_CDC_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_date", StringType()),
        StructField("status", StringType()),
        StructField("campaign_id", LongType()),
    ]
)

_CAMPAIGNS_CSV_SCHEMA = StructType(
    [
        StructField("campaign_id", LongType()),
        StructField("name", StringType()),
        StructField("channel", StringType()),
        StructField("start_date", DateType()),
        StructField("end_date", DateType()),
        StructField("budget", DoubleType()),
        StructField("spend", DoubleType()),
        StructField("clicks", LongType()),
        StructField("conversions", LongType()),
    ]
)


def parse_args() -> argparse.Namespace:
    """Parses command-line arguments.

    Returns:
        argparse.Namespace: The parsed arguments, containing optional
            start and end dates for backfilling.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="Backfill start date (YYYY-MM-DD), optional")
    parser.add_argument("--end", help="Backfill end date (YYYY-MM-DD), optional")
    return parser.parse_args()


def bootstrap_lakehouse(spark: SparkSession) -> None:
    """Idempotent: creates every namespace + table if missing, safe to call
    on every run so a fresh environment bootstraps itself."""
    for sql in bootstrap_namespaces_sql() + create_all_tables_sql():
        spark.sql(sql)
    for t in ALL_TABLES:
        ident = table_identifier(t["layer"], t["table"])
        spark.sql(f"ALTER TABLE {ident} SET TBLPROPERTIES ('write.spark.fanout.enabled'='false')")
    log.info("bootstrap_lakehouse.done", tables=len(ALL_TABLES))


def ingest_campaigns_to_bronze(spark: SparkSession) -> int:
    """
    Reads NiFi-staged campaign CSVs from STAGING_CAMPAIGNS_DIR, appends them
    to bronze.campaigns, then moves the source files into an archive
    subfolder so the next run doesn't reprocess them. Plain Python file
    move (not a Spark/Hadoop FS op) — the staging path is a regular mounted
    volume, not HDFS. Files are only archived AFTER a successful Iceberg
    write, so a failed write leaves them in place for retry.
    """
    csv_files = sorted(glob.glob(f"{STAGING_CAMPAIGNS_DIR}/*.csv"))
    if not csv_files:
        log.info("ingest_campaigns_to_bronze.no_files")
        return 0

    os.makedirs(ARCHIVED_CAMPAIGNS_DIR, exist_ok=True)
    # Append + archive one file at a time so a crash mid-loop can only ever
    # re-ingest the single file whose move didn't complete — not the whole
    # staging directory. Bronze duplicates from that one-file window are
    # collapsed downstream by the latest-per-campaign_id dedup in
    # read_bronze_tables(), so the pipeline stays correct either way.
    for path in csv_files:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(_CAMPAIGNS_CSV_SCHEMA)
            .load(path)
            .withColumn("ingested_at", F.current_timestamp())
        )
        df.writeTo(table_identifier("bronze", "campaigns")).append()
        shutil.move(path, os.path.join(ARCHIVED_CAMPAIGNS_DIR, os.path.basename(path)))
        log.info("ingest_campaigns_to_bronze.file_done", file=os.path.basename(path))

    log.info("ingest_campaigns_to_bronze.done", files=len(csv_files))
    return len(csv_files)


def ingest_reviews_to_bronze(spark: SparkSession) -> int:
    """
    Read from MongoDB reviews collection → bronze.reviews (append-only,
    incremental). The MongoDB document schema is intentionally variable
    (some docs have `images`, `verified_purchase`, etc.) — the select below
    normalizes this into the fixed bronze schema: absent fields become NULL,
    extra fields (like `images`) are silently dropped.

    Incremental watermark: only documents with submitted_at strictly newer
    than the max already landed in bronze are pulled, so re-running the
    batch doesn't duplicate the whole collection into bronze. (Late edits to
    an already-ingested review are out of scope for this source — reviews
    are immutable once submitted.)
    """
    from dataone.config import mongo

    watermark = None
    if spark.catalog.tableExists(table_identifier("bronze", "reviews")):
        watermark = (
            spark.read.format("iceberg")
            .load(table_identifier("bronze", "reviews"))
            .agg(F.max("submitted_at"))
            .collect()[0][0]
        )

    df = (
        spark.read.format("mongodb")
        .option("connection.uri", mongo.uri)
        .option("database", mongo.db)
        .option("collection", "reviews")
        .load()
        .select(
            F.col("review_id"),
            F.col("product_id").cast("long"),
            F.col("customer_id").cast("long"),
            F.col("rating").cast("int"),
            F.col("title"),
            F.col("body"),
            F.col("verified_purchase").cast("boolean"),
            F.to_timestamp("submitted_at").alias("submitted_at"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
    if watermark is not None:
        df = df.filter(F.col("submitted_at") > F.lit(watermark))

    # Cache so the count action below doesn't re-pull the collection from
    # MongoDB after the append already consumed it once.
    df = df.cache()
    try:
        count = df.count()
        if count:
            df.writeTo(table_identifier("bronze", "reviews")).append()
        log.info("ingest_reviews_to_bronze.done", rows=count, watermark=str(watermark))
        return count
    finally:
        df.unpersist()


def read_bronze_tables(spark: SparkSession, start: str | None, end: str | None) -> dict[str, DataFrame]:
    """The backfill hook: start/end (YYYY-MM-DD strings) filter the
    time-partitioned bronze reads. None on either side means unbounded."""
    cdc_df = spark.read.format("iceberg").load(table_identifier("bronze", "orders_cdc"))
    if start:
        cdc_df = cdc_df.filter(F.col("captured_at") >= F.to_timestamp(F.lit(start)))
    if end:
        cdc_df = cdc_df.filter(F.col("captured_at") <= F.to_timestamp(F.lit(end)))

    clickstream_df = spark.read.format("iceberg").load(table_identifier("bronze", "clickstream"))
    if start:
        clickstream_df = clickstream_df.filter(F.col("ts") >= F.to_timestamp(F.lit(start)))
    if end:
        clickstream_df = clickstream_df.filter(F.col("ts") <= F.to_timestamp(F.lit(end)))

    # Campaigns are reference-sized (tens of rows per generator run, see
    # campaign_generator.py) — not worth date-filtering for backfill. Bronze
    # may legitimately hold the same campaign_id more than once (regenerated
    # files, or the one-file crash window in ingest_campaigns_to_bronze), so
    # collapse to the latest ingested version per campaign_id here — the
    # same latest-per-key pattern used for the CDC feeds.
    campaigns_df = _latest_per_key(
        spark.read.format("iceberg").load(table_identifier("bronze", "campaigns")),
        key_col="campaign_id",
        order_col="ingested_at",
    )
    reviews_df = spark.read.format("iceberg").load(table_identifier("bronze", "reviews"))

    return {"cdc": cdc_df, "clickstream": clickstream_df, "campaigns": campaigns_df, "reviews": reviews_df}


def read_postgres_table(
    spark: SparkSession,
    table_name: str,
    partition_col: str | None = None,
    lower_bound: int | None = None,
    upper_bound: int | None = None,
    num_partitions: int = 8,
) -> DataFrame:
    """Direct JDBC extract — used for order_items/products, which have no
    CDC or streaming path (see schemas.py module docstring for why)."""
    reader = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{postgres.host}:{postgres.port}/{postgres.db}")
        .option("dbtable", table_name)
        .option("user", postgres.user)
        .option("password", postgres.password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
    )
    if partition_col and lower_bound is not None and upper_bound is not None:
        reader = (
            reader.option("partitionColumn", partition_col)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(num_partitions))
        )
    return reader.load()


def _postgres_max_id(table_name: str, id_column: str) -> int:
    """
    Current MAX(id) of a source table, fetched with a single cheap query so
    the parallel JDBC extract's upperBound tracks the real table size instead
    of a hardcoded guess (a too-small bound silently skews all overflow rows
    into the last partition).
    """
    allowed = {("order_items", "order_item_id"), ("products", "product_id")}
    if (table_name, id_column) not in allowed:
        raise ValueError(f"unexpected table/column pair: {table_name}.{id_column}")
    with contextlib.closing(psycopg2.connect(postgres.dsn)) as conn:
        with conn, conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX({id_column}), 1) FROM {table_name}")
            return int(cur.fetchone()[0])


def _latest_per_key(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    """Dedup to one row per key_col, keeping the row with the max order_col
    — the standard pattern for collapsing an append-only CDC event log down
    to "latest known state per entity"."""
    window = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def parse_customers_from_cdc(cdc_df: DataFrame) -> DataFrame:
    """Parses customer records from the CDC JSON payload.

    Args:
        cdc_df (DataFrame): The bronze CDC dataframe.

    Returns:
        DataFrame: A dataframe containing the parsed customer attributes,
            deduplicated to keep the latest state per customer.
    """
    return _latest_per_key(
        cdc_df.filter(F.col("table_name") == "customers")
        .withColumn("_parsed", F.from_json(F.col("data_json"), _CDC_CUSTOMERS_SCHEMA))
        .select("_parsed.*", "captured_at"),
        key_col="customer_id",
        order_col="captured_at",
    ).withColumn("updated_at", F.to_timestamp("updated_at"))


def parse_orders_from_cdc(cdc_df: DataFrame) -> DataFrame:
    """Parses order records from the CDC JSON payload.

    Args:
        cdc_df (DataFrame): The bronze CDC dataframe.

    Returns:
        DataFrame: A dataframe containing the parsed order attributes,
            deduplicated to keep the latest state per order.
    """
    return _latest_per_key(
        cdc_df.filter(F.col("table_name") == "orders")
        .withColumn("_parsed", F.from_json(F.col("data_json"), _CDC_ORDERS_SCHEMA))
        .select("_parsed.*", "captured_at"),
        key_col="order_id",
        order_col="captured_at",
    ).withColumn("order_date", F.to_timestamp("order_date"))


def merge_into_silver(spark: SparkSession, df: DataFrame, table_name: str, merge_key: str) -> None:
    """Merges a dataframe into a Silver Iceberg table using a UPSERT operation.

    Args:
        spark (SparkSession): The active Spark session.
        df (DataFrame): The incoming dataframe to merge.
        table_name (str): The target table name in the Silver layer.
        merge_key (str): The column name to use as the join key for the merge.
    """
    import uuid
    view_name = f"_incoming_silver_{table_name}_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view_name)
    target = table_identifier("silver", table_name)
    
    spark.sql(f"""
        MERGE INTO {target} t
        USING {view_name} s
        ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def build_fact_order_items(orders_df: DataFrame, order_items_df: DataFrame, products_df: DataFrame, customer_dim_full_df: DataFrame) -> DataFrame:
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
            on=(F.col("o.customer_id") == F.col("c.customer_id")) &
               (F.col("o.order_date") >= F.col("c.valid_from")) &
               (F.col("c.valid_to").isNull() | (F.col("o.order_date") < F.col("c.valid_to"))),
            how="inner"
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

@F.udf(returnType=DoubleType())
def _sentiment_udf(text: str | None) -> float | None:
    """Lightweight polarity score in [-1, 1]. The import lives inside the
    UDF so it resolves on the executor's Python worker (cached in
    sys.modules after the first row — not a per-row cost). A scoring failure
    on one pathological body yields NULL for that row instead of failing
    the whole stage."""
    if text is None:
        return None
    try:
        from textblob import TextBlob

        return float(TextBlob(text).sentiment.polarity)
    except Exception:
        return None


def build_silver_reviews(bronze_reviews_df: DataFrame) -> DataFrame:
    """
    Deduplicate by review_id (at-least-once bronze may have duplicates),
    apply lightweight sentiment scoring, and write to silver.reviews.
    """
    window = Window.partitionBy("review_id").orderBy(F.col("ingested_at").desc())
    return (
        bronze_reviews_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("sentiment_score", _sentiment_udf(F.col("body")))
    )

def build_product_sentiment(silver_reviews_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """Builds the product sentiment summary mart.

    Args:
        silver_reviews_df (DataFrame): The silver reviews dataframe.
        products_df (DataFrame): The products dataframe.

    Returns:
        DataFrame: A dataframe containing sentiment metrics aggregated by product.
    """
    agg = (
        silver_reviews_df.groupBy("product_id")
        .agg(
            F.count("*").alias("review_count"),
            F.avg("rating").alias("avg_rating"),
            F.avg("sentiment_score").alias("avg_sentiment"),
            (F.sum(F.col("verified_purchase").cast("int")) / F.count("*"))
            .alias("pct_verified"),
        )
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
        .withColumn("_day_ordinal", F.datediff(F.col("sales_date"), F.lit("1970-01-01").cast("date")))
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
    return (
        agg.withColumn("rank_in_category", F.rank().over(rank_window))
           .select("product_id", "product_name", "sku", "category",
                   "units_sold", "revenue", "rank_in_category")
    )


def build_customer_segments(fact_order_items_df: DataFrame, customer_dim_full_df: DataFrame) -> DataFrame:
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
    return campaigns_df.select("campaign_id", "name", "channel", "spend", "clicks", "conversions").withColumn(
        "cost_per_conversion",
        F.when(F.col("conversions") > 0, F.col("spend") / F.col("conversions")).otherwise(F.lit(None)),
    )


def build_dim_date(spark: SparkSession, start: str | None = None, end: str | None = None) -> DataFrame:
    """
    Calendar dimension. Range comes from DIM_DATE_START/DIM_DATE_END env
    vars (defaults below) — and main() rebuilds this table every run
    (createOrReplace, it's only ~1k rows), so widening the env range is
    picked up immediately instead of being frozen at first bootstrap.
    """
    start = start or os.getenv("DIM_DATE_START", "2025-01-01")
    end = end or os.getenv("DIM_DATE_END", "2027-12-31")
    date_range = spark.sql(f"SELECT sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY) AS dates")
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
    return products_df.select("product_id", "sku", F.col("name").alias("product_name"),
                               "category", F.col("unit_price").alias("current_price")).withColumn(
        "sk_product_id", make_surrogate_key("product_id")
    )


def build_dim_campaign(campaigns_df: DataFrame) -> DataFrame:
    """Builds the campaign dimension table.

    Args:
        campaigns_df (DataFrame): The raw campaigns dataframe.

    Returns:
        DataFrame: A dataframe containing the campaign dimension attributes and surrogate keys.
    """
    return campaigns_df.select("campaign_id", "name", "channel", "start_date", "end_date").withColumn(
        "sk_campaign_id", make_surrogate_key("campaign_id")
    )


def build_customer_clv(fact_order_items_df: DataFrame, customer_dim_current_df: DataFrame) -> DataFrame:
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
        .withColumn(
            "days_since_last_order",
            F.datediff(F.current_date(), F.col("last_order_date"))
        )
    )
    return clv.join(
        customer_dim_current_df.select("customer_id", "full_name", "segment"),
        on="customer_id",
        how="left"
    ).select(
        "customer_id", "full_name", "total_orders", "total_spend",
        "first_order_date", "last_order_date", "avg_order_value",
        "days_since_last_order", "segment"
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
        F.when(F.col("add_to_cart") > 0,
               F.col("checkout_start") / F.col("add_to_cart")).otherwise(F.lit(None))
    ).withColumn(
        "checkout_to_purchase_rate",
        F.when(F.col("checkout_start") > 0,
               F.col("checkout_complete") / F.col("checkout_start")).otherwise(F.lit(None))
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
    return campaigns_df.join(attributed_revenue, on="campaign_id", how="left").withColumn(
        "roas",
        F.when(F.col("spend") > 0,
               F.col("attributed_revenue") / F.col("spend")).otherwise(F.lit(None))
    ).select(
        "campaign_id", "name", "channel", "start_date", "end_date", "spend", "attributed_revenue", "roas"
    )


def write_overwrite_partitions(df: DataFrame, layer: str, table_name: str) -> None:
    """Full-recompute-per-run idempotency: replaces only the partitions
    present in df (Iceberg's dynamic partition overwrite), leaving
    historical partitions outside the current backfill window untouched.
    For unpartitioned tables this overwrites the whole (small) table, which
    is exactly what we want for a fully-recomputed global aggregate."""
    target = table_identifier(layer, table_name)
    df.writeTo(target).overwritePartitions()
    log.info("write_overwrite_partitions.done", table=target)


def write_append(df: DataFrame, layer: str, table_name: str) -> None:
    """Quarantine accumulates as a persistent audit trail across runs —
    never overwritten."""
    target = table_identifier(layer, table_name)
    df.writeTo(target).append()
    log.info("write_append.done", table=target)


_CLICKHOUSE_HTTP_TIMEOUT_SECONDS = 60
_CLICKHOUSE_RETRIES = 3


def _clickhouse_query(query: str) -> None:
    """
    Fire a DDL/maintenance statement at ClickHouse over HTTP. Bounded by a
    timeout (a hung socket must not hang the whole batch run) and retried a
    few times with backoff — transient ClickHouse restarts shouldn't fail
    the nightly job. On an HTTP error the response body (ClickHouse puts the
    real error text there) is surfaced in the exception.
    """
    url = f"http://{clickhouse.host}:{clickhouse.port}/"
    auth_str = f"{clickhouse.user}:{clickhouse.password}"
    auth_b64 = b64encode(auth_str.encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth_b64}"}
    req = urllib.request.Request(url, data=query.encode("utf-8"), headers=headers, method="POST")

    last_exc: Exception | None = None
    for attempt in range(1, _CLICKHOUSE_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=_CLICKHOUSE_HTTP_TIMEOUT_SECONDS) as response:
                response.read()
            return
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            last_exc = RuntimeError(f"ClickHouse HTTP {exc.code}: {body}")
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        log.warning("clickhouse.query.retry", attempt=attempt, error=str(last_exc))
        time.sleep(2**attempt)
    raise last_exc

def _truncate_clickhouse_tables(table_names: typing.Iterable[str]) -> None:
    for t in table_names:
        try:
            _clickhouse_query(f"TRUNCATE TABLE IF EXISTS {clickhouse.db}.{t}")
            log.info("clickhouse.truncate.done", table=t)
        except Exception as e:
            log.error("clickhouse.truncate.failed", table=t, error=str(e))
            raise


def load_clickhouse_marts(gold_tables: dict[str, DataFrame]) -> None:
    """
    Mirrors each freshly computed gold DataFrame into its ClickHouse table
    for Grafana — same full-recompute-each-run idempotency as the Iceberg
    gold tables. We manually TRUNCATE first, then append via JDBC so the
    ClickHouse-side MergeTree table definition/engine settings survive
    (created in infra/docker/clickhouse/init/002_marts.sql) without Spark
    falling back to DROP/CREATE.
    """
    jdbc_url = f"jdbc:clickhouse://{clickhouse.host}:{clickhouse.port}/{clickhouse.db}"
    
    for table_name, df in gold_tables.items():
        staging = f"{table_name}_staging"
        try:
            # Step 1: Truncate and fill the STAGING table (target untouched if this fails)
            _truncate_clickhouse_tables([staging])
            (
                df.write.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", f"{clickhouse.db}.{staging}")
                .option("user", clickhouse.user)
                .option("password", clickhouse.password)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .mode("append")
                .save()
            )
            # Step 2: Atomic swap — either both succeed or neither takes effect
            _clickhouse_query(
                f"EXCHANGE TABLES {clickhouse.db}.{table_name} AND {clickhouse.db}.{staging}"
            )
            log.info("load_clickhouse_marts.done", table=table_name)
        except Exception as exc:
            log.error("load_clickhouse_marts.failed", table=table_name, error=str(exc), exc_info=True)
            # Best-effort staging cleanup — target table is unaffected. Guarded
            # so a cleanup failure can't mask the original exception.
            try:
                _clickhouse_query(f"TRUNCATE TABLE IF EXISTS {clickhouse.db}.{staging}")
            except Exception:
                log.warning("load_clickhouse_marts.staging_cleanup_failed", table=staging)
            raise


def _start_pipeline_run(job_name: str, start: str | None, end: str | None) -> str:
    """Records the start of a pipeline run in the postgres metadata database.

    Args:
        job_name (str): The name of the pipeline job.
        start (str | None): The start date of the backfill window, if any.
        end (str | None): The end date of the backfill window, if any.

    Returns:
        str: The generated run ID.
    """
    # NOTE: psycopg2's `with conn:` commits/rolls back but does NOT close the
    # socket — hence closing() so each metadata write releases its connection.
    with contextlib.closing(psycopg2.connect(postgres.dsn)) as conn:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO _pipeline_runs (job_name, status, date_range_start, date_range_end)
                VALUES (%s, 'running', %s, %s)
                RETURNING run_id
                """,
                (job_name, start, end),
            )
            return str(cur.fetchone()[0])


def _complete_pipeline_run(
    run_id: str,
    status: str,
    rows_processed: int | None = None,
    rows_quarantined: int | None = None,
    error_message: str | None = None,
) -> None:
    """Records the completion (success or failure) of a pipeline run.

    Args:
        run_id (str): The ID of the pipeline run.
        status (str): The final status of the run (e.g., 'success', 'failed').
        rows_processed (int | None, optional): The number of successfully processed rows. Defaults to None.
        rows_quarantined (int | None, optional): The number of quarantined rows. Defaults to None.
        error_message (str | None, optional): The error message if the run failed. Defaults to None.
    """
    with contextlib.closing(psycopg2.connect(postgres.dsn)) as conn:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE _pipeline_runs
                SET end_time = now(),
                    status = %s,
                    rows_processed = %s,
                    rows_quarantined = %s,
                    error_message = %s
                WHERE run_id = %s
                """,
                (status, rows_processed, rows_quarantined, error_message, run_id),
            )

def build_quality_gate_summary(
    spark: SparkSession,
    batch_date: "date",
    results: dict[str, QualityResult],
) -> DataFrame:
    """One row per dataset for this run: (batch_date, table_name, passed_count,
    quarantined_count). Built from counts run_quality_gate() already computed —
    no new Spark actions, no re-scanning any table."""
    rows = [
        (batch_date, table_name, result.passed_count, result.quarantined_count)
        for table_name, result in results.items()
    ]
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
            .groupBy("batch_date", "table_name", F.col("_quarantine_reason").alias("failure_reason"))
            .agg(F.count("*").alias("row_count"))
        )
    
    orders_fact_agg = _agg("fact_order_items", "order_date")
    orders_raw_agg = _agg("orders", "captured_at")
    campaigns_agg = _agg("campaigns", "start_date")
    reviews_agg = _agg("reviews", "submitted_at")
    customers_agg = _agg("customers", "captured_at")
    products_agg = _agg("products", "created_at")
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

def main() -> None:
    """Main entrypoint for the bronze-to-silver batch pipeline.

    Coordinates the ingestion of campaigns and reviews to bronze, applies
    data quality gates, merges data into the silver layer, and aggregates
    business metrics into the gold layer before loading them into ClickHouse.
    """
    args = parse_args()
    log.info("bronze_to_silver.start", start=args.start, end=args.end)
    run_id = _start_pipeline_run("bronze_to_silver", args.start, args.end)
    spark: SparkSession | None = None
    try:
        spark = build_spark_session("dataone-batch")
        bootstrap_lakehouse(spark)
        ingest_campaigns_to_bronze(spark)
        ingest_reviews_to_bronze(spark)

        bronze = read_bronze_tables(spark, args.start, args.end)

        campaigns_quality = run_quality_gate(
            bronze["campaigns"],
            required_columns=["campaign_id", "name", "start_date", "end_date"],
            column_bounds={"budget": (0, None), "spend": (0, None), "clicks": (0, None), "conversions": (0, None)},
        )
        bronze["campaigns"] = campaigns_quality.passed_df
        write_append(campaigns_quality.quarantined_df, "quarantine", "campaigns")

        customers_df = parse_customers_from_cdc(bronze["cdc"])
        orders_df = parse_orders_from_cdc(bronze["cdc"])

        customers_quality = run_quality_gate(
            customers_df,
            required_columns=["customer_id"]
        )
        customers_passed = customers_quality.passed_df.cache()
        write_append(customers_quality.quarantined_df, "quarantine", "customers")

        orders_quality = run_quality_gate(
            orders_df,
            required_columns=["order_id", "customer_id", "order_date"]
        )
        orders_passed = orders_quality.passed_df.cache()
        write_append(orders_quality.quarantined_df, "quarantine", "orders")

        # Materialize the full Silver layer using Iceberg MERGE INTO (upsert).
        merge_into_silver(spark, customers_passed, "customers", "customer_id")
        merge_into_silver(spark, orders_passed, "orders", "order_id")

        # Re-read from Silver to feed the Gold layer, verifying persistence 
        # and serving as a clean, strongly-typed source.
        customers_silver_df = spark.read.format("iceberg").load(table_identifier("silver", "customers"))
        orders_silver_df = spark.read.format("iceberg").load(table_identifier("silver", "orders"))

        apply_scd2_merge(spark, customers_silver_df)
        customer_dim_full_df = spark.read.format("iceberg").load(
            table_identifier("gold", "dim_customer")
        )
        customer_dim_current_df = customer_dim_full_df.filter(F.col("is_current"))

        # upperBound tracks the real table size (cheap MAX() probe) instead
        # of a hardcoded ceiling that silently skews the JDBC partition split
        # once the table outgrows it.
        order_items_df = read_postgres_table(
            spark,
            "order_items",
            partition_col="order_item_id",
            lower_bound=1,
            upper_bound=_postgres_max_id("order_items", "order_item_id"),
            num_partitions=8,
        )
        products_df = read_postgres_table(spark, "products")
        products_quality = run_quality_gate(
            products_df,
            required_columns=["product_id", "category", "name"]
        )
        products_df = products_quality.passed_df
        write_append(products_quality.quarantined_df, "quarantine", "products")
        
        # Build Gold using the persisted Silver DataFrames
        fact_order_items_df = build_fact_order_items(orders_silver_df, order_items_df, products_df, customer_dim_full_df).withColumn(
            "sk_order_id", F.md5(F.concat_ws("||", F.lit("postgres"), F.col("order_id").cast("string"), F.col("product_id").cast("string")))
        ).cache()

        quality_result = run_quality_gate(
            fact_order_items_df,
            required_columns=["order_id", "customer_id", "product_id"],
            column_bounds={"unit_price": (0, None), "quantity": (1, None)},
        )
        # No-data-drop invariant: every input row must land on exactly one
        # side of the gate. fact_order_items_df is cached, so this third count is
        # cheap; a mismatch is logged loudly by reconcile_row_counts.
        reconcile_row_counts(
            source_count=fact_order_items_df.count(),
            landed_count=quality_result.passed_count + quality_result.quarantined_count,
        )
        write_overwrite_partitions(
            quality_result.passed_df.sort("order_date", "customer_id"),
            "gold", 
            "fact_order_items"
        )
        write_append(quality_result.quarantined_df, "quarantine", "fact_order_items")

        silver_reviews_df = build_silver_reviews(bronze["reviews"])
        reviews_quality = run_quality_gate(
            silver_reviews_df,
            required_columns=["review_id", "product_id", "rating"],
            column_bounds={"rating": (1, 5)},
        )
        silver_reviews_passed = reviews_quality.passed_df.cache()
        write_overwrite_partitions(silver_reviews_passed, "silver", "reviews")
        write_append(reviews_quality.quarantined_df, "quarantine", "reviews")

        clickstream_quality = run_quality_gate(
            bronze["clickstream"],
            required_columns=["session_id", "event_type", "ts"],
        )
        # Apply custom event_type filtering for quarantine
        EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "checkout_start", "checkout_complete"]
        is_valid_event = F.col("event_type").isin(EVENT_TYPES)
        
        silver_clickstream_passed = clickstream_quality.passed_df.filter(is_valid_event).cache()
        
        invalid_event_quarantine = clickstream_quality.passed_df.filter(~is_valid_event).withColumn(
            "_quarantine_reason", F.lit("invalid_event_type")
        )
        
        final_clickstream_quarantine = clickstream_quality.quarantined_df.unionByName(invalid_event_quarantine)
        
        write_overwrite_partitions(silver_clickstream_passed, "silver", "clickstream")
        write_append(final_clickstream_quarantine, "quarantine", "clickstream")

        from datetime import date
        quality_results_by_table = {
            "campaigns": campaigns_quality,
            "customers": customers_quality,
            "orders": orders_quality,
            "products": products_quality,
            "fact_order_items": quality_result,
            "reviews": reviews_quality,
            "clickstream": clickstream_quality,
        }

        # Rebuilt (not create-if-missing) every run: it's ~1k rows, and this
        # way a widened DIM_DATE_START/END env range takes effect immediately
        # instead of being frozen at first bootstrap.
        build_dim_date(spark).writeTo(table_identifier("gold", "dim_date")).createOrReplace()

        gold = {
            "daily_sales": build_daily_sales(quality_result.passed_df),
            "top_products": build_top_products(quality_result.passed_df, products_df),
            # Full dimension (all SCD2 versions) — build_customer_segments
            # does a point-in-time interval join, not a current-state join.
            "customer_segments": build_customer_segments(quality_result.passed_df, customer_dim_full_df),
            "conversion_rate": build_conversion_rate(silver_clickstream_passed),
            "campaign_effectiveness": build_campaign_effectiveness(bronze["campaigns"]),
            "product_sentiment": build_product_sentiment(silver_reviews_passed, products_df),
            "dim_product": build_dim_product(products_df),
            "dim_campaign": build_dim_campaign(bronze["campaigns"]),
            "customer_clv": build_customer_clv(quality_result.passed_df, customer_dim_current_df),
            "funnel_conversion": build_funnel_conversion(silver_clickstream_passed),
            "roas": build_roas(quality_result.passed_df, bronze["campaigns"]),
            "quarantine_summary": build_quarantine_summary(spark),
            "quality_gate_summary": build_quality_gate_summary(
                spark, date.today(), quality_results_by_table
            ),
        }
        for name, df in gold.items():
            write_overwrite_partitions(df, "gold", name)

        # Reload each gold mart from Iceberg (not the in-memory df computed
        # above) before pushing to ClickHouse. write_overwrite_partitions only
        # replaces the partitions touched by THIS run — correct and
        # backfill-safe on the Iceberg side — but the ClickHouse JDBC load does
        # a full TRUNCATE + reload (no partition concept over plain JDBC). If we
        # pushed `df` directly, a narrow --start/--end backfill would silently
        # wipe ClickHouse's history for every month outside the backfilled
        # window. Reloading from Iceberg guarantees ClickHouse always mirrors
        # the complete, current table regardless of how narrow this run's scope
        # was.
        gold_from_iceberg = {
            name: spark.read.format("iceberg").load(table_identifier("gold", name)) for name in gold
        }
        gold_from_iceberg["fact_order_items"] = spark.read.format("iceberg").load(table_identifier("gold", "fact_order_items"))
        gold_from_iceberg["dim_date"] = spark.read.format("iceberg").load(table_identifier("gold", "dim_date"))
        load_clickhouse_marts(gold_from_iceberg)
        fact_order_items_df.unpersist()
        silver_reviews_passed.unpersist()
        log.info("bronze_to_silver.done")
        _complete_pipeline_run(
            run_id,
            "success",
            rows_processed=quality_result.passed_count,
            rows_quarantined=quality_result.quarantined_count,
        )
    except Exception as exc:
        # exc_info keeps the traceback in the structured log — str(exc)
        # alone made production failures near-undiagnosable.
        log.error("bronze_to_silver.failed", error=str(exc), exc_info=True)
        _complete_pipeline_run(run_id, "failed", error_message=str(exc))
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
