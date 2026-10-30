"""
Nightly Spark batch job: bronze -> silver/gold. Ingests NiFi-staged campaign
files, MongoDB reviews, and full/incremental snapshots of the Postgres products
and order_items tables into Bronze Iceberg; promotes all Bronze tables through
Silver quality gates; applies window functions, runs the SCD2 customer-dimension
merge, runs the data-quality gate, writes curated Iceberg tables, and loads the
business marts into ClickHouse.

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
import typing
from datetime import date

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from dataone.batch.clickhouse_sync import (  # noqa: F401  (re-exported)
    _clickhouse_query,
    _truncate_clickhouse_tables,
    load_clickhouse_marts,
)
from dataone.batch.gold_marts import (  # noqa: F401  (re-exported)
    build_campaign_effectiveness,
    build_conversion_rate,
    build_customer_clv,
    build_customer_segments,
    build_daily_sales,
    build_dim_campaign,
    build_dim_date,
    build_dim_product,
    build_fact_order_items,
    build_funnel_conversion,
    build_product_sentiment,
    build_quality_gate_summary,
    build_quarantine_summary,
    build_roas,
    build_top_products,
)
from dataone.batch.scd2_customer_dim import apply_scd2_merge
from dataone.config import postgres
from dataone.quality.validators import QualityResult, reconcile_row_counts, run_quality_gate
from dataone.utils.iceberg_helpers import (
    bootstrap_namespaces_sql,
    table_identifier,
)
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
            start and end dates for backfilling, and stage filters.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="Backfill start date (YYYY-MM-DD), optional")
    parser.add_argument("--end", help="Backfill end date (YYYY-MM-DD), optional")
    parser.add_argument(
        "--stage",
        choices=["ingest_bronze", "standardize_silver", "model_gold", "sync_clickhouse"],
        help="Specify the pipeline stage to run (optional, runs all by default)",
    )
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


def ingest_products_to_bronze(spark: SparkSession) -> int:
    """Full daily snapshot of the products dimension table into bronze.products.

    Products is a small, mutable reference table (updated prices, names, categories).
    A full overwrite snapshot is correct: it keeps Bronze storage bounded and ensures
    the Silver SCD1 merge sees the full current state, not just deltas.

    Write mode is createOrReplace (Iceberg atomic overwrite), which is safe because:
    - The table is small (thousands of rows, not millions).
    - Downstream Silver reads use a merge — not dependent on Bronze history.
    - A failed overwrite leaves the previous snapshot intact (Iceberg atomic commit).
    """
    df = read_postgres_table(spark, "products").withColumn("ingested_at", F.current_timestamp())
    df = df.cache()
    try:
        count = df.count()
        if count:
            df.writeTo(table_identifier("bronze", "products")).createOrReplace()
        log.info("ingest_products_to_bronze.done", rows=count)
        return count
    finally:
        df.unpersist()


def ingest_order_items_to_bronze(spark: SparkSession) -> int:
    """Watermark-incremental JDBC extract of order_items into bronze.order_items.

    order_items is append-only by business logic (a placed order line item is never
    modified or deleted). We track MAX(order_item_id) already in Bronze as a watermark
    and pull only strictly new rows — identical to how ingest_reviews_to_bronze
    watermarks on submitted_at.

    The partitioned JDBC read uses order_item_id as the partition column with
    lower_bound=watermark+1 and upper_bound=postgres_max_id to spread the load
    across Spark executors without skewing all overflow rows into one partition.

    IMPORTANT: The explicit filter(order_item_id > watermark) after the JDBC read is
    intentional — Spark JDBC lowerBound/upperBound are partition-splitting hints, not
    WHERE clause filters. Without this guard, rows <= watermark can appear in the result
    set and cause duplicate Bronze rows on re-runs.
    """
    watermark = 0
    if spark.catalog.tableExists(table_identifier("bronze", "order_items")):
        result = (
            spark.read.format("iceberg")
            .load(table_identifier("bronze", "order_items"))
            .agg(F.max("order_item_id"))
            .collect()[0][0]
        )
        if result is not None:
            watermark = int(result)

    upper = _postgres_max_id("order_items", "order_item_id")
    if upper <= watermark:
        log.info("ingest_order_items_to_bronze.no_new_rows", watermark=watermark)
        return 0

    df = (
        read_postgres_table(
            spark,
            "order_items",
            partition_col="order_item_id",
            lower_bound=watermark + 1,
            upper_bound=upper,
            num_partitions=8,
        )
        .filter(F.col("order_item_id") > watermark)  # guard: JDBC bounds are hints, not filters
        .withColumn("ingested_at", F.current_timestamp())
    )
    df = df.cache()
    try:
        count = df.count()
        if count:
            df.writeTo(table_identifier("bronze", "order_items")).append()
        log.info("ingest_order_items_to_bronze.done", rows=count, watermark=watermark, upper=upper)
        return count
    finally:
        df.unpersist()


def read_bronze_tables(
    spark: SparkSession, start: str | None, end: str | None
) -> dict[str, DataFrame]:
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

    return {
        "cdc": cdc_df,
        "clickstream": clickstream_df,
        "campaigns": campaigns_df,
        "reviews": reviews_df,
    }


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
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


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

    spark.sql(
        f"""
        MERGE INTO {target} t
        USING {view_name} s
        ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    )


@F.udf(returnType=DoubleType())
def _sentiment_udf(text: str | None) -> float | None:
    """Lightweight polarity score in [-1, 1]. The import lives inside the
    UDF so it resolves on the executor's Python worker (cached in
    sys.modules after the first row — not a per-row cost). Only the scoring of
    a malformed body (ValueError/TypeError) yields NULL for that row; a missing
    textblob install or corpus is a deployment error and is allowed to fail
    the stage instead of silently nulling every score."""
    if text is None:
        return None
    from textblob import TextBlob

    try:
        return float(TextBlob(text).sentiment.polarity)
    except (ValueError, TypeError):
        return None


def build_silver_reviews(bronze_reviews_df: DataFrame) -> DataFrame:
    """
    Deduplicate by review_id (at-least-once bronze may have duplicates),
    apply lightweight sentiment scoring, and write to silver.reviews.
    """
    window = Window.partitionBy("review_id").orderBy(F.col("ingested_at").desc())
    return (
        bronze_reviews_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("sentiment_score", _sentiment_udf(F.col("body")))
    )


def build_silver_products(bronze_df: DataFrame) -> DataFrame:
    """Conforms the raw Bronze products snapshot to the silver.products contract.

    Drops audit-only columns (ingested_at, created_at) not needed downstream.
    Casts unit_price to DECIMAL to match the Iceberg schema contract.
    No deduplication needed — bronze.products is a full overwrite snapshot so
    there is exactly one row per product_id per run.
    """
    return (
        bronze_df.select(
            F.col("product_id").cast("long"),
            F.col("sku"),
            F.col("name"),
            F.col("category"),
            F.col("unit_price").cast("decimal(10,2)"),
            F.to_timestamp("updated_at").alias("updated_at"),
        )
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("sku").isNotNull())
    )


def build_silver_order_items(bronze_df: DataFrame) -> DataFrame:
    """Conforms raw Bronze order_items to the silver.order_items contract.

    Deduplicates on order_item_id keeping the latest ingested_at in case of a
    Bronze re-ingestion overlap at a watermark boundary. Drops ingested_at since
    Silver represents business state, not the ingestion audit trail. Uses the
    same _latest_per_key() window pattern as parse_customers_from_cdc() and
    build_silver_reviews() for consistency across the codebase.
    """
    return (
        _latest_per_key(bronze_df, key_col="order_item_id", order_col="ingested_at")
        .select(
            F.col("order_item_id").cast("long"),
            F.col("order_id").cast("long"),
            F.col("product_id").cast("long"),
            F.col("quantity").cast("int"),
            F.col("unit_price").cast("decimal(10,2)"),
        )
        .filter(F.col("order_item_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
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


# ── Shared stage steps ────────────────────────────────────────────────────────
# Each helper below is used by BOTH the per-stage entrypoints and the full
# (monolithic) run, so the silver/gold logic exists exactly once.

VALID_EVENT_TYPES = [
    "page_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "checkout_complete",
]

# Gold tables mirrored to ClickHouse. Order matters: it is the load order.
GOLD_TABLE_NAMES = [
    "daily_sales",
    "top_products",
    "customer_segments",
    "conversion_rate",
    "campaign_effectiveness",
    "product_sentiment",
    "dim_product",
    "dim_campaign",
    "customer_clv",
    "funnel_conversion",
    "roas",
    "quarantine_summary",
    "quality_gate_summary",
    "fact_order_items",
    "dim_date",
]


def _validate_schema(schema: StructType, dataset_name: str) -> None:
    # Imported lazily, as before: pulls in the metadata registry only when a
    # stage actually runs.
    from dataone.metadata.contracts import validate_schema

    validate_schema(schema, dataset_name)


def _record_output(tracker, table: str, quality: QualityResult) -> None:
    tracker.add_output(
        table,
        records_written=quality.passed_count,
        records_failed=quality.quarantined_count,
    )


@contextlib.contextmanager
def _stage_session(job_name: str) -> typing.Iterator[SparkSession]:
    """Spark session for one job: bootstraps the lakehouse, logs and re-raises
    any failure, and always stops the session."""
    spark: SparkSession | None = None
    try:
        spark = build_spark_session(job_name)
        bootstrap_lakehouse(spark)
        yield spark
    except Exception as exc:
        log.error(f"{job_name}.failed", error=str(exc), exc_info=True)
        raise
    finally:
        if spark is not None:
            spark.stop()


def _read_silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format("iceberg").load(table_identifier("silver", table))


def standardize_campaigns(bronze: dict[str, DataFrame], tracker) -> QualityResult:
    quality = run_quality_gate(bronze["campaigns"], dataset_name="bronze.campaigns")
    _validate_schema(quality.passed_df.schema, "silver.campaigns")
    write_overwrite_partitions(quality.passed_df, "silver", "campaigns")
    write_append(quality.quarantined_df, "quarantine", "campaigns")
    _record_output(tracker, "silver.campaigns", quality)
    return quality


def standardize_customers(spark: SparkSession, cdc_df: DataFrame, tracker) -> QualityResult:
    customers_df = parse_customers_from_cdc(cdc_df)
    _validate_schema(customers_df.schema, "silver.customers")
    quality = run_quality_gate(customers_df, dataset_name="silver.customers")
    passed = quality.passed_df.cache()
    write_append(quality.quarantined_df, "quarantine", "customers")
    merge_into_silver(spark, passed, "customers", "customer_id")
    _record_output(tracker, "silver.customers", quality)
    passed.unpersist()
    return quality


def standardize_orders(spark: SparkSession, cdc_df: DataFrame, tracker) -> QualityResult:
    orders_df = parse_orders_from_cdc(cdc_df)
    _validate_schema(orders_df.schema, "silver.orders")
    quality = run_quality_gate(orders_df, dataset_name="silver.orders")
    passed = quality.passed_df.cache()
    write_append(quality.quarantined_df, "quarantine", "orders")
    merge_into_silver(spark, passed, "orders", "order_id")
    _record_output(tracker, "silver.orders", quality)
    passed.unpersist()
    return quality


def standardize_reviews(bronze_reviews_df: DataFrame, tracker) -> tuple[QualityResult, DataFrame]:
    """Returns (quality, cached passed DataFrame). The caller unpersists it."""
    quality = run_quality_gate(
        build_silver_reviews(bronze_reviews_df), dataset_name="silver.reviews"
    )
    passed = quality.passed_df.cache()
    _validate_schema(passed.schema, "silver.reviews")
    write_overwrite_partitions(passed, "silver", "reviews")
    write_append(quality.quarantined_df, "quarantine", "reviews")
    _record_output(tracker, "silver.reviews", quality)
    return quality, passed


def standardize_clickstream(
    bronze_clickstream_df: DataFrame, tracker
) -> tuple[QualityResult, DataFrame]:
    """Returns (quality, cached passed DataFrame). The caller unpersists it."""
    quality = run_quality_gate(bronze_clickstream_df, dataset_name="quarantine.clickstream")
    is_valid_event = F.col("event_type").isin(VALID_EVENT_TYPES)

    passed = quality.passed_df.filter(is_valid_event).cache()
    invalid_event_quarantine = quality.passed_df.filter(~is_valid_event).withColumn(
        "_quarantine_reason", F.lit("invalid_event_type")
    )
    final_quarantine = quality.quarantined_df.unionByName(invalid_event_quarantine)

    _validate_schema(passed.schema, "silver.clickstream")
    write_overwrite_partitions(passed, "silver", "clickstream")
    write_append(final_quarantine, "quarantine", "clickstream")
    tracker.add_output(
        "silver.clickstream",
        records_written=passed.count(),
        records_failed=final_quarantine.count(),
    )
    return quality, passed


def standardize_products(spark: SparkSession, tracker) -> QualityResult:
    """Products -> silver.products (SCD Type 1 upsert)."""
    bronze_df = spark.read.format("iceberg").load(table_identifier("bronze", "products"))
    quality = run_quality_gate(build_silver_products(bronze_df), dataset_name="quarantine.products")
    merge_into_silver(spark, quality.passed_df, "products", "product_id")
    write_append(quality.quarantined_df, "quarantine", "products")
    _record_output(tracker, "silver.products", quality)
    return quality


def standardize_order_items(spark: SparkSession, tracker) -> QualityResult:
    """Order items -> silver.order_items (insert-only merge)."""
    bronze_df = spark.read.format("iceberg").load(table_identifier("bronze", "order_items"))
    quality = run_quality_gate(
        build_silver_order_items(bronze_df), dataset_name="quarantine.order_items"
    )
    merge_into_silver(spark, quality.passed_df, "order_items", "order_item_id")
    write_append(quality.quarantined_df, "quarantine", "order_items")
    _record_output(tracker, "silver.order_items", quality)
    return quality


def build_and_write_fact_order_items(
    orders_silver_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    customer_dim_full_df: DataFrame,
    tracker,
) -> tuple[DataFrame, QualityResult]:
    """Builds gold.fact_order_items, runs its quality gate and writes it.

    Returns (cached fact DataFrame, quality result). The caller unpersists the
    DataFrame."""
    fact_order_items_df = (
        build_fact_order_items(orders_silver_df, order_items_df, products_df, customer_dim_full_df)
        .withColumn(
            "sk_order_id",
            F.md5(
                F.concat_ws(
                    "||",
                    F.lit("postgres"),
                    F.col("order_id").cast("string"),
                    F.col("product_id").cast("string"),
                )
            ),
        )
        .cache()
    )

    quality_result = run_quality_gate(fact_order_items_df, dataset_name="gold.fact_order_items")
    reconcile_row_counts(
        source_count=fact_order_items_df.count(),
        landed_count=quality_result.passed_count + quality_result.quarantined_count,
    )
    _validate_schema(quality_result.passed_df.schema, "gold.fact_order_items")
    write_overwrite_partitions(
        quality_result.passed_df.sort("order_date", "customer_id"),
        "gold",
        "fact_order_items",
    )
    write_append(quality_result.quarantined_df, "quarantine", "fact_order_items")
    _record_output(tracker, "gold.fact_order_items", quality_result)
    return fact_order_items_df, quality_result


def build_and_write_gold_marts(
    spark: SparkSession,
    tracker,
    fact_quality: QualityResult,
    products_df: DataFrame,
    customer_dim_full_df: DataFrame,
    customer_dim_current_df: DataFrame,
    clickstream_df: DataFrame,
    reviews_df: DataFrame,
    campaigns_df: DataFrame,
    quality_results_by_table: dict[str, QualityResult | None],
) -> None:
    """Builds every gold mart other than fact_order_items and writes it to Iceberg."""
    build_dim_date(spark).writeTo(table_identifier("gold", "dim_date")).createOrReplace()
    tracker.add_output("gold.dim_date", records_written=1096)

    fact_df = fact_quality.passed_df
    gold = {
        "daily_sales": build_daily_sales(fact_df),
        "top_products": build_top_products(fact_df, products_df),
        "customer_segments": build_customer_segments(fact_df, customer_dim_full_df),
        "conversion_rate": build_conversion_rate(clickstream_df),
        "campaign_effectiveness": build_campaign_effectiveness(campaigns_df),
        "product_sentiment": build_product_sentiment(reviews_df, products_df),
        "dim_product": build_dim_product(products_df),
        "dim_campaign": build_dim_campaign(campaigns_df),
        "customer_clv": build_customer_clv(fact_df, customer_dim_current_df),
        "funnel_conversion": build_funnel_conversion(clickstream_df),
        "roas": build_roas(fact_df, campaigns_df),
        "quarantine_summary": build_quarantine_summary(spark),
        "quality_gate_summary": build_quality_gate_summary(
            spark, date.today(), quality_results_by_table
        ),
    }
    for name, df in gold.items():
        _validate_schema(df.schema, f"gold.{name}")
        write_overwrite_partitions(df, "gold", name)
        tracker.add_output(f"gold.{name}", records_written=df.count())


def sync_gold_to_clickhouse(spark: SparkSession) -> None:
    gold_from_iceberg = {
        name: spark.read.format("iceberg").load(table_identifier("gold", name))
        for name in GOLD_TABLE_NAMES
    }
    load_clickhouse_marts(gold_from_iceberg)


# ── Stage entrypoints ─────────────────────────────────────────────────────────


def run_ingest_bronze(args: argparse.Namespace) -> None:
    from dataone.lineage.tracker import LineageTracker

    job = "bronze_to_silver.ingest_bronze"
    with LineageTracker(job, args.start, args.end) as tracker:
        tracker.add_input("mongodb.reviews")
        tracker.add_input("nifi.campaigns")
        tracker.add_input("postgres.products")
        tracker.add_input("postgres.order_items")
        with _stage_session(job) as spark:
            campaigns_files = ingest_campaigns_to_bronze(spark)
            reviews_count = ingest_reviews_to_bronze(spark)
            products_count = ingest_products_to_bronze(spark)
            order_items_count = ingest_order_items_to_bronze(spark)
            tracker.add_output("bronze.campaigns", records_written=campaigns_files)
            tracker.add_output("bronze.reviews", records_written=reviews_count)
            tracker.add_output("bronze.products", records_written=products_count)
            tracker.add_output("bronze.order_items", records_written=order_items_count)
            log.info(f"{job}.done")


def run_standardize_silver(args: argparse.Namespace) -> None:
    from dataone.lineage.tracker import LineageTracker

    job = "bronze_to_silver.standardize_silver"
    with LineageTracker(job, args.start, args.end) as tracker:
        for name in (
            "orders_cdc",
            "clickstream",
            "campaigns",
            "reviews",
            "products",
            "order_items",
        ):
            tracker.add_input(f"bronze.{name}")
        with _stage_session(job) as spark:
            bronze = read_bronze_tables(spark, args.start, args.end)

            standardize_campaigns(bronze, tracker)
            standardize_customers(spark, bronze["cdc"], tracker)
            standardize_orders(spark, bronze["cdc"], tracker)
            _, reviews_passed = standardize_reviews(bronze["reviews"], tracker)
            reviews_passed.unpersist()
            _, clickstream_passed = standardize_clickstream(bronze["clickstream"], tracker)
            clickstream_passed.unpersist()
            standardize_products(spark, tracker)
            standardize_order_items(spark, tracker)
            log.info(f"{job}.done")


def run_model_gold(args: argparse.Namespace) -> None:
    from dataone.lineage.tracker import LineageTracker

    job = "bronze_to_silver.model_gold"
    with LineageTracker(job, args.start, args.end) as tracker:
        for name in (
            "customers",
            "orders",
            "reviews",
            "clickstream",
            "order_items",
            "products",
            "campaigns",
        ):
            tracker.add_input(f"silver.{name}")
        with _stage_session(job) as spark:
            customers_silver_df = _read_silver(spark, "customers")
            orders_silver_df = _read_silver(spark, "orders")
            reviews_df = _read_silver(spark, "reviews")
            clickstream_df = _read_silver(spark, "clickstream")
            campaigns_df = _latest_per_key(
                _read_silver(spark, "campaigns"),
                key_col="campaign_id",
                order_col="ingested_at",
            )

            apply_scd2_merge(spark, customers_silver_df)
            customer_dim_full_df = spark.read.format("iceberg").load(
                table_identifier("gold", "dim_customer")
            )
            customer_dim_current_df = customer_dim_full_df.filter(F.col("is_current"))

            # Read from Silver — no direct Postgres connection
            order_items_df = _read_silver(spark, "order_items")
            products_df = _read_silver(spark, "products")

            fact_order_items_df, fact_quality = build_and_write_fact_order_items(
                orders_silver_df, order_items_df, products_df, customer_dim_full_df, tracker
            )

            # The standalone gold stage has no in-memory quality results for the
            # silver tables; build_quality_gate_summary falls back to counting
            # the Iceberg tables for those.
            quality_results_by_table: dict[str, QualityResult | None] = {
                "campaigns": None,
                "customers": None,
                "orders": None,
                "products": None,
                "fact_order_items": fact_quality,
                "reviews": None,
                "clickstream": None,
            }
            build_and_write_gold_marts(
                spark,
                tracker,
                fact_quality,
                products_df,
                customer_dim_full_df,
                customer_dim_current_df,
                clickstream_df,
                reviews_df,
                campaigns_df,
                quality_results_by_table,
            )

            fact_order_items_df.unpersist()
            log.info(f"{job}.done")


def run_sync_clickhouse(args: argparse.Namespace) -> None:
    from dataone.lineage.tracker import LineageTracker

    job = "bronze_to_silver.sync_clickhouse"
    with LineageTracker(job, args.start, args.end) as tracker:
        for name in GOLD_TABLE_NAMES:
            tracker.add_input(f"gold.{name}")
        with _stage_session(job) as spark:
            sync_gold_to_clickhouse(spark)
            log.info(f"{job}.done")


def run_full_pipeline(args: argparse.Namespace) -> None:
    """All stages in one Spark session and one lineage run (no --stage given)."""
    from dataone.lineage.tracker import LineageTracker

    job = "bronze_to_silver"
    with LineageTracker(job, args.start, args.end) as tracker:
        tracker.add_input("bronze.orders_cdc")
        tracker.add_input("bronze.clickstream")
        tracker.add_input("bronze.campaigns")
        tracker.add_input("bronze.reviews")
        tracker.add_input("postgres.products")  # ingested to bronze.products
        tracker.add_input("postgres.order_items")  # ingested to bronze.order_items
        tracker.add_input("mongodb.reviews")
        tracker.add_input("nifi.campaigns")

        with _stage_session(job) as spark:
            ingest_campaigns_to_bronze(spark)
            ingest_reviews_to_bronze(spark)
            ingest_products_to_bronze(spark)
            ingest_order_items_to_bronze(spark)

            bronze = read_bronze_tables(spark, args.start, args.end)

            campaigns_quality = standardize_campaigns(bronze, tracker)
            campaigns_df = campaigns_quality.passed_df
            customers_quality = standardize_customers(spark, bronze["cdc"], tracker)
            orders_quality = standardize_orders(spark, bronze["cdc"], tracker)

            apply_scd2_merge(spark, _read_silver(spark, "customers"))
            orders_silver_df = _read_silver(spark, "orders")
            customer_dim_full_df = spark.read.format("iceberg").load(
                table_identifier("gold", "dim_customer")
            )
            customer_dim_current_df = customer_dim_full_df.filter(F.col("is_current"))

            products_quality = standardize_products(spark, tracker)
            standardize_order_items(spark, tracker)

            # Read from Silver — no direct Postgres connection in Gold
            order_items_df = _read_silver(spark, "order_items")
            products_df = _read_silver(spark, "products")

            fact_order_items_df, fact_quality = build_and_write_fact_order_items(
                orders_silver_df, order_items_df, products_df, customer_dim_full_df, tracker
            )

            reviews_quality, reviews_passed = standardize_reviews(bronze["reviews"], tracker)
            clickstream_quality, clickstream_passed = standardize_clickstream(
                bronze["clickstream"], tracker
            )

            quality_results_by_table: dict[str, QualityResult | None] = {
                "campaigns": campaigns_quality,
                "customers": customers_quality,
                "orders": orders_quality,
                "products": products_quality,
                "fact_order_items": fact_quality,
                "reviews": reviews_quality,
                "clickstream": clickstream_quality,
            }
            build_and_write_gold_marts(
                spark,
                tracker,
                fact_quality,
                products_df,
                customer_dim_full_df,
                customer_dim_current_df,
                clickstream_passed,
                reviews_passed,
                campaigns_df,
                quality_results_by_table,
            )

            sync_gold_to_clickhouse(spark)

            fact_order_items_df.unpersist()
            reviews_passed.unpersist()
            clickstream_passed.unpersist()
            log.info(f"{job}.done")


_STAGES = {
    "ingest_bronze": run_ingest_bronze,
    "standardize_silver": run_standardize_silver,
    "model_gold": run_model_gold,
    "sync_clickhouse": run_sync_clickhouse,
}


def main() -> None:
    """Main entrypoint for the bronze-to-silver batch pipeline.

    With --stage, runs just that stage (this is how the Prefect flow calls it);
    without, runs ingest -> silver -> gold -> ClickHouse in one Spark session.
    """
    args = parse_args()
    log.info("bronze_to_silver.start", start=args.start, end=args.end, stage=args.stage)
    _STAGES.get(args.stage, run_full_pipeline)(args)


if __name__ == "__main__":
    main()
