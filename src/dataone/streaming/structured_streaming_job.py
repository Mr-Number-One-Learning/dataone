"""
Spark Structured Streaming job: consumes the orders-cdc and clickstream
Kafka topics continuously, appends raw events into Iceberg bronze tables,
and writes a lightweight per-minute "live activity" aggregate straight to
ClickHouse for the real-time portion of the dashboard.

Note on what "live" means here: revenue/order-amount metrics aren't
derivable from streaming sources alone — order_items/products are
batch-JDBC-only (see utils/schemas.py module docstring for why). So the live
aggregate below is activity counts (events, active sessions, checkout
completions), not revenue; the revenue marts are the batch job's job
(batch/bronze_to_silver.py).

Run (inside the spark-worker-streaming container):
    spark-submit structured_streaming_job.py

TESTABILITY NOTE: real PySpark Structured Streaming + Iceberg + Kafka +
ClickHouse JDBC — written carefully against the documented APIs (the Iceberg
streaming sink's `.toTable()` call was verified against current Iceberg
docs, not recalled from memory), but this project's dev sandbox can't run
any of it for real. First real run is on your laptop.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import LongType, StringType, StructField, StructType

from dataone.config import clickhouse, kafka
from dataone.utils.iceberg_helpers import bootstrap_namespaces_sql, table_identifier
from dataone.utils.logging_config import get_logger
from dataone.utils.schemas import ALL_TABLES, create_all_tables_sql
from dataone.utils.spark_session import build_spark_session

log = get_logger(__name__)

CHECKPOINT_BASE = "/data/lakehouse/checkpoints"

# Outer Kafka envelope for orders-cdc — flat and uniform regardless of which
# source table (customers/orders) the row came from; `data` is pre-stringified
# JSON, not a nested object — see ingestion/cdc_simulator.py's
# emit_change_event() for why.
_CDC_ENVELOPE_SCHEMA = StructType(
    [
        StructField("table", StringType()),
        StructField("op", StringType()),
        StructField("pk_column", StringType()),
        StructField("data", StringType()),
        StructField("captured_at", StringType()),
    ]
)

# Matches clickstream_generator.build_event()'s shape exactly.
_CLICKSTREAM_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("event_type", StringType()),
        StructField("product_id", LongType()),
        StructField("customer_id", LongType()),  # absent in JSON for anonymous events -> null
        StructField("ts", StringType()),
    ]
)


def bootstrap_lakehouse(spark) -> None:
    """Bootstraps the Iceberg namespaces and tables if they don't exist.

    Idempotent — same bootstrap the batch job runs, duplicated here
    rather than imported across job boundaries so this job can start cold
    (e.g. before the batch job has ever run) without a hard dependency.

    Args:
        spark: The active SparkSession.
    """
    for stmt in bootstrap_namespaces_sql():
        spark.sql(stmt)
    for stmt in create_all_tables_sql():
        spark.sql(stmt)
    log.info("bootstrap_lakehouse.done", tables=len(ALL_TABLES))


def read_kafka_stream(spark, topic: str) -> DataFrame:
    """Reads a streaming DataFrame from a Kafka topic.

    Args:
        spark: The active SparkSession.
        topic (str): The Kafka topic to subscribe to.

    Returns:
        DataFrame: A streaming DataFrame containing raw Kafka data.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def parse_with_dead_letter(
    raw_df: DataFrame, schema: StructType, good_name: str
) -> tuple[DataFrame, DataFrame]:
    """Splits a raw Kafka stream into successfully parsed and dead-letter DataFrames.

    Split a raw Kafka stream into (parseable, dead-letter). "Bad" means the
    JSON didn't parse at all, or its first schema field (the record's
    identifier — table for CDC, event_id for clickstream) is null: an
    identifier-less record can't be deduplicated or keyed downstream, so it
    belongs in the DLQ for human triage rather than in bronze. Deeper
    semantic validation is deliberately left to the batch quality gate.

    Args:
        raw_df (DataFrame): The raw streaming DataFrame from Kafka.
        schema (StructType): The expected schema to apply to the JSON payload.
        good_name (str): The name/source string to tag bad records with.

    Returns:
        tuple[DataFrame, DataFrame]: A tuple of (good_dataframe, bad_dataframe).
    """
    parsed = raw_df.select(
        "*",
        F.from_json(F.col("value").cast("string"), schema).alias("_parsed"),
    )
    first_field = schema.fields[0].name
    is_bad = F.col("_parsed").isNull() | F.col(f"_parsed.{first_field}").isNull()

    good = parsed.filter(~is_bad)
    bad = parsed.filter(is_bad).select(
        F.col("value").cast("string").alias("value"),
        F.lit(good_name).alias("source_topic"),
        F.current_timestamp().alias("failed_at"),
    )
    return good, bad


def parse_cdc_stream(good_parsed: DataFrame) -> DataFrame:
    """Transforms a parsed CDC stream into the bronze table schema.

    Raw Kafka columns -> bronze.orders_cdc's exact column set.

    Args:
        good_parsed (DataFrame): The successfully parsed DataFrame.

    Returns:
        DataFrame: The formatted DataFrame ready for insertion into bronze.orders_cdc.
    """
    return good_parsed.select(
        F.col("_parsed.table").alias("table_name"),
        F.col("_parsed.op").alias("op"),
        F.col("_parsed.pk_column").alias("pk_column"),
        F.col("key").cast("string").alias("pk_value"),
        F.col("_parsed.data").alias("data_json"),
        F.to_timestamp(F.col("_parsed.captured_at")).alias("captured_at"),
    )


def parse_clickstream_stream(good_parsed: DataFrame) -> DataFrame:
    """Transforms a parsed clickstream stream into the bronze table schema.

    Raw Kafka columns -> bronze.clickstream's exact column set.

    Args:
        good_parsed (DataFrame): The successfully parsed DataFrame.

    Returns:
        DataFrame: The formatted DataFrame ready for insertion into bronze.clickstream.
    """
    return good_parsed.select(
        F.col("_parsed.event_id"),
        F.col("_parsed.session_id"),
        F.col("_parsed.event_type"),
        F.col("_parsed.product_id"),
        F.col("_parsed.customer_id"),
        F.to_timestamp(F.col("_parsed.ts")).alias("ts"),
    )


def write_to_bronze(
    df: DataFrame,
    layer: str,
    table_name: str,
    checkpoint_name: str,
    dedup_cols: list[str] | None = None,
    event_time_col: str | None = None,
    watermark_delay: str = "10 minutes",
) -> StreamingQuery:
    """Appends a parsed stream into an Iceberg bronze table.

    When dedup_cols is given, an event-time watermark is REQUIRED
    (event_time_col): plain dropDuplicates on a stream keeps every key ever
    seen in the state store — unbounded memory growth. Bounding the dedup
    window with dropDuplicatesWithinWatermark keeps state finite; duplicates
    arriving later than watermark_delay slip through here and are collapsed
    by the batch job's silver-side dedup.

    Args:
        df (DataFrame): The parsed streaming DataFrame.
        layer (str): The target catalog layer (e.g., "bronze").
        table_name (str): The target table name.
        checkpoint_name (str): The path suffix for the streaming checkpoint.
        dedup_cols (list[str] | None, optional): Columns to deduplicate on. Defaults to None.
        event_time_col (str | None, optional): Event time column for watermarking. Required if dedup_cols is set. Defaults to None.
        watermark_delay (str, optional): The watermark delay duration. Defaults to "10 minutes".

    Returns:
        StreamingQuery: The active streaming query handle.

    Raises:
        ValueError: If dedup_cols is provided without event_time_col.
    """
    if dedup_cols:
        if not event_time_col:
            raise ValueError("dedup_cols requires event_time_col to bound streaming state")
        df = df.withWatermark(event_time_col, watermark_delay).dropDuplicatesWithinWatermark(
            dedup_cols
        )
    target = table_identifier(layer, table_name)
    return (
        df.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_name}")
        .toTable(target)
    )


def build_live_activity_aggregate(clickstream_df: DataFrame) -> DataFrame:
    """Builds a real-time activity aggregate from the clickstream.
    
    1-minute tumbling window of clickstream activity — the real-time
    pulse metric for the dashboard.

    Args:
        clickstream_df (DataFrame): The parsed clickstream DataFrame.

    Returns:
        DataFrame: A windowed streaming DataFrame containing aggregate counts.
    """
    watermarked = clickstream_df.withWatermark("ts", "2 minutes")
    return watermarked.groupBy(F.window("ts", "1 minute")).agg(
        F.count("*").alias("event_count"),
        F.approx_count_distinct("session_id").alias("active_sessions"),
        F.sum(F.when(F.col("event_type") == "checkout_complete", 1).otherwise(0)).alias(
            "checkout_completions"
        ),
    )


# Windows whose checkout completion rate falls below this fraction are
# flagged as anomalies (e.g. a broken payment provider: sessions keep
# starting checkout but almost none complete).
ANOMALY_COMPLETION_RATE_THRESHOLD = 0.01


def build_anomaly_detector(clickstream_df: DataFrame) -> DataFrame:
    """Builds an anomaly detection stream for checkout completion rates.

    Flags 1-minute windows whose checkout completion rate
    (checkout_complete / checkout_start) drops below
    ANOMALY_COMPLETION_RATE_THRESHOLD.

    Implemented as ONE windowed aggregation with conditional sums rather
    than joining two aggregated streams — Spark disallows stream-stream
    joins downstream of aggregations, so the earlier two-branch + join
    formulation would have failed at query start.

    Args:
        clickstream_df (DataFrame): The parsed clickstream DataFrame.

    Returns:
        DataFrame: A streaming DataFrame containing anomaly alerts.
    """
    watermarked = clickstream_df.withWatermark("ts", "10 minutes")
    windowed = (
        watermarked.filter(F.col("event_type").isin(["checkout_start", "checkout_complete"]))
        .groupBy(F.window("ts", "1 minute"))
        .agg(
            F.sum(F.when(F.col("event_type") == "checkout_start", 1).otherwise(0)).alias(
                "start_count"
            ),
            F.sum(F.when(F.col("event_type") == "checkout_complete", 1).otherwise(0)).alias(
                "complete_count"
            ),
        )
    )
    return (
        windowed.filter(F.col("start_count") > 0)
        .withColumn("completion_rate", F.col("complete_count") / F.col("start_count"))
        .filter(F.col("completion_rate") < ANOMALY_COMPLETION_RATE_THRESHOLD)
    )


def _write_batch_to_clickhouse(table_name: str):
    """Generates a foreachBatch function to write to ClickHouse.

    Returns a foreachBatch function bound to table_name — the standard
    pattern for sinking Structured Streaming output to a JDBC target (no
    native ClickHouse streaming sink exists).

    Args:
        table_name (str): The target ClickHouse table name.

    Returns:
        Callable: A function matching the foreachBatch signature (batch_df, batch_id).
    """

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        # Delivery is at-least-once: if Spark retries a micro-batch after a
        # partial write, the same window rows are appended again. The
        # ClickHouse table is ReplacingMergeTree keyed on window_start (see
        # infra/docker/clickhouse/init/002_marts.sql), so replays collapse
        # to one row per window at merge time — dashboard queries stay
        # correct without exactly-once JDBC gymnastics.
        jdbc_url = f"jdbc:clickhouse://{clickhouse.host}:{clickhouse.port}/{clickhouse.db}"
        (
            batch_df.selectExpr(
                "window.start as window_start",
                "window.end as window_end",
                "event_count",
                "active_sessions",
                "checkout_completions",
            )
            .write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", clickhouse.user)
            .option("password", clickhouse.password)
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .mode("append")
            .save()
        )
        log.info("live_activity.batch_written", batch_id=batch_id, table=table_name)

    return _write


def write_live_aggregates_to_clickhouse(aggregate_df: DataFrame) -> StreamingQuery:
    """Starts a streaming query to continuously update ClickHouse.

    Args:
        aggregate_df (DataFrame): The streaming DataFrame of aggregates.

    Returns:
        StreamingQuery: The active streaming query handle.
    """
    return (
        aggregate_df.writeStream.outputMode("update")
        .trigger(processingTime="1 minute")
        .foreachBatch(_write_batch_to_clickhouse("live_activity"))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/live_activity")
        .start()
    )


def main() -> None:
    """Main function for the structured streaming job.

    Initializes the Spark session, bootstraps the lakehouse, defines all
    streaming pipelines (CDC parsing, clickstream parsing, anomalies, aggregates),
    starts them, and awaits termination.
    """
    spark = build_spark_session("dataone-streaming", with_kafka=True)
    bootstrap_lakehouse(spark)

    cdc_raw = read_kafka_stream(spark, kafka.topic_orders_cdc)
    cdc_good, cdc_bad = parse_with_dead_letter(cdc_raw, _CDC_ENVELOPE_SCHEMA, kafka.topic_orders_cdc)
    cdc_parsed = parse_cdc_stream(cdc_good)
    
    clickstream_raw = read_kafka_stream(spark, kafka.topic_clickstream)
    clickstream_good, clickstream_bad = parse_with_dead_letter(clickstream_raw, _CLICKSTREAM_EVENT_SCHEMA, kafka.topic_clickstream)
    clickstream_parsed = parse_clickstream_stream(clickstream_good)

    from dataone.lineage.tracker import LineageTracker
    from dataone.metadata.contracts import validate_schema

    with LineageTracker("structured_streaming_job") as tracker:
        tracker.add_input("kafka.orders_cdc")
        tracker.add_input("kafka.clickstream")
        tracker.add_output("bronze.orders_cdc")
        tracker.add_output("bronze.clickstream")
        tracker.add_output("bronze.dead_letters")
        tracker.add_output("kafka.anomaly_alerts")

        # Validate contract schemas statically before running queries
        validate_schema(cdc_parsed.schema, "bronze.orders_cdc")
        validate_schema(clickstream_parsed.schema, "bronze.clickstream")

        cdc_query = write_to_bronze(cdc_parsed, "bronze", "orders_cdc", "orders_cdc")
        clickstream_query = write_to_bronze(
            clickstream_parsed,
            "bronze",
            "clickstream",
            "clickstream",
            dedup_cols=["event_id"],
            event_time_col="ts",
        )

        dlq_query = write_to_bronze(cdc_bad.unionByName(clickstream_bad), "bronze", "dead_letters", "dead_letters")

        live_activity_query = write_live_aggregates_to_clickhouse(
            build_live_activity_aggregate(clickstream_parsed)
        )

        anomaly_query = (
            build_anomaly_detector(clickstream_parsed)
            .selectExpr("CAST(window.start AS STRING) AS key", "to_json(struct(*)) AS value")
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
            .option("topic", kafka.topic_anomaly_alerts)
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/anomaly_alerts")
            .outputMode("append")
            .start()
        )

        queries = [cdc_query, clickstream_query, dlq_query, live_activity_query, anomaly_query]
        log.info("structured_streaming_job.started", queries=[q.name for q in queries])
        try:
            spark.streams.awaitAnyTermination()
        except Exception as exc:
            log.error("structured_streaming_job.query_failed", error=str(exc), exc_info=True)
            raise
        finally:
            # awaitAnyTermination returns/raises when ONE query dies — stop the
            # survivors cleanly (checkpoints make restart safe) instead of
            # leaving them running against a session we're about to tear down.
            for q in queries:
                if q.isActive:
                    q.stop()
            spark.stop()


if __name__ == "__main__":
    main()

