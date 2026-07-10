"""
Shared SparkSession builder for both the batch and streaming jobs — wires up
the Iceberg catalog config from iceberg_helpers.spark_catalog_conf() so both
jobs definitely agree on catalog settings instead of each hardcoding their
own copy, with Kafka packages added only when the streaming job needs them.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from dataone.config import iceberg
from dataone.utils.iceberg_helpers import spark_catalog_conf

# Versions matched to what's baked into infra/docker/spark/Dockerfile.
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
OPENLINEAGE_PACKAGE = "io.openlineage:openlineage-spark_2.12:1.13.1"


def build_spark_session(app_name: str, with_kafka: bool = False) -> SparkSession:
    """Builds and configures a shared SparkSession.

    The custom Spark image already bakes the Iceberg jar onto the classpath
    at build time, so spark.jars.packages isn't strictly required for
    Iceberg in that container — but spark-sql-kafka isn't baked in, so
    streaming jobs still need it via packages. Listing both here is
    harmless (Spark no-ops a package it already has) and keeps this
    function correct even if it's ever run somewhere the jar wasn't
    pre-baked.

    Args:
        app_name (str): The name of the Spark application.
        with_kafka (bool, optional): Whether to include Kafka packages for 
            streaming jobs. Defaults to False.

    Returns:
        SparkSession: The configured SparkSession instance.
    """
    builder = SparkSession.builder.appName(app_name)

    packages = [ICEBERG_PACKAGE, OPENLINEAGE_PACKAGE]
    if with_kafka:
        packages.append(KAFKA_PACKAGE)
    builder = builder.config("spark.jars.packages", ",".join(packages))

    if app_name == "dataone-streaming":
        builder = builder.config("spark.sql.streaming.metricsEnabled", "true")
        builder = builder.config("spark.ui.prometheus.enabled", "true")

    # OpenLineage configuration
    builder = builder.config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    builder = builder.config("spark.openlineage.namespace", "dataone")

    # --- Switch Transport from HTTP to Kafka ---
    builder = builder.config("spark.openlineage.transport.type", "kafka")
    builder = builder.config("spark.openlineage.transport.topicName", "openlineage-events")
    builder = builder.config("spark.openlineage.transport.properties.bootstrap.servers", "kafka:29092")
    builder = builder.config("spark.openlineage.transport.properties.key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    builder = builder.config("spark.openlineage.transport.properties.value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    # --- Filter Out Temporary Datasets ---
    builder = builder.config("spark.openlineage.dataset.removePath.pattern", r"(.*\/tmp\/.*)|(.*\/_checkpoints\/.*)")

    for key, value in spark_catalog_conf().items():
        builder = builder.config(key, value)

    # Unqualified table names resolve into our catalog, not Spark's
    # built-in spark_catalog.
    builder = builder.config("spark.sql.defaultCatalog", iceberg.catalog_name)

    # Performance tuning for the single-worker local cluster: AQE coalesces
    # post-shuffle partitions at runtime (so the 200-partition default doesn't
    # spray tiny files), and a modest static shuffle-partition count keeps
    # task-scheduling overhead down on 1 executor core. Both are no-op-safe
    # if a larger cluster overrides them via spark-submit --conf.
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.sql.shuffle.partitions", "16")

    # zstd instead of Spark's default snappy. NOT an Alpine/musl-vs-glibc
    # workaround — apache/spark:3.5.1-python3 is Debian/Ubuntu-based, not
    # Alpine, so that specific failure mode doesn't apply here. If a real
    # Snappy native-library error shows up, the more likely cause given our
    # own setup is a classpath conflict: infra/docker/spark/Dockerfile bakes
    # three separately-downloaded jars (Iceberg runtime, Postgres JDBC,
    # ClickHouse JDBC) onto the same classpath Spark already populates, and
    # if any of them bundle their own shaded snappy-java at a different
    # version, that's a collision we introduced, not a base-image issue.
    # zstd is also just a reasonable choice on its own merits (better
    # compression ratio than snappy at a modest CPU cost). This SparkConf
    # setting alone may not reliably reach Iceberg's own Parquet writer
    # though — see iceberg_helpers.create_table_sql()'s
    # write.parquet.compression-codec TBLPROPERTIES, which sets Iceberg's
    # own dedicated knob for the same thing, so we're not depending on
    # either mechanism alone.
    builder = builder.config("spark.sql.parquet.compression.codec", "zstd")

    return builder.getOrCreate()