"""
Opt-in integration test: an Iceberg table written and read back through
Iceberg's S3FileIO against a real MinIO.

Excluded from the default run (marker `minio`). To run it:

    docker compose --profile core up -d minio minio-init
    export MINIO_TEST_ENDPOINT=http://localhost:9100   # host port of the minio service
    export MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=...    # the app user from .env
    make test-minio

Needs a JDK and network on first run (Iceberg runtime + AWS bundle jars).
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.minio

ICEBERG_JARS = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.9.1"
)


@pytest.fixture(scope="module")
def minio_spark():
    endpoint = os.getenv("MINIO_TEST_ENDPOINT")
    access = os.getenv("MINIO_ACCESS_KEY")
    secret = os.getenv("MINIO_SECRET_KEY")
    if not (endpoint and access and secret):
        pytest.skip("set MINIO_TEST_ENDPOINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY to run")

    from pyspark.sql import SparkSession

    bucket = os.getenv("MINIO_BUCKET", "lakehouse")
    prefix = f"pytest-{uuid.uuid4().hex[:8]}"
    cat = "spark.sql.catalog.minio_test"
    session = (
        SparkSession.builder.appName("dataone-minio-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_JARS)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.type", "hadoop")
        .config(f"{cat}.warehouse", f"s3://{bucket}/{prefix}")
        .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{cat}.s3.endpoint", endpoint)
        .config(f"{cat}.s3.path-style-access", "true")
        .config(f"{cat}.s3.access-key-id", access)
        .config(f"{cat}.s3.secret-access-key", secret)
        .config(f"{cat}.client.region", os.getenv("MINIO_REGION", "us-east-1"))
        .getOrCreate()
    )
    yield session
    session.sql("DROP TABLE IF EXISTS minio_test.db.roundtrip PURGE")
    session.stop()


def test_iceberg_table_roundtrip_on_minio(minio_spark):
    minio_spark.sql("CREATE NAMESPACE IF NOT EXISTS minio_test.db")
    minio_spark.sql("CREATE TABLE minio_test.db.roundtrip (id BIGINT, name STRING) USING iceberg")
    minio_spark.sql("INSERT INTO minio_test.db.roundtrip VALUES (1, 'a'), (2, 'b')")

    rows = minio_spark.sql("SELECT id, name FROM minio_test.db.roundtrip ORDER BY id").collect()

    assert [(r.id, r.name) for r in rows] == [(1, "a"), (2, "b")]
