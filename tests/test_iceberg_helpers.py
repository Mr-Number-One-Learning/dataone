"""
Tests for the Spark+Iceberg catalog wiring helpers. All pure string/dict
building — no Spark session needed, which is exactly why this module is
fully testable while the Spark jobs that consume it are not.
"""
from __future__ import annotations

import pytest

from dataone.config import postgres
from dataone.utils import iceberg_helpers as ih


def test_spark_catalog_conf_has_required_keys():
    conf = ih.spark_catalog_conf()
    name = "dataone_catalog"
    assert conf[f"spark.sql.catalog.{name}"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf[f"spark.sql.catalog.{name}.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
    assert conf[f"spark.sql.catalog.{name}.uri"] == f"jdbc:postgresql://{postgres.host}:{postgres.port}/{postgres.db}"
    assert conf[f"spark.sql.catalog.{name}.jdbc.user"] == postgres.user
    assert conf[f"spark.sql.catalog.{name}.jdbc.password"] == postgres.password
    assert conf[f"spark.sql.catalog.{name}.warehouse"].startswith("file://")
    assert "spark.sql.extensions" in conf


def test_table_identifier_format():
    assert ih.table_identifier("bronze", "orders") == "dataone_catalog.bronze.orders"
    assert ih.table_identifier("quarantine", "rejected") == "dataone_catalog.quarantine.rejected"


def test_table_identifier_rejects_unknown_layer():
    with pytest.raises(ValueError):
        ih.table_identifier("nonsense", "orders")


def test_create_namespace_sql():
    assert ih.create_namespace_sql("silver") == "CREATE NAMESPACE IF NOT EXISTS dataone_catalog.silver"


def test_bootstrap_namespaces_sql_covers_all_layers():
    statements = ih.bootstrap_namespaces_sql()
    assert len(statements) == len(ih.LAYERS)
    for layer in ih.LAYERS:
        assert any(layer in s for s in statements)


def test_create_table_sql_with_partition():
    ddl = ih.create_table_sql(
        "bronze",
        "orders_cdc",
        columns=[("order_id", "BIGINT"), ("order_date", "TIMESTAMP")],
        partition_by=["days(order_date)"],
    )
    assert ddl == (
        "CREATE TABLE IF NOT EXISTS dataone_catalog.bronze.orders_cdc "
        "(order_id BIGINT, order_date TIMESTAMP) USING iceberg PARTITIONED BY (days(order_date)) "
        "TBLPROPERTIES ('write.distribution-mode'='hash','write.parquet.compression-codec'='zstd',"
        "'write.parquet.row-group-size-bytes'='16777216','write.parquet.dict-size-bytes'='1048576',"
        "'write.spark.fanout.enabled'='false')"
    )


def test_create_table_sql_without_partition():
    ddl = ih.create_table_sql("quarantine", "rejected_orders", columns=[("raw", "STRING")])
    assert "PARTITIONED BY" not in ddl
    assert ddl == (
        "CREATE TABLE IF NOT EXISTS dataone_catalog.quarantine.rejected_orders (raw STRING) "
        "USING iceberg TBLPROPERTIES ('write.distribution-mode'='hash','write.parquet.compression-codec'='zstd',"
        "'write.parquet.row-group-size-bytes'='16777216','write.parquet.dict-size-bytes'='1048576',"
        "'write.spark.fanout.enabled'='false')"
    )


def test_create_table_sql_sets_compression_codec_even_when_unpartitioned():
    """Unlike write.distribution-mode (a documented no-op on unpartitioned
    tables), compression-codec applies regardless of partitioning — this
    guards against a future edit moving TBLPROPERTIES back inside an
    `if partition_by:` block, which would silently drop codec control on
    every unpartitioned table (gold.top_products, gold.customer_segments,
    gold.campaign_effectiveness, quarantine.fact_order_items)."""
    ddl = ih.create_table_sql("gold", "top_products", columns=[("product_id", "BIGINT")])
    assert "write.parquet.compression-codec" in ddl


def test_create_table_sql_rejects_empty_columns():
    with pytest.raises(ValueError):
        ih.create_table_sql("bronze", "x", columns=[])