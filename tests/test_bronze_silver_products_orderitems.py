"""
Unit tests for the new Bronze/Silver transformation and ingestion functions for
products and order_items. All tests run without a real SparkSession by patching
the PySpark F.* calls that require a live SparkContext.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chainable_df():
    """Returns a MagicMock DataFrame that supports fluent chaining."""
    mock = MagicMock()
    mock.select.return_value = mock
    mock.filter.return_value = mock
    mock.withColumn.return_value = mock
    mock.cache.return_value = mock
    mock.count.return_value = 10
    mock.unpersist.return_value = None
    mock.agg.return_value = mock
    mock.collect.return_value = [[None]]
    write = MagicMock()
    mock.writeTo.return_value = write
    mock._write_mock = write
    return mock


# ---------------------------------------------------------------------------
# Tests for build_silver_products
# ---------------------------------------------------------------------------

class TestBuildSilverProducts:
    """Tests for build_silver_products() Silver transformation function."""

    def test_select_drops_audit_columns(self):
        """build_silver_products must call .select() to project columns
        (thereby dropping ingested_at and created_at)."""
        from dataone.batch.bronze_to_silver import build_silver_products

        bronze_df = _chainable_df()

        with patch("dataone.batch.bronze_to_silver.F") as mock_F:
            mock_F.col.return_value = MagicMock()
            mock_F.to_timestamp.return_value = MagicMock()
            build_silver_products(bronze_df)

        bronze_df.select.assert_called_once()

    def test_filters_null_product_id(self):
        """build_silver_products must call .filter() at least twice
        (product_id not null AND sku not null)."""
        from dataone.batch.bronze_to_silver import build_silver_products

        bronze_df = _chainable_df()

        with patch("dataone.batch.bronze_to_silver.F") as mock_F:
            mock_F.col.return_value = MagicMock()
            mock_F.to_timestamp.return_value = MagicMock()
            build_silver_products(bronze_df)

        assert bronze_df.filter.call_count >= 2

    def test_filters_null_sku(self):
        """At least two filter calls confirm both product_id and sku are checked."""
        from dataone.batch.bronze_to_silver import build_silver_products

        bronze_df = _chainable_df()

        with patch("dataone.batch.bronze_to_silver.F") as mock_F:
            mock_F.col.return_value = MagicMock()
            mock_F.to_timestamp.return_value = MagicMock()
            build_silver_products(bronze_df)

        assert bronze_df.filter.call_count >= 2


# ---------------------------------------------------------------------------
# Tests for build_silver_order_items
# ---------------------------------------------------------------------------

class TestBuildSilverOrderItems:
    """Tests for build_silver_order_items() Silver transformation function."""

    def test_select_applied(self):
        """build_silver_order_items must call .select() to project columns."""
        from dataone.batch.bronze_to_silver import build_silver_order_items

        bronze_df = _chainable_df()

        with patch("dataone.batch.bronze_to_silver._latest_per_key", return_value=bronze_df), \
             patch("dataone.batch.bronze_to_silver.F") as mock_F:
            mock_F.col.return_value = MagicMock()
            build_silver_order_items(bronze_df)

        bronze_df.select.assert_called_once()

    def test_deduplication_calls_latest_per_key(self):
        """build_silver_order_items must call _latest_per_key on order_item_id."""
        from dataone.batch import bronze_to_silver

        bronze_df = _chainable_df()

        with patch.object(bronze_to_silver, "_latest_per_key", return_value=bronze_df) as mock_dedup, \
             patch("dataone.batch.bronze_to_silver.F") as mock_F:
            mock_F.col.return_value = MagicMock()
            bronze_to_silver.build_silver_order_items(bronze_df)

        mock_dedup.assert_called_once()
        _, kwargs = mock_dedup.call_args
        assert kwargs.get("key_col") == "order_item_id"
        assert kwargs.get("order_col") == "ingested_at"

    def test_filters_null_keys(self):
        """build_silver_order_items must apply at least two filter calls
        (order_item_id and order_id)."""
        from dataone.batch.bronze_to_silver import build_silver_order_items

        bronze_df = _chainable_df()

        with patch("dataone.batch.bronze_to_silver._latest_per_key", return_value=bronze_df), \
             patch("dataone.batch.bronze_to_silver.F") as mock_F:
            mock_F.col.return_value = MagicMock()
            build_silver_order_items(bronze_df)

        assert bronze_df.filter.call_count >= 2


# ---------------------------------------------------------------------------
# Tests for ingest_order_items_to_bronze (watermark logic)
# ---------------------------------------------------------------------------

class TestIngestOrderItemsToBronze:
    """Tests for the watermark logic in ingest_order_items_to_bronze()."""

    def test_skips_when_no_new_rows(self):
        """When postgres MAX(id) <= bronze watermark, no write should be attempted."""
        from dataone.batch import bronze_to_silver

        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True

        watermark_df = _chainable_df()
        watermark_df.collect.return_value = [[500]]
        mock_spark.read.format.return_value.load.return_value = watermark_df

        with patch.object(bronze_to_silver, "_postgres_max_id", return_value=500), \
             patch("dataone.batch.bronze_to_silver.F") as mock_F, \
             patch.object(bronze_to_silver, "table_identifier", return_value="test.table"):
            mock_F.max.return_value = MagicMock()
            count = bronze_to_silver.ingest_order_items_to_bronze(mock_spark)

        assert count == 0
        watermark_df.writeTo.assert_not_called()

    def test_applies_watermark_filter_after_jdbc(self):
        """When new rows exist (upper > watermark), the function must call
        read_postgres_table and apply a filter guard."""
        from dataone.batch import bronze_to_silver

        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True

        watermark_df = _chainable_df()
        watermark_df.collect.return_value = [[100]]
        mock_spark.read.format.return_value.load.return_value = watermark_df

        jdbc_df = _chainable_df()
        jdbc_df.count.return_value = 100

        with patch.object(bronze_to_silver, "_postgres_max_id", return_value=200), \
             patch.object(bronze_to_silver, "read_postgres_table", return_value=jdbc_df), \
             patch("dataone.batch.bronze_to_silver.F") as mock_F, \
             patch.object(bronze_to_silver, "table_identifier", return_value="test.table"):
            mock_col = MagicMock()
            mock_col.__gt__ = MagicMock(return_value=MagicMock())  # col > watermark returns a Column mock
            mock_F.max.return_value = MagicMock()
            mock_F.col.return_value = mock_col
            mock_F.current_timestamp.return_value = MagicMock()
            count = bronze_to_silver.ingest_order_items_to_bronze(mock_spark)

        # Guard filter must have been applied
        jdbc_df.filter.assert_called()
        # writeTo should have been called to append
        jdbc_df.writeTo.assert_called()


# ---------------------------------------------------------------------------
# Tests for ingest_products_to_bronze (snapshot overwrite logic)
# ---------------------------------------------------------------------------

class TestIngestProductsToBronze:
    """Tests for ingest_products_to_bronze()."""

    def test_uses_create_or_replace(self):
        """Products must be written with createOrReplace() (atomic overwrite) not append."""
        from dataone.batch import bronze_to_silver

        mock_spark = MagicMock()
        products_df = _chainable_df()
        products_df.count.return_value = 50

        with patch.object(bronze_to_silver, "read_postgres_table", return_value=products_df), \
             patch("dataone.batch.bronze_to_silver.F") as mock_F, \
             patch.object(bronze_to_silver, "table_identifier", return_value="test.table"):
            mock_F.current_timestamp.return_value = MagicMock()
            count = bronze_to_silver.ingest_products_to_bronze(mock_spark)

        # Verify createOrReplace was used (not append)
        products_df._write_mock.createOrReplace.assert_called_once()
        assert count == 50

    def test_no_write_when_empty(self):
        """When Postgres returns 0 rows, writeTo should not be called."""
        from dataone.batch import bronze_to_silver

        mock_spark = MagicMock()
        products_df = _chainable_df()
        products_df.count.return_value = 0

        with patch.object(bronze_to_silver, "read_postgres_table", return_value=products_df), \
             patch("dataone.batch.bronze_to_silver.F") as mock_F, \
             patch.object(bronze_to_silver, "table_identifier", return_value="test.table"):
            mock_F.current_timestamp.return_value = MagicMock()
            count = bronze_to_silver.ingest_products_to_bronze(mock_spark)

        products_df.writeTo.assert_not_called()
        assert count == 0
