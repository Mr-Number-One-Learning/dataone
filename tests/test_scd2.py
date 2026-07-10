"""
Tests for the SCD2 customer dimension merge.

_format_timestamp_literal is plain Python and runs in the fast default test
loop. Everything else needs a REAL Iceberg catalog (MERGE INTO is an
Iceberg-Spark-extension feature, not something a plain SparkSession
supports) — uses the `iceberg_spark` fixture (tests/conftest.py), which
needs Java + network on first run (fetches the Iceberg jar from Maven).
`make test-iceberg` to run these; excluded from the default `make test`.
Written carefully against the real API, never executed in this project's dev
sandbox (no Java, no network for the jar fetch) — first real run is wherever
you actually have both.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dataone.batch.scd2_customer_dim import _format_timestamp_literal

CUSTOMER_COLUMNS = ["customer_id", "full_name", "email", "segment", "address"]


def test_format_timestamp_literal_strips_timezone_suffix():
    ts = datetime(2026, 6, 27, 14, 30, 0, 123456, tzinfo=timezone.utc)
    rendered = _format_timestamp_literal(ts)
    assert rendered == "2026-06-27 14:30:00.123456"
    assert "+" not in rendered


def test_format_timestamp_literal_normalizes_to_utc():
    ts = datetime(2026, 6, 27, 9, 30, 0, tzinfo=timezone(timedelta(hours=-5)))
    assert _format_timestamp_literal(ts) == "2026-06-27 14:30:00.000000"


def _reset_customer_dim_table(spark):
    """Fresh, empty gold.dim_customer before each Iceberg-backed test —
    the iceberg_spark fixture is session-scoped, so without this, tests
    would see each other's rows."""
    from dataone.utils.iceberg_helpers import create_table_sql
    from dataone.utils.schemas import GOLD_DIM_CUSTOMER

    spark.sql("CREATE NAMESPACE IF NOT EXISTS dataone_catalog.gold")
    spark.sql("DROP TABLE IF EXISTS dataone_catalog.gold.dim_customer")
    spark.sql(
        create_table_sql(
            GOLD_DIM_CUSTOMER["layer"],
            GOLD_DIM_CUSTOMER["table"],
            GOLD_DIM_CUSTOMER["columns"],
            GOLD_DIM_CUSTOMER["partition_by"],
        )
    )


@pytest.mark.iceberg
def test_new_customer_inserts_as_current(iceberg_spark):
    from dataone.batch.scd2_customer_dim import apply_scd2_merge

    _reset_customer_dim_table(iceberg_spark)
    incoming = iceberg_spark.createDataFrame(
        [(1, "Ada Lovelace", "ada@example.com", "standard", "123 Main St")], CUSTOMER_COLUMNS
    )
    apply_scd2_merge(iceberg_spark, incoming, as_of=datetime(2026, 1, 1, tzinfo=timezone.utc))

    rows = iceberg_spark.sql("SELECT * FROM dataone_catalog.gold.dim_customer").collect()
    assert len(rows) == 1
    assert rows[0].customer_id == 1
    assert rows[0].is_current is True
    assert rows[0].valid_to is None


@pytest.mark.iceberg
def test_unchanged_customer_is_noop(iceberg_spark):
    from dataone.batch.scd2_customer_dim import apply_scd2_merge

    _reset_customer_dim_table(iceberg_spark)
    row_data = [(1, "Ada Lovelace", "ada@example.com", "standard", "123 Main St")]

    apply_scd2_merge(
        iceberg_spark,
        iceberg_spark.createDataFrame(row_data, CUSTOMER_COLUMNS),
        as_of=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    apply_scd2_merge(
        iceberg_spark,
        iceberg_spark.createDataFrame(row_data, CUSTOMER_COLUMNS),
        as_of=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    rows = iceberg_spark.sql("SELECT * FROM dataone_catalog.gold.dim_customer").collect()
    assert len(rows) == 1, "no new version should be created when nothing changed"
    assert rows[0].is_current is True


@pytest.mark.iceberg
def test_changed_segment_creates_new_version_and_closes_old(iceberg_spark):
    from dataone.batch.scd2_customer_dim import apply_scd2_merge

    _reset_customer_dim_table(iceberg_spark)
    t1, t2 = datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc)

    apply_scd2_merge(
        iceberg_spark,
        iceberg_spark.createDataFrame(
            [(1, "Ada Lovelace", "ada@example.com", "standard", "123 Main St")], CUSTOMER_COLUMNS
        ),
        as_of=t1,
    )
    apply_scd2_merge(
        iceberg_spark,
        iceberg_spark.createDataFrame(
            [(1, "Ada Lovelace", "ada@example.com", "vip", "123 Main St")], CUSTOMER_COLUMNS
        ),
        as_of=t2,
    )

    rows = iceberg_spark.sql(
        "SELECT * FROM dataone_catalog.gold.dim_customer ORDER BY valid_from"
    ).collect()
    assert len(rows) == 2, "should have closed the old version and inserted a new one"

    old, new = rows
    assert old.segment == "standard" and old.is_current is False and old.valid_to is not None
    assert new.segment == "vip" and new.is_current is True and new.valid_to is None


@pytest.mark.iceberg
def test_new_and_unchanged_customers_in_same_batch(iceberg_spark):
    """A single incoming batch can contain both a brand-new customer and an
    unchanged existing one — both share step 2's NOT EXISTS predicate, this
    confirms neither path breaks the other."""
    from dataone.batch.scd2_customer_dim import apply_scd2_merge

    _reset_customer_dim_table(iceberg_spark)
    t1, t2 = datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 2, tzinfo=timezone.utc)

    apply_scd2_merge(
        iceberg_spark,
        iceberg_spark.createDataFrame(
            [(1, "Ada Lovelace", "ada@example.com", "standard", "123 Main St")], CUSTOMER_COLUMNS
        ),
        as_of=t1,
    )
    apply_scd2_merge(
        iceberg_spark,
        iceberg_spark.createDataFrame(
            [
                (1, "Ada Lovelace", "ada@example.com", "standard", "123 Main St"),  # unchanged
                (2, "Grace Hopper", "grace@example.com", "premium", "456 Oak Ave"),  # brand new
            ],
            CUSTOMER_COLUMNS,
        ),
        as_of=t2,
    )

    rows = {r.customer_id: r for r in iceberg_spark.sql(
        "SELECT * FROM dataone_catalog.gold.dim_customer"
    ).collect()}
    assert len(rows) == 2
    assert rows[1].is_current is True  # unchanged customer, still one row, no duplicate version
    assert rows[2].is_current is True  # new customer inserted
