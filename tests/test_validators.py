"""
Unit tests for the data-quality gate.

reconcile_row_counts is pure Python — runs in the fast default test loop.
check_nulls/check_ranges/run_quality_gate operate on real PySpark Columns/
DataFrames and are marked @pytest.mark.spark, using the local SparkSession
fixture in conftest.py. These need Java installed wherever you run them
(this project's sandbox has neither Java nor a way to fake PySpark's
DataFrame semantics convincingly — written carefully against the real,
documented API, but never executed here; first real run is wherever you
actually have Spark available).
"""
import pytest

from dataone.quality.validators import reconcile_row_counts


def test_reconcile_row_counts_match():
    assert reconcile_row_counts(100, 100) is True


def test_reconcile_row_counts_mismatch():
    assert reconcile_row_counts(100, 95) is False


@pytest.mark.spark
def test_check_nulls_flags_rows_with_any_required_column_null(spark):
    from dataone.quality.validators import check_nulls

    df = spark.createDataFrame(
        [(1, "a", 10), (2, None, 20), (3, "c", None), (4, "d", 40)],
        ["id", "name", "amount"],
    )
    flagged_ids = {row.id for row in df.filter(check_nulls(["name", "amount"])).collect()}
    assert flagged_ids == {2, 3}


@pytest.mark.spark
def test_check_nulls_empty_required_columns_flags_nothing(spark):
    from dataone.quality.validators import check_nulls

    df = spark.createDataFrame([(1, "a"), (2, None)], ["id", "name"])
    flagged_ids = {row.id for row in df.filter(check_nulls([])).collect()}
    assert flagged_ids == set()


@pytest.mark.spark
def test_check_ranges_flags_out_of_bound_rows(spark):
    from dataone.quality.validators import check_ranges

    df = spark.createDataFrame(
        [(1, 10.0, 2), (2, -5.0, 2), (3, 10.0, 0), (4, 10.0, 100), (5, 10.0, 2)],
        ["id", "unit_price", "quantity"],
    )
    cond = check_ranges({"unit_price": (0, None), "quantity": (1, 4)})
    flagged_ids = {row.id for row in df.filter(cond).collect()}
    assert flagged_ids == {2, 3, 4}  # 1 and 5 are within bounds on both columns


@pytest.mark.spark
def test_run_quality_gate_quarantines_without_dropping_anything(spark):
    from dataone.quality.validators import run_quality_gate

    df = spark.createDataFrame(
        [(1, "ok", 10.0), (2, None, 10.0), (3, "ok", -5.0), (4, "ok", 10.0)],
        ["id", "name", "unit_price"],
    )
    result = run_quality_gate(df, required_columns=["name"], column_bounds={"unit_price": (0, None)})

    assert result.passed_count == 2
    assert result.quarantined_count == 2
    assert result.passed_count + result.quarantined_count == df.count(), "no row should ever be dropped"

    reasons = {row.id: row._quarantine_reason for row in result.quarantined_df.collect()}
    assert reasons[2] == "null_check_failed"
    assert reasons[3] == "range_check_failed"

    # passed_df must NOT carry the internal bookkeeping column forward
    assert "_quarantine_reason" not in result.passed_df.columns


@pytest.mark.spark
def test_run_quality_gate_with_no_bounds_only_checks_nulls(spark):
    from dataone.quality.validators import run_quality_gate

    df = spark.createDataFrame([(1, "ok"), (2, None)], ["id", "name"])
    result = run_quality_gate(df, required_columns=["name"])

    assert result.passed_count == 1
    assert result.quarantined_count == 1
