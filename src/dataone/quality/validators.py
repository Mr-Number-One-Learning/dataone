"""
Data quality gate applied at the bronze -> silver boundary: null checks,
range validation, row-count reconciliation, and a quarantine path. Failing
rows are tagged with a reason and routed to quarantine — never dropped,
never null-inserted.

TESTABILITY NOTE: the functions below operate on real PySpark DataFrames.
Unlike psycopg2/pymongo/kafka-python (simple enough to fake convincingly for
offline testing — see tests/ for those), there's no practical way to fake
enough of PySpark's DataFrame/Column API in this project's dev sandbox to
exercise real filter/withColumn semantics. This is written carefully against
PySpark's stable, documented API (df.filter, F.col, F.when/.otherwise — none
of it exotic), but it has only been syntax-checked, never executed. First
real test is on an actual Spark session.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

QUARANTINE_REASON_COLUMN = "_quarantine_reason"


@dataclass
class QualityResult:
    passed_df: DataFrame
    quarantined_df: DataFrame
    passed_count: int
    quarantined_count: int


def check_nulls(required_columns: list[str]) -> Column:
    """Generates a boolean column expression to identify null values in required columns.

    Boolean column expression: True where the row violates a required-column-not-null 
    rule (i.e. SHOULD be quarantined).

    Args:
        required_columns (list[str]): A list of column names that must not be null.

    Returns:
        Column: A PySpark Column containing the boolean expression.
    """
    condition = F.lit(False)
    for column in required_columns:
        condition = condition | F.col(column).isNull()
    return condition


def check_ranges(column_bounds: dict[str, tuple[float | None, float | None]]) -> Column:
    """Generates a boolean column expression to identify out-of-range values.

    Boolean column expression: True where the row violates a range rule.
    Each bound is (low, high); either side can be None for "unbounded on
    that side", e.g. {"unit_price": (0, None), "quantity": (1, None)}.

    Args:
        column_bounds (dict[str, tuple[float | None, float | None]]): A dictionary 
            mapping column names to (low, high) bounds.

    Returns:
        Column: A PySpark Column containing the boolean expression.
    """
    condition = F.lit(False)
    for column, (low, high) in column_bounds.items():
        col = F.col(column)
        out_of_range = F.lit(False)
        if low is not None:
            out_of_range = out_of_range | (col < low)
        if high is not None:
            out_of_range = out_of_range | (col > high)
        condition = condition | out_of_range
    return condition


def reconcile_row_counts(source_count: int, landed_count: int) -> bool:
    """Checks if the source row count matches the landed row count.

    Exact match expected for this pipeline; any mismatch is logged loudly
    rather than silently tolerated.

    Args:
        source_count (int): The number of rows in the source.
        landed_count (int): The number of rows successfully written/landed.

    Returns:
        bool: True if counts match exactly, False otherwise.
    """
    if source_count != landed_count:
        log.warning("row_count_mismatch", source=source_count, landed=landed_count)
        return False
    return True


def run_quality_gate(
    df: DataFrame,
    required_columns: list[str],
    column_bounds: dict[str, tuple[float | None, float | None]] | None = None,
) -> QualityResult:
    """Applies quality rules and splits rows into passed and quarantined dataframes.

    Splits df into (passed, quarantined) based on null + range checks.
    Quarantined rows are tagged with a human-readable reason in
    QUARANTINE_REASON_COLUMN instead of being dropped — the caller is
    expected to write quarantined_df to a `quarantine.*` Iceberg table (see
    iceberg_helpers.table_identifier("quarantine", ...)), never to /dev/null.
    A row failing both checks gets both reasons ("null_check_failed,
    range_check_failed") so quarantine triage doesn't lose information.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        required_columns (list[str]): A list of column names that must not be null.
        column_bounds (dict[str, tuple[float | None, float | None]] | None, optional): 
            A dictionary mapping column names to (low, high) bounds. Defaults to None.

    Returns:
        QualityResult: A dataclass containing the passed dataframe, quarantined dataframe,
            and their respective row counts.

    Raises:
        ValueError: If quality rules reference columns absent from the DataFrame.
    """
    column_bounds = column_bounds or {}

    # Fail fast with a clear message instead of letting Spark raise an
    # opaque AnalysisException mid-plan when a rule references a column the
    # DataFrame doesn't have (usually a typo in the caller's rule set).
    referenced = set(required_columns) | set(column_bounds)
    missing = sorted(referenced - set(df.columns))
    if missing:
        raise ValueError(f"quality rules reference columns absent from DataFrame: {missing}")

    null_fail = check_nulls(required_columns)
    range_fail = check_ranges(column_bounds) if column_bounds else F.lit(False)
    fails_any = null_fail | range_fail

    reason = F.concat_ws(
        ",",
        F.when(null_fail, F.lit("null_check_failed")),
        F.when(range_fail, F.lit("range_check_failed")),
    )
    reason = F.when(fails_any, reason).otherwise(F.lit(None))

    # Cache before the two .count() actions + downstream writes so the full
    # upstream lineage (JDBC reads, joins) isn't recomputed three times.
    # Caller-visible contract unchanged; Spark evicts the cache under memory
    # pressure, so this is safe at any volume.
    tagged = df.withColumn(QUARANTINE_REASON_COLUMN, reason).cache()
    quarantined_df = tagged.filter(fails_any)
    passed_df = tagged.filter(~fails_any).drop(QUARANTINE_REASON_COLUMN)

    passed_count = passed_df.count()
    quarantined_count = quarantined_df.count()
    if quarantined_count:
        log.warning("quality_gate.quarantined", count=quarantined_count)
    log.info("quality_gate.done", passed=passed_count, quarantined=quarantined_count)

    return QualityResult(
        passed_df=passed_df,
        quarantined_df=quarantined_df,
        passed_count=passed_count,
        quarantined_count=quarantined_count,
    )
