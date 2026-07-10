"""
Maintains the customer dimension (gold.dim_customer) as Slowly Changing
Dimension Type 2: valid_from / valid_to / is_current columns track
historical changes to segment/address/etc, implemented via Iceberg MERGE INTO.

Testability note: this needs a real Iceberg catalog to test (the
`iceberg_spark` fixture in tests/conftest.py), not just a plain SparkSession,
since MERGE INTO is an Iceberg-Spark-extension feature.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from dataone.utils.iceberg_helpers import table_identifier, make_surrogate_key
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

# Columns that, if changed, trigger a new SCD2 version. customer_id itself
# is the merge key, not a "tracked" column.
SCD_TRACKED_COLUMNS = ("full_name", "email", "segment", "address")


def _format_timestamp_literal(ts: datetime) -> str:
    """Formats a datetime object as a Spark timestamp literal.

    Spark's TIMESTAMP literal parser wants 'yyyy-MM-dd HH:mm:ss[.ffffff]' —
    embedding a timezone-suffixed isoformat() string (e.g. '...+00:00')
    risks ambiguous parsing across Spark versions, so normalize to UTC and
    strip the offset before formatting.

    Args:
        ts (datetime): The datetime to format.

    Returns:
        str: The formatted timestamp string.
    """
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def apply_scd2_merge(spark: SparkSession, incoming_df: DataFrame, as_of: datetime | None = None) -> None:
    """Merges an incoming dataframe into the customer dimension (gold.dim_customer) as SCD Type 2.

    Contract: `incoming_df` must already be deduplicated to ONE row per
    customer_id representing the latest known state as of this run (the
    batch job's transform step is responsible for that — e.g. via
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY captured_at DESC) = 1
    on the parsed bronze CDC events). This function does not do that
    deduplication itself.

    For each incoming customer_id:
      - no existing current row -> insert as a new current version
      - existing current row, no tracked column changed -> no-op
      - existing current row, a tracked column changed -> close out the old
        version (valid_to = as_of, is_current = false) and insert a new
        current version (valid_from = as_of, valid_to = NULL, is_current = true)

    Implemented as two statements (a MERGE to close out changed rows, then
    an INSERT for anything new-or-just-closed) rather than one MERGE,
    because a single MERGE can't both UPDATE a matched row's "old version"
    AND INSERT an unrelated "new version" row for that same logical entity
    in one pass.

    Args:
        spark (SparkSession): The active Spark session.
        incoming_df (DataFrame): The deduplicated incoming customer dataframe.
        as_of (datetime | None, optional): The effective timestamp for the merge. 
            Defaults to the current UTC time.

    Raises:
        RuntimeError: If the incoming dataframe violates the deduplication contract 
            (multiple current rows per customer).
    """
    as_of = as_of or datetime.now(timezone.utc)
    as_of_sql = _format_timestamp_literal(as_of)
    target = table_identifier("gold", "dim_customer")

    # Unique view name per invocation so two merges sharing one SparkSession
    # (e.g. a backfill loop) can't clobber each other's source view.
    incoming_view = f"_scd2_incoming_{uuid.uuid4().hex}"
    incoming_df.createOrReplaceTempView(incoming_view)

    changed_predicate = " OR ".join(
        f"target.{c} IS DISTINCT FROM source.{c}" for c in SCD_TRACKED_COLUMNS
    )

    has_captured_at = "captured_at" in incoming_df.columns
    has_updated_at = "updated_at" in incoming_df.columns
    if has_updated_at:
        valid_time_expr = "CAST(source.updated_at AS TIMESTAMP)"
    else:
        valid_time_expr = "source.captured_at" if has_captured_at else f"TIMESTAMP '{as_of_sql}'"

    # Step 1: close out current rows whose tracked columns changed.
    spark.sql(
        f"""
        MERGE INTO {target} AS target
        USING {incoming_view} AS source
        ON target.customer_id = source.customer_id AND target.is_current = true
        WHEN MATCHED AND ({changed_predicate}) THEN UPDATE SET
            target.valid_to = {valid_time_expr},
            target.is_current = false
        """
    )

    # Step 2: insert a fresh current version for anything new or just closed.
    # This NOT EXISTS also self-heals a crash between step 1 and step 2 of a
    # PREVIOUS run: a customer left with zero current rows gets a new current
    # version the next time it appears in incoming.
    select_tracked = ", ".join(f"source.{c}" for c in SCD_TRACKED_COLUMNS)
    spark.sql(
        f"""
        INSERT INTO {target}
        SELECT
            MD5(CONCAT_WS('||', 'postgres', CAST(source.customer_id AS STRING), CAST({valid_time_expr} AS STRING))) AS sk_customer_id, source.customer_id, {select_tracked},
            {valid_time_expr} AS valid_from,
            CAST(NULL AS TIMESTAMP) AS valid_to,
            true AS is_current
        FROM {incoming_view} AS source
        WHERE NOT EXISTS (
            SELECT 1 FROM {target} AS target
            WHERE target.customer_id = source.customer_id AND target.is_current = true
        )
        """
    )

    # Invariant check: exactly one current row per customer. A violation
    # means the incoming dedup contract was broken (duplicate customer_id in
    # one batch) — fail the run loudly rather than let a corrupted dimension
    # flow into the marts.
    dupes = spark.sql(
        f"""
        SELECT customer_id FROM {target}
        WHERE is_current = true
        GROUP BY customer_id HAVING COUNT(*) > 1
        """
    ).limit(5)
    dupe_rows = dupes.collect()
    if dupe_rows:
        sample = [r.customer_id for r in dupe_rows]
        raise RuntimeError(
            f"SCD2 invariant violated: multiple is_current rows for customer_ids {sample} "
            "(incoming batch was not deduplicated to one row per customer_id)"
        )

    spark.catalog.dropTempView(incoming_view)
    log.info("scd2_merge.done", as_of=as_of.isoformat())
