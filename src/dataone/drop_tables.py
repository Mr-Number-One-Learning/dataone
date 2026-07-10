"""
Destructive dev utility: drop the gold marts so the next batch run rebuilds
them from scratch. Guarded behind an explicit --yes flag because DROP TABLE
on the lakehouse is irreversible.
"""
from __future__ import annotations

import argparse

from dataone.utils.iceberg_helpers import table_identifier
from dataone.utils.logging_config import get_logger
from dataone.utils.spark_session import build_spark_session

log = get_logger(__name__)

GOLD_TABLES = ("daily_sales", "conversion_rate", "fact_order_items")


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop the gold mart tables (irreversible).")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required confirmation flag — without it, nothing is dropped.",
    )
    args = parser.parse_args()
    if not args.yes:
        parser.error("refusing to drop gold tables without --yes")

    spark = build_spark_session("drop_tables")
    try:
        for table in GOLD_TABLES:
            ident = table_identifier("gold", table)
            spark.sql(f"DROP TABLE IF EXISTS {ident}")
            log.info("table_dropped", table=ident)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
