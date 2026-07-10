"""
Ops utility: show the most recent entries in the streaming dead-letter table
(bronze.dead_letters), where the structured-streaming job routes Kafka
messages that failed JSON/schema parsing.
"""
from __future__ import annotations

import argparse

from dataone.utils.iceberg_helpers import table_identifier
from dataone.utils.spark_session import build_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect the streaming dead-letter queue.")
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="How many of the most recent dead letters to show (default: 5).",
    )
    args = parser.parse_args()

    spark = build_spark_session("query_dlq")
    try:
        ident = table_identifier("bronze", "dead_letters")
        spark.sql(
            f"SELECT * FROM {ident} ORDER BY failed_at DESC LIMIT {int(args.limit)}"
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
