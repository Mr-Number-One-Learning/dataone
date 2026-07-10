"""
Cross-checks infra/docker/clickhouse/init/002_marts.sql's column names
against utils/schemas.py's GOLD_* definitions. Spark's JDBC writer inserts
by column NAME, not position — if these ever drift apart (someone renames a
column in schemas.py but forgets the SQL file, or vice versa), the batch
job's ClickHouse load would fail at runtime. This test catches that at
review time instead.

Pure text parsing, no ClickHouse instance needed.
"""
from __future__ import annotations

import re
from pathlib import Path

from dataone.utils import schemas

DDL_PATH = Path(__file__).parent.parent / "infra" / "docker" / "clickhouse" / "init" / "002_marts.sql"

GOLD_SCHEMAS_BY_TABLE = {
    "daily_sales": schemas.GOLD_DAILY_SALES,
    "top_products": schemas.GOLD_TOP_PRODUCTS,
    "customer_segments": schemas.GOLD_CUSTOMER_SEGMENTS,
    "conversion_rate": schemas.GOLD_CONVERSION_RATE,
    "campaign_effectiveness": schemas.GOLD_CAMPAIGN_EFFECTIVENESS,
    "fact_order_items": schemas.GOLD_FACT_ORDER_ITEMS,
}


def _split_top_level(text: str) -> list[str]:
    """Split on commas, but not commas inside type parens like Nullable(Float64)."""
    parts, depth, current = [], 0, []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _extract_table_columns(sql: str) -> dict[str, list[str]]:
    """table_name -> [column_name, ...] for every CREATE TABLE in the file."""
    no_comments = re.sub(r"--.*", "", sql)
    tables = {}
    for match in re.finditer(
        r"CREATE TABLE IF NOT EXISTS dataone_marts\.(\w+)\s*\((.*?)\)\s*ENGINE",
        no_comments,
        re.DOTALL,
    ):
        table_name, columns_block = match.group(1), match.group(2)
        column_names = []
        for part in _split_top_level(columns_block):
            part = part.strip()
            if part:
                column_names.append(part.split()[0])
        tables[table_name] = column_names
    return tables


def test_ddl_file_exists():
    assert DDL_PATH.exists(), f"expected {DDL_PATH} to exist"


def test_every_gold_table_has_matching_clickhouse_ddl():
    tables = _extract_table_columns(DDL_PATH.read_text())
    for table_name, schema in GOLD_SCHEMAS_BY_TABLE.items():
        assert table_name in tables, f"no CREATE TABLE found for dataone_marts.{table_name}"
        ddl_columns = tables[table_name]
        schema_columns = [name for name, _ in schema["columns"]]
        assert ddl_columns == schema_columns, (
            f"{table_name}: DDL columns {ddl_columns} != schemas.py columns {schema_columns}"
        )


def test_live_activity_table_exists_with_expected_columns():
    """live_activity has no Iceberg/schemas.py counterpart (it's ClickHouse-only,
    written by the streaming job's foreachBatch sink) — checked directly
    against what structured_streaming_job.py's _write_batch_to_clickhouse
    actually selects."""
    tables = _extract_table_columns(DDL_PATH.read_text())
    assert tables["live_activity"] == [
        "window_start",
        "window_end",
        "event_count",
        "active_sessions",
        "checkout_completions",
    ]
