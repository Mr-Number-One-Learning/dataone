"""Tests for the shared bronze/silver/gold/quarantine schema definitions."""
from __future__ import annotations

from dataone.utils import schemas


def test_create_all_tables_sql_covers_every_table():
    ddls = schemas.create_all_tables_sql()
    assert len(ddls) == len(schemas.ALL_TABLES)
    for ddl in ddls:
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS dataone_catalog.")
        assert "USING iceberg" in ddl


def test_quarantine_schema_is_fact_order_items_plus_reason_column():
    fact_cols = schemas.GOLD_FACT_ORDER_ITEMS["columns"]
    quarantine_cols = schemas.QUARANTINE_FACT_ORDER_ITEMS["columns"]
    assert quarantine_cols[:-1] == fact_cols
    assert quarantine_cols[-1] == ("_quarantine_reason", "STRING")


def test_no_duplicate_layer_table_pairs():
    pairs = [(t["layer"], t["table"]) for t in schemas.ALL_TABLES]
    assert len(pairs) == len(set(pairs)), "duplicate (layer, table) entries in ALL_TABLES"


def test_every_table_has_at_least_one_column():
    for t in schemas.ALL_TABLES:
        assert len(t["columns"]) > 0, f"{t['layer']}.{t['table']} has no columns"
