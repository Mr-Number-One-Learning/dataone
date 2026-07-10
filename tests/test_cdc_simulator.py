"""
Tests for the CDC simulator's watermark and polling logic. Uses a small
self-contained fake Postgres connection (not a real DB) — only psycopg2's
import surface (psycopg2.Error, the cursor/connection protocol) needs to be
real, never an actual database.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from dataone.ingestion import cdc_simulator as cdc


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._rows: list[dict] = []

    def execute(self, query, params=None):
        result = self.conn.query_handler(query, params) if self.conn.query_handler else None
        self._rows = list(result) if result is not None else []

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class _FakeConn:
    def __init__(self, query_handler=None):
        self.query_handler = query_handler
        self.committed = False

    def cursor(self, cursor_factory=None):
        return _FakeCursor(self)

    def commit(self):
        self.committed = True


@pytest.fixture(autouse=True)
def _patch_send(monkeypatch):
    """cdc_simulator imported `send` by name, so patch it there, not on the
    kafka_producers module — that's where the call actually resolves."""
    sent = []
    monkeypatch.setattr(cdc, "send", lambda topic, value, key=None: sent.append((topic, value, key)))
    return sent


# ---------------------------------------------------------------------------
# _serialize_row / emit_change_event
# ---------------------------------------------------------------------------

def test_serialize_row_converts_datetime_to_iso():
    ts = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    out = cdc._serialize_row({"id": 1, "updated_at": ts, "name": "Ada"})
    assert out == {"id": 1, "updated_at": "2026-06-01T12:00:00+00:00", "name": "Ada"}


def test_emit_change_event_payload_shape(_patch_send):
    row = {"customer_id": 7, "updated_at": datetime(2026, 6, 1, tzinfo=timezone.utc)}
    cdc.emit_change_event("customers", "customer_id", row)

    assert len(_patch_send) == 1
    topic, value, key = _patch_send[0]
    assert value["table"] == "customers"
    assert value["pk_column"] == "customer_id"
    assert isinstance(value["data"], str), "data must be pre-stringified, not a nested object"
    assert json.loads(value["data"])["customer_id"] == 7
    assert key == "7"


# ---------------------------------------------------------------------------
# get_watermark
# ---------------------------------------------------------------------------

def test_get_watermark_falls_back_to_epoch_when_nothing_stored():
    conn = _FakeConn(query_handler=lambda q, p: [])
    assert cdc.get_watermark(conn, "customers") == cdc.EPOCH


def test_get_watermark_returns_stored_value():
    stored = (datetime(2026, 5, 1, tzinfo=timezone.utc),)
    conn = _FakeConn(query_handler=lambda q, p: [stored])
    assert cdc.get_watermark(conn, "customers") == stored[0]


# ---------------------------------------------------------------------------
# poll_once
# ---------------------------------------------------------------------------

def test_poll_once_emits_all_rows_and_advances_to_max_watermark(_patch_send):
    t0 = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = [
        {"customer_id": 1, "updated_at": t0 + timedelta(minutes=1)},
        {"customer_id": 2, "updated_at": t0 + timedelta(minutes=5)},  # max
        {"customer_id": 3, "updated_at": t0 + timedelta(minutes=3)},
    ]

    def handler(query, params):
        if "_cdc_watermarks" in query and query.strip().upper().startswith("SELECT"):
            return []
        if "FROM customers WHERE" in query:
            return rows
        return None

    conn = _FakeConn(query_handler=handler)
    emitted_count = cdc.poll_once(conn, "customers", "customer_id")

    assert emitted_count == 3
    assert len(_patch_send) == 3
    assert {json.loads(v["data"])["customer_id"] for _, v, _ in _patch_send} == {1, 2, 3}
    assert conn.committed, "set_watermark should have committed the advance"


def test_poll_once_with_no_changes_emits_nothing(_patch_send):
    def handler(query, params):
        return []  # both the watermark lookup and the changed-rows query return empty

    conn = _FakeConn(query_handler=handler)
    emitted_count = cdc.poll_once(conn, "orders", "order_id")

    assert emitted_count == 0
    assert _patch_send == []
    assert not conn.committed, "no rows -> watermark should not be touched"
