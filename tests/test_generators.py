"""
Tests for the synthetic data generators. Requires the real third-party deps
from requirements.txt (faker, psycopg2) to be installed — these are NOT
mocked, except for bulk_copy's Postgres connection, which is faked here
since bulk_copy itself only depends on the cursor/copy_expert interface,
not psycopg2 directly.
"""
from __future__ import annotations

import io
from collections import Counter

from dataone.generators.campaign_generator import build_campaign_row
from dataone.generators.clickstream_generator import build_event
from dataone.generators.orders_generator import _zipf_customer_assignment
from dataone.generators.reviews_generator import build_review
from dataone.utils.db_bulk import bulk_copy


# ---------------------------------------------------------------------------
# bulk_copy — pure batching logic, no real Postgres needed
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self):
        self.copy_calls: list[str] = []

    def copy_expert(self, sql: str, buf: io.StringIO) -> None:
        self.copy_calls.append(buf.getvalue())

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self):
        self.cursor_obj = _FakeCursor()

    def cursor(self):
        return self.cursor_obj


def test_bulk_copy_writes_all_rows_in_batches(monkeypatch):
    import psycopg2.extensions
    monkeypatch.setattr(psycopg2.extensions, "quote_ident", lambda s, scope=None: f'"{s}"')
    conn = _FakeConn()
    rows = [(i, f"row-{i}") for i in range(1, 251)]  # 250 rows, batch_size=100 -> 3 flushes
    written = bulk_copy(conn, "t", ["id", "name"], iter(rows), batch_size=100)
    assert written == 250
    assert len(conn.cursor_obj.copy_calls) == 3  # 100 + 100 + 50

    total_lines = sum(call.count("\n") for call in conn.cursor_obj.copy_calls)
    assert total_lines == 250


def test_bulk_copy_empty_input(monkeypatch):
    import psycopg2.extensions
    monkeypatch.setattr(psycopg2.extensions, "quote_ident", lambda s, scope=None: f'"{s}"')
    conn = _FakeConn()
    written = bulk_copy(conn, "t", ["id"], iter([]), batch_size=100)
    assert written == 0
    assert conn.cursor_obj.copy_calls == []


# ---------------------------------------------------------------------------
# orders_generator — customer skew
# ---------------------------------------------------------------------------

def test_zipf_customer_assignment_covers_all_orders():
    assignment = _zipf_customer_assignment(n_orders=5_000, max_customer_id=500)
    assert len(assignment) == 5_000
    assert all(1 <= cid <= 500 for cid in assignment)


def test_zipf_customer_assignment_is_skewed_but_not_absurd():
    """
    Guards against the bug caught during manual testing: a pure Zipf
    (exponent=1.0) gave one customer ~13% of ALL orders. The current
    exponent should give a visible skew (top customer above the mean)
    without any single customer dominating implausibly.
    """
    n_orders, n_customers = 10_000, 1_000
    assignment = _zipf_customer_assignment(n_orders, n_customers)
    counts = Counter(assignment)
    mean = n_orders / n_customers
    top_count = counts.most_common(1)[0][1]

    assert top_count > mean, "expected some repeat-customer skew"
    assert top_count < n_orders * 0.05, "no single customer should dominate >5% of all orders"


# ---------------------------------------------------------------------------
# clickstream_generator
# ---------------------------------------------------------------------------

def test_build_event_required_fields():
    event = build_event(max_customer_id=100, max_product_id=20, session_id="s1", logged_in=True)
    for field in ("event_id", "session_id", "event_type", "product_id", "ts", "customer_id"):
        assert field in event
    assert 1 <= event["product_id"] <= 20
    assert 1 <= event["customer_id"] <= 100


def test_build_event_anonymous_has_no_customer_id():
    event = build_event(max_customer_id=100, max_product_id=20, session_id="s1", logged_in=False)
    assert "customer_id" not in event


# ---------------------------------------------------------------------------
# reviews_generator — variable schema is the point, not a bug
# ---------------------------------------------------------------------------

def test_build_review_schema_genuinely_varies():
    from faker import Faker

    faker = Faker()
    shapes = {
        tuple(sorted(build_review(faker, max_product_id=20, max_customer_id=100).keys()))
        for _ in range(500)
    }
    assert len(shapes) > 1, "reviews should have intentionally variable shape, not one fixed schema"


def test_build_review_required_fields_always_present(monkeypatch):
    from faker import Faker
    import dataone.generators.reviews_generator as rg
    monkeypatch.setattr(rg, "GEN_MESSINESS_RATE", 0.0)

    faker = Faker()
    for _ in range(50):
        review = build_review(faker, max_product_id=20, max_customer_id=100)
        for field in ("review_id", "product_id", "rating", "title", "body", "submitted_at"):
            assert field in review
        assert 1 <= review["rating"] <= 5


# ---------------------------------------------------------------------------
# campaign_generator
# ---------------------------------------------------------------------------

def test_build_campaign_row_fields_and_invariants(monkeypatch):
    import dataone.generators.campaign_generator as cg
    monkeypatch.setattr(cg, "GEN_MESSINESS_RATE", 0.0)
    
    row = build_campaign_row(campaign_id=1)
    assert row["campaign_id"] == 1
    assert row["budget"] > 0
    assert row["spend"] > 0
    assert row["clicks"] > 0
    assert 0 <= row["conversions"] <= row["clicks"]
    assert row["start_date"] <= row["end_date"]
