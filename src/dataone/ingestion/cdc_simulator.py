"""
Log-based-CDC-lite: polls Postgres customers/orders on the updated_at watermark
and emits change events to the 'orders-cdc' Kafka topic. Stands in for Debezium,
which is not in the allowed bootcamp toolset.

Watermarks are persisted in Postgres itself (a small `_cdc_watermarks`
bookkeeping table this module creates on first run), not held in memory only
— a restart resumes from where it left off instead of re-emitting everything
or silently skipping the gap.

Known limitation, accepted for this project's scope: if multiple rows share
the exact same `updated_at` timestamp and a poll's LIMIT cuts the batch off
in the middle of that group, the watermark still advances to that timestamp
and any sibling rows with the identical value that fell outside the LIMIT
won't be re-fetched on the next poll. A production CDC tool avoids this with
a compound (timestamp, primary key) cursor; not worth the complexity here.

Run: python -m dataone.ingestion.cdc_simulator
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from dataone.config import kafka, postgres
from dataone.ingestion.kafka_producers import flush_producer, send
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

POLL_INTERVAL_SECONDS = 5
BATCH_LIMIT = 5_000

# Insert-vs-update heuristic window: rows whose created_at and updated_at are
# within this many seconds of each other are treated as fresh inserts. The
# slack (rather than strict equality) absorbs trigger/clock jitter between
# the two column defaults on the same row.
INSERT_DETECTION_WINDOW_SECONDS = 1

# table_name -> primary key column. Both already have an updated_at index
# (see infra/docker/postgres/init/001_schema.sql) for this poll to use.
WATCHED_TABLES = {
    "customers": "customer_id",
    "orders": "order_id",
}

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def ensure_watermark_table(conn) -> None:
    """Creates the watermark bookkeeping table if it doesn't exist.

    Args:
        conn: The psycopg2 database connection.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS _cdc_watermarks (
                table_name TEXT PRIMARY KEY,
                last_watermark TIMESTAMPTZ NOT NULL
            )
            """
        )
    conn.commit()


def get_watermark(conn, table: str) -> datetime:
    """Retrieves the last known watermark for a table.

    Args:
        conn: The psycopg2 database connection.
        table (str): The name of the table.

    Returns:
        datetime: The last seen updated_at timestamp, or EPOCH if none exists.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT last_watermark FROM _cdc_watermarks WHERE table_name = %s", (table,))
        row = cur.fetchone()
    return row[0] if row else EPOCH


def set_watermark(conn, table: str, new_watermark: datetime) -> None:
    """Persists a new watermark for a table.

    Args:
        conn: The psycopg2 database connection.
        table (str): The name of the table.
        new_watermark (datetime): The new updated_at timestamp to store.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO _cdc_watermarks (table_name, last_watermark)
            VALUES (%s, %s)
            ON CONFLICT (table_name) DO UPDATE SET last_watermark = EXCLUDED.last_watermark
            """,
            (table, new_watermark),
        )
    conn.commit()


def fetch_changed_rows(
    conn, table: str, since_watermark: datetime, limit: int = BATCH_LIMIT
) -> list[dict]:
    """Fetches recently changed rows from a table.

    Executes: SELECT * FROM {table} WHERE updated_at > since_watermark ORDER BY updated_at LIMIT limit.
    `table` is only ever one of WATCHED_TABLES' fixed keys, never user input.

    Args:
        conn: The psycopg2 database connection.
        table (str): The name of the table.
        since_watermark (datetime): The watermark timestamp to filter by.
        limit (int, optional): The maximum number of rows to fetch. Defaults to BATCH_LIMIT.

    Returns:
        list[dict]: A list of rows, each represented as a dictionary.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT * FROM {table} WHERE updated_at > %s ORDER BY updated_at LIMIT %s",  # noqa: S608
            (since_watermark, limit),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def _serialize_row(row: dict) -> dict:
    """Serializes a database row to a JSON-compatible dictionary.

    JSON can't carry datetime objects directly — convert to ISO strings.

    Args:
        row (dict): The row to serialize.

    Returns:
        dict: The serialized row.
    """
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


def emit_change_event(table: str, pk_column: str, row: dict) -> None:
    """Emits a single change event to Kafka.

    Args:
        table (str): The source table name.
        pk_column (str): The primary key column name.
        row (dict): The row payload to emit.
    """
    # Distinguish inserts from updates by comparing created_at to updated_at.
    # If they are close enough, this is the row's first appearance.
    created = row.get("created_at")
    updated = row.get("updated_at")
    if created and updated:
        delta = abs((updated - created).total_seconds())
        op = "insert" if delta < INSERT_DETECTION_WINDOW_SECONDS else "update"
    else:
        op = "update"  # fallback

    event = {
        "table": table,
        "op": op,
        "pk_column": pk_column,
        # Pre-stringified, not a nested object: customers and orders have
        # entirely different row shapes, so keeping the OUTER Kafka envelope
        # flat and uniform (table, op, pk_column, data: STRING, captured_at)
        # lets any downstream consumer parse it with one fixed schema,
        # instead of needing per-source-table schemas at the Kafka layer.
        # The streaming job stores this string as-is into bronze.orders_cdc's
        # data_json column — see batch/bronze_to_silver.py's parse_*_from_cdc.
        "data": json.dumps(_serialize_row(row)),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }
    send(kafka.topic_orders_cdc, event, key=str(row[pk_column]))


def poll_once(conn, table: str, pk_column: str) -> int:
    """Polls a table for changes and emits events.

    One poll cycle for one table: fetch rows changed since the stored
    watermark, emit each as a change event, then advance the watermark to the
    max updated_at actually seen — only after every row in the batch has been
    emitted, so a crash mid-batch re-emits on retry (at-least-once) rather
    than silently losing the tail of the batch.

    Args:
        conn: The psycopg2 database connection.
        table (str): The name of the table to poll.
        pk_column (str): The primary key column of the table.

    Returns:
        int: The number of rows processed in this poll cycle.
    """
    watermark = get_watermark(conn, table)
    rows = fetch_changed_rows(conn, table, watermark)
    if not rows:
        return 0

    for row in rows:
        emit_change_event(table, pk_column, row)
    # One flush per poll batch (not per message): the watermark only advances
    # after the whole batch is confirmed handed to Kafka, preserving the
    # at-least-once guarantee described above without per-send flush cost.
    flush_producer()

    new_watermark = max(row["updated_at"] for row in rows)
    set_watermark(conn, table, new_watermark)
    log.info(
        "cdc_simulator.poll",
        table=table,
        emitted=len(rows),
        new_watermark=new_watermark.isoformat(),
    )
    return len(rows)


def _reconnect(conn):
    """Re-establishes a broken database connection.

    Close a (possibly dead) connection and open a fresh one. Without this,
    a dropped connection would make every subsequent poll retry against the
    same dead socket forever — tenacity retries the call, not the resource.

    Args:
        conn: The broken psycopg2 database connection.

    Returns:
        The new psycopg2 database connection.
    """
    try:
        conn.close()
    except Exception:  # already-broken connections can raise on close too
        pass
    new_conn = psycopg2.connect(postgres.dsn)
    log.info("cdc_simulator.reconnected")
    return new_conn


def run() -> None:
    """Main daemon loop for the CDC simulator.

    Connects to Postgres, initializes watermarks, and continuously polls
    watched tables for changes, emitting them to Kafka.
    """
    conn = psycopg2.connect(postgres.dsn)
    ensure_watermark_table(conn)
    log.info(
        "cdc_simulator.start",
        poll_interval=POLL_INTERVAL_SECONDS,
        watched_tables=list(WATCHED_TABLES),
    )
    try:
        while True:
            for table, pk_column in WATCHED_TABLES.items():
                try:
                    poll_once(conn, table, pk_column)
                except (psycopg2.OperationalError, psycopg2.InterfaceError):
                    # Connection-level failure: the session itself is gone, so
                    # re-establish it before the next poll instead of retrying
                    # the same dead connection every cycle.
                    log.exception("cdc_simulator.connection_lost", table=table)
                    conn = _reconnect(conn)
                except Exception:
                    # poll_once already retries transient psycopg2 errors;
                    # if it still raised, log (with traceback) and move on to
                    # the next table / next cycle rather than killing the
                    # whole long-running daemon over one bad poll.
                    log.exception("cdc_simulator.poll_failed", table=table)
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
