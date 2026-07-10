"""
Shared helper for fast bulk-loading rows into Postgres via COPY ... FROM STDIN,
batched so memory stays bounded even at multi-million-row volumes — this is
what makes seeding GEN_ORDERS_TOTAL_ROWS=2000000 (the big-data bonus target)
practical on a laptop.
"""
from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Sequence

from psycopg2 import sql


def bulk_copy(
    conn,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence],
    batch_size: int = 50_000,
    commit_every_batch: bool = False,
) -> int:
    """Executes a batched COPY ... FROM STDIN into Postgres.

    COPY `rows` into `table` (only the given `columns`), batched so we never
    hold more than `batch_size` rows of CSV in memory at once.
    `rows` should be a generator, not a pre-built list, so the caller never
    materializes the full dataset in memory either.

    By default, caller is responsible for conn.commit() / conn.rollback().
    If `commit_every_batch=True`, this function commits the connection after
    each batch to bound transaction memory.

    Args:
        conn: The psycopg2 database connection.
        table (str): The target table name.
        columns (Sequence[str]): A sequence of column names to insert.
        rows (Iterable[Sequence]): An iterable yielding sequences of row values.
        batch_size (int, optional): The number of rows per batch. Defaults to 50,000.
        commit_every_batch (bool, optional): Whether to commit after each batch. Defaults to False.

    Returns:
        int: The total number of rows written.
    """
    # Compose identifiers with psycopg2.sql (same standard reset_sequence
    # already follows) so table/column names can never be treated as raw SQL.
    copy_sql = sql.SQL("COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)").format(
        table=sql.Identifier(*table.split(".")),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    ).as_string(conn)

    cur = conn.cursor()
    total = 0
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    batch_rows = 0

    def flush() -> None:
        nonlocal buffer, writer, batch_rows
        if batch_rows == 0:
            return
        buffer.seek(0)
        cur.copy_expert(copy_sql, buffer)
        if commit_every_batch:
            conn.commit()
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        batch_rows = 0

    try:
        for row in rows:
            writer.writerow(row)
            batch_rows += 1
            total += 1
            if batch_rows >= batch_size:
                flush()
        flush()
    finally:
        cur.close()

    return total


def reset_sequence(conn, table: str, id_column: str) -> None:
    """Resets the sequence for a table's primary key after a bulk load.

    After an explicit-ID bulk load (we assign IDs ourselves for COPY speed
    instead of letting SERIAL generate them), sync the table's sequence so
    the next normal INSERT doesn't collide with the IDs we just wrote.

    Args:
        conn: The psycopg2 database connection.
        table (str): The table name.
        id_column (str): The name of the auto-incrementing ID column.
    """
    query = sql.SQL(
        "SELECT setval(pg_get_serial_sequence({tbl}, {col}), "
        "COALESCE((SELECT MAX({col_ident}) FROM {table_ident}), 1))"
    ).format(
        tbl=sql.Literal(table),
        col=sql.Literal(id_column),
        col_ident=sql.Identifier(id_column),
        table_ident=sql.Identifier(table),
    )
    with conn.cursor() as cur:
        cur.execute(query)
