import psycopg2
from prefect import flow, task

from dataone.config import postgres
from dataone.ingestion.cdc_simulator import ensure_watermark_table, poll_once

TABLES = [("customers", "customer_id"), ("orders", "order_id")]

@task(retries=3, retry_delay_seconds=10, retry_jitter_factor=0.3)
def poll_table(table: str, pk_column: str) -> int:
    conn = psycopg2.connect(postgres.dsn)
    try:
        return poll_once(conn, table, pk_column)
    finally:
        conn.close()

@task
def init_watermarks() -> None:
    conn = psycopg2.connect(postgres.dsn)
    try:
        ensure_watermark_table(conn)
    finally:
        conn.close()

@flow(name="cdc-poll")
def cdc_poll() -> None:
    init_watermarks()
    for table, pk in TABLES:
        poll_table(table, pk)

if __name__ == "__main__":
    cdc_poll.serve(name="cdc-poll", interval=5)
