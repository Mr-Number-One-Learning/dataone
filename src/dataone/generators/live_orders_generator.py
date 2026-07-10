"""
Live orders generator: runs continuously as a daemon, inserting new orders and
updating existing order statuses to simulate real-time production traffic.
Unlike orders_generator.py which is a one-time bulk seeder, this script keeps
running, feeding the CDC simulator.

Run: python -m dataone.generators.live_orders_generator
"""
from __future__ import annotations

import os
import random
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from dataone.config import postgres
from dataone.generators.domain import fetch_max_id
from dataone.generators.orders_generator import (
    MAX_ITEMS_PER_ORDER,
    MIN_ITEMS_PER_ORDER,
    _zipf_customer_assignment,
)
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

ORDERS_PER_MINUTE = int(os.getenv("GEN_LIVE_ORDERS_PER_MINUTE", "20"))
UPDATE_BATCH_SIZE = int(os.getenv("GEN_LIVE_UPDATE_BATCH_SIZE", "10"))
SEED = int(os.getenv("GEN_LIVE_SEED", "99"))
MESSINESS_RATE = float(os.getenv("GEN_MESSINESS_RATE", "0.01"))


def _connect():
    """Establishes a connection to the Postgres database.

    Returns:
        A psycopg2 connection object.
    """
    return psycopg2.connect(postgres.dsn)


def generate_live_orders(
    conn, max_customer_id: int, product_prices: dict[int, float]
) -> None:
    """Inserts a single new order into Postgres.

    Args:
        conn: The psycopg2 database connection.
        max_customer_id (int): The maximum customer ID available to associate with orders.
        product_prices (dict[int, float]): A mapping of {product_id: current_unit_price}.
    """
    if max_customer_id == 0 or not product_prices:
        log.warning("live_generator.skip_insert", reason="No customers or products seeded")
        return

    # Reuse the skew distribution logic to pick one customer
    customer_id = _zipf_customer_assignment(1, max_customer_id)[0]
    product_ids = list(product_prices.keys())

    # Ensure campaigns are somewhat populated, otherwise None
    campaign_id = random.randint(1, 50) if random.random() < 0.3 else None

    # Noise
    final_customer_id = customer_id
    if random.random() < MESSINESS_RATE:
        if random.random() < 0.5:
            final_customer_id = None

    now = datetime.now(timezone.utc).isoformat()
    status = "placed"

    try:
        with conn.cursor() as cur:
            # Insert the order and get its ID
            cur.execute(
                """
                INSERT INTO orders (customer_id, order_date, status, created_at, updated_at, campaign_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING order_id
                """,
                (final_customer_id, now, status, now, now, campaign_id),
            )
            (order_id,) = cur.fetchone()

            # Insert 1-5 order items
            num_items = random.randint(MIN_ITEMS_PER_ORDER, MAX_ITEMS_PER_ORDER)
            for _ in range(num_items):
                product_id = random.choice(product_ids)
                unit_price = product_prices[product_id]
                quantity = random.randint(1, 4)

                final_product_id = product_id
                if random.random() < MESSINESS_RATE:
                    noise = random.choice(["negative_price", "negative_quantity", "null_product"])
                    if noise == "negative_price":
                        unit_price = -unit_price
                    elif noise == "negative_quantity":
                        quantity = -quantity
                    elif noise == "null_product":
                        final_product_id = None

                cur.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (order_id, final_product_id, quantity, unit_price),
                )
        conn.commit()
        log.debug("live_generator.order_inserted", order_id=order_id, status=status)
    except Exception as e:
        conn.rollback()
        log.error("live_generator.insert_failed", error=str(e))


def update_live_orders(conn) -> None:
    """Selects a small batch of recent orders and progresses their status.

    Args:
        conn: The psycopg2 database connection.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find recent 'placed' orders to ship
            cur.execute(
                """
                SELECT order_id FROM orders
                WHERE status = 'placed'
                ORDER BY RANDOM()
                LIMIT %s
                """,
                (UPDATE_BATCH_SIZE // 2,),
            )
            placed_orders = [row["order_id"] for row in cur.fetchall()]

            # Find recent 'shipped' orders to deliver
            cur.execute(
                """
                SELECT order_id FROM orders
                WHERE status = 'shipped'
                ORDER BY RANDOM()
                LIMIT %s
                """,
                (UPDATE_BATCH_SIZE // 2,),
            )
            shipped_orders = [row["order_id"] for row in cur.fetchall()]

            updated_count = 0
            for oid in placed_orders:
                new_status = "cancelled" if random.random() < 0.05 else "shipped"
                cur.execute(
                    "UPDATE orders SET status = %s WHERE order_id = %s",
                    (new_status, oid)
                )
                updated_count += 1

            for oid in shipped_orders:
                # Could add 'returned' simulation here later
                new_status = "returned" if random.random() < 0.05 else "delivered"
                cur.execute(
                    "UPDATE orders SET status = %s WHERE order_id = %s",
                    (new_status, oid)
                )
                updated_count += 1

        conn.commit()
        if updated_count > 0:
            log.info("live_generator.orders_updated", count=updated_count)
    except Exception as e:
        conn.rollback()
        log.error("live_generator.update_failed", error=str(e))


def main() -> None:
    """Main daemon loop for the live orders generator.

    Initializes seeding, establishes database connection, looks up existing data,
    and runs a continuous loop that periodically inserts new orders and updates
    existing order statuses.
    """
    random.seed(SEED)
    log.info(
        "live_generator.start",
        orders_per_minute=ORDERS_PER_MINUTE,
        update_batch_size=UPDATE_BATCH_SIZE,
        seed=SEED,
    )

    sleep_interval = 60.0 / ORDERS_PER_MINUTE if ORDERS_PER_MINUTE > 0 else 60.0

    conn = _connect()
    try:
        max_customer_id = fetch_max_id("customer_id", "customers", fallback=0)
        product_prices = {}

        if max_customer_id > 0:
            with conn.cursor() as cur:
                cur.execute("SELECT product_id, unit_price FROM products")
                for pid, price in cur.fetchall():
                    product_prices[pid] = float(price)

        log.info(
            "live_generator.ready",
            max_customer_id=max_customer_id,
            products=len(product_prices),
            sleep_interval=sleep_interval
        )

        update_cycle_counter = 0

        while True:
            # 1. Insert a new order
            if ORDERS_PER_MINUTE > 0:
                generate_live_orders(conn, max_customer_id, product_prices)

            # 2. Update statuses periodically (e.g. every 5 inserts)
            update_cycle_counter += 1
            if update_cycle_counter >= 5:
                update_live_orders(conn)
                update_cycle_counter = 0

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        log.info("live_generator.stopped_by_user")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
