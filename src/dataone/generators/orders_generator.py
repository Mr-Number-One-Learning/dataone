"""
Synthetic OLTP data generator: seeds customers / products / orders / order_items
into Postgres. Sized via GEN_ORDERS_TOTAL_ROWS in .env to support the big-data
bonus criterion.

Run: python -m dataone.generators.orders_generator
"""
from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone

import psycopg2
from faker import Faker

from dataone.config import postgres
from dataone.generators.domain import (
    CATEGORY_CODES,
    CATEGORY_PRICE_RANGE,
    CATEGORY_PRODUCTS,
    N_CAMPAIGNS,
    category_for_product_id,
)
from dataone.utils.db_bulk import bulk_copy, reset_sequence
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

TOTAL_ORDERS = int(os.getenv("GEN_ORDERS_TOTAL_ROWS", "2000000"))
N_PRODUCTS = int(os.getenv("GEN_PRODUCTS_TOTAL", "2000"))
SEED = int(os.getenv("GEN_SEED", "42"))
GEN_MESSINESS_RATE = float(os.getenv("GEN_MESSINESS_RATE", "0.0"))

# A realistic e-commerce business has far fewer customers than orders (repeat
# purchases). Capped at 100k so Faker-driven generation stays fast even when
# TOTAL_ORDERS is in the millions.
N_CUSTOMERS = min(100_000, max(5_000, TOTAL_ORDERS // 20))

SEGMENTS = ["standard", "premium", "vip"]
SEGMENT_WEIGHTS = [0.70, 0.22, 0.08]

ORDER_STATUSES = ["delivered", "shipped", "placed", "cancelled", "returned"]
ORDER_STATUS_WEIGHTS = [0.62, 0.15, 0.08, 0.10, 0.05]

# Recency skew for order_date: small mean -> most orders recent, long tail
# reaching back up to MAX_ORDER_AGE_DAYS.
MEAN_ORDER_AGE_DAYS = 60
MAX_ORDER_AGE_DAYS = 365

# Items per order.
MIN_ITEMS_PER_ORDER = 1
MAX_ITEMS_PER_ORDER = 5


def _connect():
    """Establishes a connection to the Postgres database.

    Returns:
        A psycopg2 connection object.
    """
    return psycopg2.connect(postgres.dsn)


def _get_max_id(conn, column: str, table: str) -> int:
    """Retrieves the current maximum ID for a given table and column.

    Args:
        conn: The psycopg2 database connection.
        column (str): The name of the ID column.
        table (str): The name of the table.

    Returns:
        int: The maximum ID, or 0 if the table is empty.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX({column}) FROM {table}")
        (max_id,) = cur.fetchone()
    return max_id or 0


def _customer_row(cid: int, faker: Faker, now: datetime) -> tuple:
    """Generates a single synthetic customer row.

    Args:
        cid (int): The assigned customer ID.
        faker (Faker): The Faker instance for generating fake data.
        now (datetime): The current UTC datetime for bounding creation dates.

    Returns:
        tuple: A tuple containing (customer_row_tuple, created_at_datetime).
    """
    first, last = faker.first_name(), faker.last_name()
    # Deterministic-unique email (id suffix) instead of Faker's .unique, which
    # gets slow/unreliable at six-figure volumes.
    email = f"{first.lower()}.{last.lower()}{cid}@example.com"
    segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
    created_at = now - timedelta(days=random.randint(0, 730))
    address = faker.address().replace("\n", ", ")
    created = created_at.isoformat()
    return (cid, f"{first} {last}", email, segment, address, created, created), created_at


def _product_row(pid: int, faker: Faker, now: datetime) -> tuple[tuple, float]:
    """Generates a single synthetic product row.

    Args:
        pid (int): The assigned product ID.
        faker (Faker): The Faker instance for generating fake data.
        now (datetime): The current UTC datetime.

    Returns:
        tuple[tuple, float]: A tuple containing the (product_row, unit_price).
            The price is handed back so generate_orders can snapshot price-at-purchase
            consistently.
    """
    category = category_for_product_id(pid)
    noun = random.choice(CATEGORY_PRODUCTS[category])
    brand = faker.company().split(",")[0]
    name = f"{brand} {noun}"
    sku = f"{CATEGORY_CODES[category]}-{pid:06d}"
    low, high = CATEGORY_PRICE_RANGE[category]
    price = round(random.uniform(low, high), 2)
    created_at = now - timedelta(days=random.randint(0, 730))
    # Inject deliberate noise to test the Quarantine Layer
    if random.random() < GEN_MESSINESS_RATE:
        noise_type = random.choice(["null_name", "null_category"])
        if noise_type == "null_name":
            name = None
        elif noise_type == "null_category":
            category = None
            
    row = (pid, sku, name, category, price, created_at.isoformat(), created_at.isoformat())
    return row, price


def generate_customers(conn, n: int, start_id: int, faker: Faker) -> tuple[int, dict[int, datetime]]:
    """Generates and bulk-inserts synthetic customers into the database.

    Bulk-insert n synthetic customers via COPY. IDs assigned explicitly
    so order generation can reference them without a round-trip.

    Args:
        conn: The psycopg2 database connection.
        n (int): The number of customers to generate.
        start_id (int): The starting ID for the new customers.
        faker (Faker): The Faker instance for generating fake data.

    Returns:
        tuple[int, dict[int, datetime]]: A tuple containing the new maximum customer ID
            and a mapping of {customer_id: created_at} for generated customers.
    """
    now = datetime.now(timezone.utc)
    customer_created = {}

    def rows():
        for cid in range(start_id + 1, start_id + n + 1):
            row, created_at = _customer_row(cid, faker, now)
            customer_created[cid] = created_at
            yield row

    written = bulk_copy(
        conn,
        "customers",
        ["customer_id", "full_name", "email", "segment", "address", "created_at", "updated_at"],
        rows(),
    )
    reset_sequence(conn, "customers", "customer_id")
    log.info("generate_customers.done", rows=written)
    return start_id + n, customer_created


def generate_products(conn, n: int, start_id: int, faker: Faker) -> dict[int, float]:
    """Generates and bulk-inserts synthetic products into the database.

    Bulk-insert n synthetic products spread across the category catalog.

    Args:
        conn: The psycopg2 database connection.
        n (int): The number of products to generate.
        start_id (int): The starting ID for the new products.
        faker (Faker): The Faker instance for generating fake data.

    Returns:
        dict[int, float]: A dictionary mapping {product_id: unit_price} so 
            generate_orders can snapshot price-at-purchase consistently with 
            the catalog instead of re-sampling.
    """
    now = datetime.now(timezone.utc)
    prices: dict[int, float] = {}

    def rows():
        for pid in range(start_id + 1, start_id + n + 1):
            row, price = _product_row(pid, faker, now)
            prices[pid] = price
            yield row

    written = bulk_copy(
        conn,
        "products",
        ["product_id", "sku", "name", "category", "unit_price", "created_at", "updated_at"],
        rows(),
    )
    reset_sequence(conn, "products", "product_id")
    log.info("generate_products.done", rows=written)
    return prices


# Repeat-customer skew exponent for _zipf_customer_assignment. A pure Zipf
# (exponent=1.0) was tested and produced one customer holding ~13% of ALL
# orders — an obvious outlier, not a believable "VIP". 0.5 gives a visible
# but plausible power-user skew (~15-20x the mean for the top customer)
# while every customer still gets at least one order.
CUSTOMER_SKEW_EXPONENT = 0.5


def _zipf_customer_assignment(n_orders: int, max_customer_id: int) -> list[int]:
    """Assigns orders to customers using a Zipfian distribution.

    Skews order volume toward a minority of customers (repeat-buyer realism)
    rather than a uniform draw. Which specific customers become "power users"
    is randomized via the shuffle, not just "customer #1 always wins".

    Args:
        n_orders (int): The total number of orders to assign.
        max_customer_id (int): The maximum customer ID available.

    Returns:
        list[int]: A list of customer IDs representing the assigned customer for each order.
    """
    customer_ids = list(range(1, max_customer_id + 1))
    random.shuffle(customer_ids)
    weights = [1.0 / (rank ** CUSTOMER_SKEW_EXPONENT) for rank in range(1, max_customer_id + 1)]
    return random.choices(customer_ids, weights=weights, k=n_orders)


def generate_orders(
    conn,
    n_orders: int,
    max_customer_id: int,
    start_order_id: int,
    start_item_id: int,
    product_prices: dict[int, float],
    customer_created: dict[int, datetime],
) -> None:
    """Generates and bulk-inserts synthetic orders and order items into the database.

    Bulk-insert n_orders orders plus 1-5 order_items each, with price-at-
    purchase snapshotted from product_prices (small drift applied to mimic
    real-world price changes over time, not an exact catalog mirror).

    Args:
        conn: The psycopg2 database connection.
        n_orders (int): The number of orders to generate.
        max_customer_id (int): The maximum customer ID available to associate with orders.
        start_order_id (int): The starting ID for the new orders.
        start_item_id (int): The starting ID for the new order items.
        product_prices (dict[int, float]): A mapping of {product_id: current_unit_price}.
        customer_created (dict[int, datetime]): A mapping of {customer_id: created_at_datetime}.
    """
    now = datetime.now(timezone.utc)
    product_ids = list(product_prices.keys())
    order_customer_ids = _zipf_customer_assignment(n_orders, max_customer_id)

    def order_rows():
        for i in range(n_orders):
            oid = start_order_id + 1 + i
            cid = order_customer_ids[i]
            customer_created_at = customer_created[cid]
            
            # Ensure the order isn't older than the customer's account
            max_days_ago = max(0, (now - customer_created_at).days)
            days_ago = min(int(random.expovariate(1 / MEAN_ORDER_AGE_DAYS)), max_days_ago)
            
            order_date = now - timedelta(
                days=days_ago, hours=random.randint(0, 23), minutes=random.randint(0, 59)
            )
            if order_date < customer_created_at:
                order_date = customer_created_at + timedelta(minutes=random.randint(1, 120))
                
            # CRITICAL FIX: Ensure the artificially shifted order_date never exceeds now()
            # This prevents orders from getting timestamps in the future and breaking the CDC simulator's watermark.
            if order_date > now:
                order_date = now
                
            status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0]
            # Attribution draws from 1..N_CAMPAIGNS — the shared constant in
            # generators/domain.py — because campaign_generator emits exactly
            # that many campaigns; drifting apart would create orders pointing
            # at campaign IDs that don't exist.
            campaign_id = random.randint(1, N_CAMPAIGNS) if random.random() < 0.3 else None

            # Inject deliberate noise to test the Quarantine Layer
            final_cid = cid
            if random.random() < GEN_MESSINESS_RATE:
                noise_type = random.choice(["null_customer"])
                if noise_type == "null_customer":
                    final_cid = None

            yield (
                oid,
                final_cid,
                order_date.isoformat(),
                status,
                order_date.isoformat(),
                order_date.isoformat(),
                campaign_id,
            )

    written = bulk_copy(
        conn,
        "orders",
        ["order_id", "customer_id", "order_date", "status", "created_at", "updated_at",
         "campaign_id"],
        order_rows(),
        commit_every_batch=True,
    )
    reset_sequence(conn, "orders", "order_id")
    log.info("generate_orders.done", rows=written)

    def order_item_rows():
        item_id = start_item_id
        for i in range(n_orders):
            oid = start_order_id + 1 + i
            for _ in range(random.randint(MIN_ITEMS_PER_ORDER, MAX_ITEMS_PER_ORDER)):
                item_id += 1
                product_id = random.choice(product_ids)
                quantity = random.randint(1, 4)
                # Small drift vs. current catalog price -> price-at-purchase,
                # not a live join; real-world prices move over time.
                unit_price = round(product_prices[product_id] * random.uniform(0.9, 1.05), 2)
                
                # Inject deliberate noise to test the Quarantine Layer
                final_product_id = product_id
                if random.random() < GEN_MESSINESS_RATE:
                    noise_type = random.choice(["negative_price", "negative_quantity", "null_product"])
                    if noise_type == "negative_price":
                        unit_price = -unit_price
                    elif noise_type == "negative_quantity":
                        quantity = -quantity
                    elif noise_type == "null_product":
                        final_product_id = None
                        
                yield (item_id, oid, final_product_id, quantity, unit_price)

    written_items = bulk_copy(
        conn,
        "order_items",
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
        order_item_rows(),
        commit_every_batch=True,
    )
    reset_sequence(conn, "order_items", "order_item_id")
    log.info("generate_order_items.done", rows=written_items)


def main() -> None:
    """Main entry point for the orders generator.

    Initializes seeding, establishes database connection, figures out starting
    IDs from existing tables, and sequentially triggers the generation of customers,
    products, and orders.
    """
    random.seed(SEED)
    Faker.seed(SEED)
    faker = Faker()

    log.info(
        "orders_generator.start",
        total_orders=TOTAL_ORDERS,
        n_customers=N_CUSTOMERS,
        n_products=N_PRODUCTS,
        seed=SEED,
    )
    conn = _connect()
    try:
        max_customer_id = _get_max_id(conn, "customer_id", "customers")
        max_product_id = _get_max_id(conn, "product_id", "products")
        max_order_id = _get_max_id(conn, "order_id", "orders")
        max_item_id = _get_max_id(conn, "order_item_id", "order_items")
        
        product_prices = {}
        if max_product_id > 0:
            with conn.cursor() as cur:
                cur.execute("SELECT product_id, unit_price FROM products")
                for pid, price in cur.fetchall():
                    product_prices[pid] = float(price)
                    
        customer_created: dict[int, datetime] = {}
        if max_customer_id > 0:
            with conn.cursor() as cur:
                cur.execute("SELECT customer_id, created_at FROM customers")
                for cid, created_at in cur.fetchall():
                    customer_created[cid] = created_at

        new_max_customer_id, new_customer_created = generate_customers(conn, N_CUSTOMERS, max_customer_id, faker)
        customer_created.update(new_customer_created)
        
        new_product_prices = generate_products(conn, N_PRODUCTS, max_product_id, faker)
        product_prices.update(new_product_prices)
        # Commit customers + products BEFORE order generation begins: orders
        # commit per batch (commit_every_batch=True), so a failure mid-orders
        # must never leave committed order batches referencing customers or
        # products that get rolled back with the final commit.
        conn.commit()

        generate_orders(
            conn, TOTAL_ORDERS, new_max_customer_id, max_order_id, max_item_id, product_prices, customer_created
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info("orders_generator.done")


if __name__ == "__main__":
    main()
