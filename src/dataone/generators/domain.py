"""
Shared product-catalog domain data used by orders_generator (to create the
catalog) and referenced read-only by clickstream/reviews generators so every
generator agrees on the same categories/price ranges without duplicating them.
"""
from __future__ import annotations

import psycopg2

from dataone.config import postgres
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

# Number of campaigns campaign_generator emits. Deliberately shared here:
# orders_generator attributes ~30% of orders to campaign IDs drawn from
# 1..N_CAMPAIGNS, so the two generators MUST agree or order attribution
# would point at campaign IDs that don't exist in the campaign files.
N_CAMPAIGNS = 50

CATEGORY_PRODUCTS = {
    "Electronics": ["Wireless Headphones", "Bluetooth Speaker", "Smartwatch", "Laptop Stand",
                    "USB-C Hub", "Action Camera", "Gaming Mouse", "Portable Charger"],
    "Apparel": ["Cotton T-Shirt", "Denim Jacket", "Running Shoes", "Wool Sweater",
                "Yoga Pants", "Baseball Cap", "Leather Belt", "Summer Dress"],
    "Home & Garden": ["Ceramic Vase", "Throw Pillow", "Garden Hose", "LED Desk Lamp",
                      "Wall Clock", "Storage Basket", "Scented Candle", "Patio Chair"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Tennis Racket", "Cycling Helmet",
               "Resistance Bands", "Soccer Ball", "Hiking Backpack", "Water Bottle"],
    "Beauty": ["Facial Serum", "Moisturizing Cream", "Lip Balm Set", "Hair Dryer",
               "Makeup Brush Set", "Shampoo Bar", "Perfume", "Nail Polish Set"],
    "Toys": ["Building Blocks Set", "Remote Control Car", "1000pc Puzzle", "Plush Bear",
             "Board Game", "Action Figure", "Art Supply Kit", "Toy Train Set"],
    "Books": ["Mystery Novel", "Cookbook", "Self-Help Guide", "Sci-Fi Anthology",
              "Picture Book", "History Atlas", "Poetry Collection", "Travel Guide"],
    "Grocery": ["Organic Coffee Beans", "Pasta Variety Pack", "Olive Oil", "Granola Bars",
                "Herbal Tea Set", "Dark Chocolate Bars", "Spice Set", "Almond Butter"],
}

CATEGORY_CODES = {
    "Electronics": "ELC", "Apparel": "APP", "Home & Garden": "HOM", "Sports": "SPO",
    "Beauty": "BEA", "Toys": "TOY", "Books": "BOK", "Grocery": "GRO",
}

CATEGORY_PRICE_RANGE = {
    "Electronics": (15, 500), "Apparel": (10, 150), "Home & Garden": (8, 200),
    "Sports": (10, 250), "Beauty": (5, 80), "Toys": (5, 100),
    "Books": (8, 40), "Grocery": (3, 50),
}

CATEGORIES = list(CATEGORY_PRODUCTS.keys())


def category_for_product_id(product_id: int) -> str:
    """Assigns a deterministic category for a given product ID.

    Deterministic category assignment so any generator can derive a
    product's category from its ID alone, without a DB round-trip.

    Args:
        product_id (int): The product ID to assign a category to.

    Returns:
        str: The assigned category name.
    """
    return CATEGORIES[product_id % len(CATEGORIES)]


def fetch_max_id(column: str, table: str, fallback: int) -> int:
    """Fetches the maximum ID from a database table.

    Looks up the current max ID actually seeded in Postgres, so generator
    product/customer references stay valid against whatever orders_generator
    produced — rather than each generator duplicating sizing constants.
    Falls back (with a warning) so generators can still run standalone for a
    quick demo before Postgres is seeded/reachable.

    Args:
        column (str): The column to find the maximum value of.
        table (str): The table to query.
        fallback (int): The fallback value if the query fails or returns nothing.

    Returns:
        int: The maximum ID, or the fallback value.
    """
    try:
        conn = psycopg2.connect(postgres.dsn)
        try:
            with conn.cursor() as cur:
                # Fixed identifiers from callers, never user input.
                cur.execute(f"SELECT MAX({column}) FROM {table}")
                (max_id,) = cur.fetchone()
            return max_id or fallback
        finally:
            conn.close()
    except Exception:  # pragma: no cover - best-effort fallback
        log.warning("max_id_lookup_failed", table=table, fallback=fallback, exc_info=True)
        return fallback
