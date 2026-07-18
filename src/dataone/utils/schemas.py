"""
Single source of truth for the Iceberg table schemas, loaded dynamically from the JSON Metadata Layer.
Exists so that both the streaming and batch jobs share identical configurations without code duplication.
"""
from __future__ import annotations

from dataone.metadata.registry import get_registry

registry = get_registry()

def _get_table_dict(dataset_name: str) -> dict:
    contract = registry.get_contract(dataset_name)
    dataset = registry.get_dataset(dataset_name)
    layer, table = dataset_name.split(".")
    
    return {
        "layer": layer,
        "table": table,
        "columns": [(col["name"], col["type"]) for col in contract["columns"]],
        "partition_by": contract.get("partition_by")
    }

# Dynamic schemas for backward compatibility
BRONZE_ORDERS_CDC  = _get_table_dict("bronze.orders_cdc")
BRONZE_CLICKSTREAM = _get_table_dict("bronze.clickstream")
BRONZE_DEAD_LETTERS = _get_table_dict("bronze.dead_letters")
BRONZE_CAMPAIGNS   = _get_table_dict("bronze.campaigns")
BRONZE_REVIEWS     = _get_table_dict("bronze.reviews")
BRONZE_PRODUCTS    = _get_table_dict("bronze.products")
BRONZE_ORDER_ITEMS = _get_table_dict("bronze.order_items")

SILVER_REVIEWS     = _get_table_dict("silver.reviews")
SILVER_CLICKSTREAM = _get_table_dict("silver.clickstream")
SILVER_CUSTOMERS   = _get_table_dict("silver.customers")
SILVER_ORDERS      = _get_table_dict("silver.orders")
SILVER_PRODUCTS    = _get_table_dict("silver.products")
SILVER_ORDER_ITEMS = _get_table_dict("silver.order_items")
SILVER_CAMPAIGNS   = _get_table_dict("silver.campaigns")

GOLD_DAILY_SALES = _get_table_dict("gold.daily_sales")
GOLD_TOP_PRODUCTS = _get_table_dict("gold.top_products")
GOLD_CUSTOMER_SEGMENTS = _get_table_dict("gold.customer_segments")
GOLD_CONVERSION_RATE = _get_table_dict("gold.conversion_rate")
GOLD_CAMPAIGN_EFFECTIVENESS = _get_table_dict("gold.campaign_effectiveness")
GOLD_PRODUCT_SENTIMENT = _get_table_dict("gold.product_sentiment")
GOLD_DIM_CUSTOMER = _get_table_dict("gold.dim_customer")
GOLD_FACT_ORDER_ITEMS = _get_table_dict("gold.fact_order_items")
GOLD_DIM_DATE = _get_table_dict("gold.dim_date")
GOLD_DIM_PRODUCT = _get_table_dict("gold.dim_product")
GOLD_DIM_CAMPAIGN = _get_table_dict("gold.dim_campaign")
GOLD_CUSTOMER_CLV = _get_table_dict("gold.customer_clv")
GOLD_FUNNEL_CONVERSION = _get_table_dict("gold.funnel_conversion")
GOLD_ROAS = _get_table_dict("gold.roas")
GOLD_QUARANTINE_SUMMARY = _get_table_dict("gold.quarantine_summary")
GOLD_QUALITY_GATE_SUMMARY = _get_table_dict("gold.quality_gate_summary")

QUARANTINE_FACT_ORDER_ITEMS = _get_table_dict("quarantine.fact_order_items")
QUARANTINE_CAMPAIGNS   = _get_table_dict("quarantine.campaigns")
QUARANTINE_REVIEWS     = _get_table_dict("quarantine.reviews")
QUARANTINE_ORDERS      = _get_table_dict("quarantine.orders")
QUARANTINE_CUSTOMERS   = _get_table_dict("quarantine.customers")
QUARANTINE_PRODUCTS    = _get_table_dict("quarantine.products")
QUARANTINE_CLICKSTREAM = _get_table_dict("quarantine.clickstream")
QUARANTINE_ORDER_ITEMS = _get_table_dict("quarantine.order_items")

ALL_TABLES = [
    # Bronze — raw ingest layer
    BRONZE_ORDERS_CDC,
    BRONZE_CLICKSTREAM,
    BRONZE_CAMPAIGNS,
    BRONZE_REVIEWS,
    BRONZE_DEAD_LETTERS,
    BRONZE_PRODUCTS,
    BRONZE_ORDER_ITEMS,
    # Silver — conformed / quality-gated layer
    SILVER_REVIEWS,
    SILVER_CLICKSTREAM,
    SILVER_CUSTOMERS,
    SILVER_ORDERS,
    SILVER_PRODUCTS,
    SILVER_ORDER_ITEMS,
    SILVER_CAMPAIGNS,
    # Gold — analytical marts
    GOLD_DIM_CUSTOMER,
    GOLD_FACT_ORDER_ITEMS,
    GOLD_DAILY_SALES,
    GOLD_TOP_PRODUCTS,
    GOLD_CUSTOMER_SEGMENTS,
    GOLD_CONVERSION_RATE,
    GOLD_CAMPAIGN_EFFECTIVENESS,
    GOLD_PRODUCT_SENTIMENT,
    GOLD_DIM_DATE,
    GOLD_DIM_PRODUCT,
    GOLD_DIM_CAMPAIGN,
    GOLD_CUSTOMER_CLV,
    GOLD_FUNNEL_CONVERSION,
    GOLD_ROAS,
    GOLD_QUARANTINE_SUMMARY,
    GOLD_QUALITY_GATE_SUMMARY,
    # Quarantine
    QUARANTINE_FACT_ORDER_ITEMS,
    QUARANTINE_CAMPAIGNS,
    QUARANTINE_REVIEWS,
    QUARANTINE_ORDERS,
    QUARANTINE_CUSTOMERS,
    QUARANTINE_PRODUCTS,
    QUARANTINE_CLICKSTREAM,
    QUARANTINE_ORDER_ITEMS,
]


def create_all_tables_sql() -> list[str]:
    """Generates CREATE TABLE SQL statements for all defined schemas.

    One CREATE TABLE IF NOT EXISTS statement per table.

    Returns:
        list[str]: A list of generated SQL statements.
    """
    from dataone.utils.iceberg_helpers import create_table_sql

    return [
        create_table_sql(t["layer"], t["table"], t["columns"], t["partition_by"])
        for t in ALL_TABLES
    ]
