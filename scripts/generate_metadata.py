"""
Bootstrap script to generate JSON metadata files from existing hardcoded schemas in schemas.py.
Creates the folders:
  - metadata/datasets/
  - metadata/ownership/
  - metadata/contracts/
  - metadata/quality/
  - metadata/lineage/
"""
from __future__ import annotations

import os
import json
import sys

# Add src to python path to import schemas
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dataone.utils.schemas import ALL_TABLES

METADATA_DIRS = ["datasets", "ownership", "contracts", "quality", "lineage"]

def get_domain(table_name: str) -> str:
    if any(k in table_name for k in ["order", "sales", "roas", "date"]):
        return "orders"
    if any(k in table_name for k in ["customer", "clv"]):
        return "customers"
    if any(k in table_name for k in ["product", "sentiment"]):
        return "products"
    if any(k in table_name for k in ["campaign", "effectiveness"]):
        return "marketing"
    if "review" in table_name:
        return "reviews"
    if any(k in table_name for k in ["click", "conversion", "funnel"]):
        return "web"
    return "platform"

def get_owner(domain: str) -> str:
    owners = {
        "orders": "Orders Platform Team",
        "customers": "Customer Experience Team",
        "products": "Catalog Management Team",
        "marketing": "Marketing Ops Team",
        "reviews": "Customer Support Team",
        "web": "Web Analytics Team",
        "platform": "Data Engineering Team"
    }
    return owners.get(domain, "Data Engineering Team")

def get_classification(layer: str, table_name: str) -> str:
    if layer == "quarantine":
        return "restricted"
    if any(k in table_name for k in ["customer", "address", "email", "order"]):
        return "confidential"
    return "internal"

def get_primary_keys(table_name: str) -> list[str]:
    if table_name == "dim_customer":
        return ["sk_customer_id"]
    if table_name == "dim_product":
        return ["sk_product_id"]
    if table_name == "dim_campaign":
        return ["sk_campaign_id"]
    if table_name == "dim_date":
        return ["date_key"]
    if table_name == "fact_order_items":
        return ["sk_order_id"]
    
    keys = ["order_id", "customer_id", "product_id", "campaign_id", "review_id", "event_id", "session_id"]
    for k in keys:
        if table_name.startswith(k.split("_")[0]):
            return [k]
    return []

def get_required_columns(name: str) -> list[str]:
    # Taken from bronze_to_silver.py quality gate requirements
    reqs = {
        "bronze.campaigns": ["campaign_id", "name", "start_date", "end_date"],
        "silver.customers": ["customer_id"],
        "silver.orders": ["order_id", "customer_id", "order_date"],
        "gold.fact_order_items": ["order_id", "customer_id", "product_id"],
        "silver.reviews": ["review_id", "product_id", "rating"],
        "silver.clickstream": ["session_id", "event_type", "ts"]
    }
    # For others, default to primary keys if any, otherwise first column
    return reqs.get(name, [])

def get_column_bounds(name: str) -> dict[str, list]:
    # Taken from bronze_to_silver.py bounds validations
    bounds = {
        "bronze.campaigns": {
            "budget": [0, None],
            "spend": [0, None],
            "clicks": [0, None],
            "conversions": [0, None]
        },
        "gold.fact_order_items": {
            "unit_price": [0, None],
            "quantity": [1, None]
        },
        "silver.reviews": {
            "rating": [1, 5]
        }
    }
    return bounds.get(name, {})

def get_lineage(name: str) -> tuple[list[str], list[str]]:
    # Dynamic lineage maps based on data flow
    upstreams = []
    downstreams = []
    
    if name == "silver.orders":
        upstreams = ["bronze.orders_cdc"]
        downstreams = ["gold.fact_order_items"]
    elif name == "silver.customers":
        upstreams = ["bronze.orders_cdc"]
        downstreams = ["gold.dim_customer"]
    elif name == "silver.clickstream":
        upstreams = ["bronze.clickstream"]
        downstreams = ["gold.conversion_rate", "gold.funnel_conversion"]
    elif name == "silver.reviews":
        upstreams = ["bronze.reviews"]
        downstreams = ["gold.product_sentiment"]
    elif name == "gold.dim_customer":
        upstreams = ["silver.customers"]
        downstreams = ["gold.fact_order_items", "gold.customer_clv"]
    elif name == "gold.fact_order_items":
        upstreams = ["silver.orders", "gold.dim_customer", "postgres.order_items", "postgres.products"]
        downstreams = ["gold.daily_sales", "gold.top_products", "gold.customer_segments", "gold.customer_clv"]
    elif name == "gold.dim_product":
        upstreams = ["postgres.products"]
    elif name == "gold.dim_campaign":
        upstreams = ["bronze.campaigns"]
    elif name == "gold.daily_sales":
        upstreams = ["gold.fact_order_items"]
    elif name == "gold.top_products":
        upstreams = ["gold.fact_order_items", "postgres.products"]
    elif name == "gold.customer_segments":
        upstreams = ["gold.fact_order_items", "gold.dim_customer"]
    elif name == "gold.conversion_rate":
        upstreams = ["silver.clickstream"]
    elif name == "gold.campaign_effectiveness":
        upstreams = ["bronze.campaigns"]
    elif name == "gold.product_sentiment":
        upstreams = ["silver.reviews", "postgres.products"]
    elif name == "gold.customer_clv":
        upstreams = ["gold.fact_order_items", "gold.dim_customer"]
    elif name == "gold.funnel_conversion":
        upstreams = ["silver.clickstream"]
    elif name == "gold.roas":
        upstreams = ["gold.fact_order_items", "bronze.campaigns"]
    
    # Generic mapping fallback
    if name.startswith("quarantine."):
        tbl = name.split(".")[1]
        if tbl == "fact_order_items":
            upstreams = ["gold.fact_order_items"]
        elif tbl in ["campaigns", "reviews", "clickstream"]:
            upstreams = [f"bronze.{tbl}"]
        else:
            upstreams = [f"silver.{tbl}"]
            
    return upstreams, downstreams

def main():
    base_dir = os.path.join(os.path.dirname(__file__), "..", "metadata")
    for d in METADATA_DIRS:
        os.makedirs(os.path.join(base_dir, d), exist_ok=True)
        
    print(f"Bootstrap-generating metadata for {len(ALL_TABLES)} datasets...")
    
    for t in ALL_TABLES:
        layer = t["layer"]
        table_name = t["table"]
        name = f"{layer}.{table_name}"
        domain = get_domain(table_name)
        owner = get_owner(domain)
        classification = get_classification(layer, table_name)
        
        # 1. Dataset metadata
        dataset_meta = {
            "name": name,
            "description": f"{layer.capitalize()} dataset for {table_name}",
            "domain": domain,
            "layer": layer,
            "data_classification": classification
        }
        
        # 2. Ownership metadata
        ownership_meta = {
            "dataset": name,
            "domain": domain,
            "owner": owner,
            "steward": "Data Platform Team",
            "retention_policy": "7 years" if layer in ["silver", "gold"] else "1 year",
            "sla": "99.9% fresh within 2 hours of ingestion" if layer in ["bronze", "silver"] else "Daily by 6 AM UTC",
            "refresh_frequency": "continuous" if name in ["bronze.orders_cdc", "bronze.clickstream", "bronze.dead_letters", "silver.clickstream"] else "daily",
            "consumers": []
        }
        
        # 3. Contract metadata
        primary_keys = get_primary_keys(table_name)
        columns = []
        for col_name, col_type in t["columns"]:
            nullable = col_name not in primary_keys
            columns.append({
                "name": col_name,
                "type": col_type,
                "nullable": nullable
            })
            
        partition_by = t.get("partition_by")
        if name == "bronze.reviews":
            partition_by = ["days(ingested_at)"]
        elif name == "silver.reviews":
            partition_by = ["days(submitted_at)"]

        contract_meta = {
            "dataset": name,
            "schema_version": "1.0.0",
            "allowed_schema_evolution": "backward",
            "primary_keys": primary_keys,
            "business_keys": primary_keys,
            "columns": columns,
            "partition_by": partition_by,
            "sort_order": [primary_keys[0]] if primary_keys else None
        }
        
        # 4. Quality metadata
        quality_meta = {
            "dataset": name,
            "required_columns": get_required_columns(name),
            "column_bounds": get_column_bounds(name),
            "custom_rules": []
        }
        
        # 5. Lineage metadata
        up, down = get_lineage(name)
        lineage_meta = {
            "dataset": name,
            "upstream": up,
            "downstream": down
        }
        
        # Populate consumers from downstream dependencies
        ownership_meta["consumers"] = down
        
        # Save files
        for d, meta in zip(METADATA_DIRS, [dataset_meta, ownership_meta, contract_meta, quality_meta, lineage_meta]):
            filepath = os.path.join(base_dir, d, f"{name}.json")
            with open(filepath, "w") as f:
                json.dump(meta, f, indent=2)
                
    print("Metadata generation completed successfully!")

if __name__ == "__main__":
    main()
