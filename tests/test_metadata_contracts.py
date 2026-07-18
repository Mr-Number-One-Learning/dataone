"""
Unit tests for the Metadata Registry, Schema Contracts, and Lineage Tracker.
"""
from __future__ import annotations

import os
import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

from dataone.metadata.registry import get_registry
from dataone.metadata.contracts import validate_schema, DataContractViolation
from dataone.lineage.tracker import LineageTracker, Dataset


def test_metadata_registry_loads_contracts():
    """Tests that the metadata registry successfully reads contracts."""
    registry = get_registry()
    
    # Assert that dim_customer exists and has valid contract data
    contract = registry.get_contract("gold.dim_customer")
    assert contract is not None
    assert contract["dataset"] == "gold.dim_customer"
    assert len(contract["columns"]) > 0
    
    # Assert that get_spark_schema constructs a valid StructType
    schema = registry.get_spark_schema("gold.dim_customer")
    assert isinstance(schema, StructType)
    assert "sk_customer_id" in schema.fieldNames()


def test_validate_schema_matching():
    """Tests that validate_schema succeeds when schemas match exactly."""
    registry = get_registry()
    
    # Construct a matching schema for a simple table (e.g. silver.customers)
    schema = StructType([
        StructField("customer_id", LongType(), False),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("address", StringType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("captured_at", TimestampType(), True),
    ])
    
    # Should not raise any exception
    validate_schema(schema, "silver.customers")


def test_validate_schema_evolution_disabled():
    """Tests that validate_schema raises DataContractViolation when extra columns are added but evolution is 'none'."""
    # We will override the contract's evolution policy temporarily
    registry = get_registry()
    contract = registry.get_contract("silver.customers")
    original_policy = contract.get("allowed_schema_evolution")
    
    try:
        contract["allowed_schema_evolution"] = "none"
        
        # Schema with an extra column
        schema = StructType([
            StructField("customer_id", LongType(), False),
            StructField("full_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("address", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("captured_at", TimestampType(), True),
            StructField("extra_column", StringType(), True)  # extra
        ])
        
        with pytest.raises(DataContractViolation) as exc:
            validate_schema(schema, "silver.customers")
        assert "Schema evolution is disabled" in str(exc.value)
    finally:
        contract["allowed_schema_evolution"] = original_policy


def test_validate_schema_missing_columns():
    """Tests that validate_schema raises DataContractViolation when required columns are missing."""
    schema = StructType([
        StructField("customer_id", LongType(), False),
        StructField("full_name", StringType(), True),
        # missing email
    ])
    
    with pytest.raises(DataContractViolation) as exc:
        validate_schema(schema, "silver.customers")
    assert "missing required contract columns" in str(exc.value)


def test_lineage_tracker_records_runs(monkeypatch):
    """Tests that LineageTracker logs inputs and outputs correctly and emits event."""
    emitted_events = []
    
    # Mock Kafka send and DB logging
    monkeypatch.setattr("dataone.ingestion.kafka_producers.send", lambda topic, val: emitted_events.append(val))
    monkeypatch.setattr("dataone.ingestion.kafka_producers.flush_producer", lambda: None)
    
    with LineageTracker("test_job") as tracker:
        tracker.add_input("silver.orders")
        tracker.add_output("gold.daily_sales", records_written=100, records_failed=2)
        
    assert len(tracker.inputs) == 1
    assert tracker.inputs[0].name == "silver.orders"
    assert len(tracker.outputs) == 1
    assert tracker.outputs[0]["dataset"].name == "gold.daily_sales"
    assert tracker.records_processed == 100
    assert tracker.records_quarantined == 2
    assert tracker.status == "success"
    
    # Assert start and complete events are emitted
    assert len(emitted_events) == 2
    assert emitted_events[0]["eventType"] == "START"
    assert emitted_events[1]["eventType"] == "COMPLETE"
    assert emitted_events[0]["job"]["name"] == "test_job"


@pytest.mark.parametrize("dataset", [
    "bronze.products",
    "bronze.order_items",
    "silver.products",
    "silver.order_items",
])
def test_new_bronze_silver_contracts_are_loadable(dataset):
    """Verifies that all four new contracts introduced for the Postgres
    products/order_items Medallion migration are present, valid JSON, and
    contain at least one column definition."""
    registry = get_registry()
    contract = registry.get_contract(dataset)
    assert contract is not None, f"Contract not found: {dataset}"
    assert contract["dataset"] == dataset
    assert "columns" in contract
    assert len(contract["columns"]) > 0, f"{dataset} contract has no columns"


def test_new_contracts_primary_keys():
    """silver.products and silver.order_items must declare primary_keys."""
    registry = get_registry()
    for dataset in ("silver.products", "silver.order_items"):
        contract = registry.get_contract(dataset)
        assert "primary_keys" in contract, f"{dataset} missing primary_keys"
        assert len(contract["primary_keys"]) > 0


def test_bronze_products_has_ingested_at():
    """bronze.products must include the ingested_at audit column."""
    registry = get_registry()
    contract = registry.get_contract("bronze.products")
    col_names = [col["name"] for col in contract["columns"]]
    assert "ingested_at" in col_names


def test_bronze_order_items_has_ingested_at_and_partition():
    """bronze.order_items must include ingested_at and a partition spec."""
    registry = get_registry()
    contract = registry.get_contract("bronze.order_items")
    col_names = [col["name"] for col in contract["columns"]]
    assert "ingested_at" in col_names
    assert "partition_by" in contract
    assert len(contract["partition_by"]) > 0
