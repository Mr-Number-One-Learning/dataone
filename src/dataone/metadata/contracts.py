"""
Data Contract enforcement module.
Validates Spark DataFrame schemas against contract schemas defined in the metadata layer.
"""
from __future__ import annotations

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from dataone.metadata.registry import get_registry
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

class DataContractViolation(RuntimeError):
    """Raised when an incoming dataset violates its defined data contract."""
    pass

def validate_schema(df_schema: StructType, dataset_name: str) -> None:
    """Validates the schema of a DataFrame against the registered dataset contract.

    Checks:
    - Columns existence (all columns in contract must exist in DataFrame).
    - Column types match exactly (or are compatible).
    - Checks allowed schema evolution based on policy ('none', 'backward').

    Args:
        df_schema (StructType): The schema of the incoming DataFrame.
        dataset_name (str): The qualified name of the dataset (e.g. 'silver.orders').

    Raises:
        DataContractViolation: If the schema violates the contract evolution policy.
    """
    registry = get_registry()
    contract = registry.get_contract(dataset_name)
    if not contract:
        log.warning("contracts.validate.no_contract_found", dataset=dataset_name)
        return

    evolution_policy = contract.get("allowed_schema_evolution", "backward").lower()
    
    contract_fields = {col["name"]: col for col in contract["columns"]}
    df_fields = {field.name: field for field in df_schema.fields}

    # 1. Check types for matching columns
    for col_name, contract_col in contract_fields.items():
        if col_name in df_fields:
            df_type = df_fields[col_name].dataType.simpleString().upper()
            contract_type = contract_col["type"].upper()
            
            # Normalize timestamp representation in PySpark simpleString
            if "TIMESTAMP" in df_type and "TIMESTAMP" in contract_type:
                continue
            
            if df_type != contract_type:
                msg = f"Type mismatch for column '{col_name}' in dataset '{dataset_name}': expected {contract_type}, got {df_type}"
                log.error("contracts.validate.type_mismatch", error=msg)
                raise DataContractViolation(msg)

    # 2. Check for missing columns
    missing_cols = [c for c in contract_fields if c not in df_fields]
    if missing_cols:
        msg = f"Schema is missing required contract columns in dataset '{dataset_name}': {missing_cols}"
        log.error("contracts.validate.missing_columns", error=msg)
        raise DataContractViolation(msg)

    # 3. Check for added columns (evolution policy check)
    added_cols = [c for c in df_fields if c not in contract_fields]
    if added_cols:
        if evolution_policy == "none":
            msg = f"Schema evolution is disabled ('none') but extra columns were found in dataset '{dataset_name}': {added_cols}"
            log.error("contracts.validate.evolution_violation", error=msg)
            raise DataContractViolation(msg)
        elif evolution_policy == "backward":
            # For backward compatibility, extra columns are allowed only if they are nullable
            non_nullable_added = [c for c in added_cols if not df_fields[c].nullable]
            if non_nullable_added:
                msg = f"Backward schema evolution allows only nullable new columns, but non-nullable columns were added in dataset '{dataset_name}': {non_nullable_added}"
                log.error("contracts.validate.evolution_violation", error=msg)
                raise DataContractViolation(msg)
            else:
                log.info("contracts.validate.evolution_allowed", dataset=dataset_name, added_columns=added_cols)

    log.info("contracts.validate.success", dataset=dataset_name)
