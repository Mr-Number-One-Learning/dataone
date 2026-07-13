"""
Metadata Registry to load and query dataset schemas, data contracts, data quality rules, and lineage.
"""
from __future__ import annotations

import os
import json
import re
from typing import Any, Dict, List, Optional

class MetadataRegistry:
    def __init__(self, metadata_dir: Optional[str] = None):
        if not metadata_dir:
            # Fallback to relative path from this file: ../../../metadata
            metadata_dir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "..", "metadata")
            )
        self.metadata_dir = metadata_dir
        self._cache: Dict[str, Dict[str, Any]] = {
            "datasets": {},
            "ownership": {},
            "contracts": {},
            "quality": {},
            "lineage": {}
        }
        self._load_all()

    def _load_all(self):
        for category in self._cache.keys():
            category_dir = os.path.join(self.metadata_dir, category)
            if not os.path.exists(category_dir):
                continue
            for filename in os.listdir(category_dir):
                if filename.endswith(".json"):
                    dataset_name = filename[:-5]  # strip .json
                    filepath = os.path.join(category_dir, filename)
                    try:
                        with open(filepath, "r") as f:
                            self._cache[category][dataset_name] = json.load(f)
                    except Exception as e:
                        # Log warning or print (fallback)
                        print(f"Error loading metadata file {filepath}: {e}")

    def get_dataset(self, name: str) -> Dict[str, Any]:
        return self._cache["datasets"].get(name, {})

    def get_ownership(self, name: str) -> Dict[str, Any]:
        return self._cache["ownership"].get(name, {})

    def get_contract(self, name: str) -> Dict[str, Any]:
        return self._cache["contracts"].get(name, {})

    def get_quality(self, name: str) -> Dict[str, Any]:
        return self._cache["quality"].get(name, {})

    def get_lineage(self, name: str) -> Dict[str, Any]:
        return self._cache["lineage"].get(name, {})

    def list_datasets(self) -> List[str]:
        return list(self._cache["datasets"].keys())

    def get_spark_schema(self, name: str):
        """Builds a PySpark StructType schema from the contract columns definition."""
        from pyspark.sql.types import StructType, StructField
        
        contract = self.get_contract(name)
        if not contract or "columns" not in contract:
            raise ValueError(f"No contract schema found for dataset: {name}")

        fields = []
        for col in contract["columns"]:
            col_name = col["name"]
            col_type = self._parse_spark_type(col["type"])
            nullable = col.get("nullable", True)
            fields.append(StructField(col_name, col_type, nullable))

        return StructType(fields)

    def _parse_spark_type(self, type_str: str):
        from pyspark.sql.types import (
            StringType, LongType, IntegerType, DoubleType,
            BooleanType, TimestampType, DateType, DecimalType
        )
        type_str = type_str.upper().strip()
        if type_str == "STRING":
            return StringType()
        if type_str == "BIGINT":
            return LongType()
        if type_str == "INT":
            return IntegerType()
        if type_str == "DOUBLE":
            return DoubleType()
        if type_str == "BOOLEAN":
            return BooleanType()
        if type_str == "TIMESTAMP":
            return TimestampType()
        if type_str == "DATE":
            return DateType()
        
        decimal_match = re.match(r"DECIMAL\((\d+)\s*,\s*(\d+)\)", type_str)
        if decimal_match:
            precision = int(decimal_match.group(1))
            scale = int(decimal_match.group(2))
            return DecimalType(precision, scale)
            
        raise ValueError(f"Unknown type string: {type_str}")

# Global registry instance
_registry = MetadataRegistry()

def get_registry() -> MetadataRegistry:
    return _registry
