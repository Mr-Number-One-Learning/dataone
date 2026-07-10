import pytest
from datetime import date
from pyspark.sql import SparkSession

from dataone.batch.bronze_to_silver import build_quality_gate_summary
from dataone.quality.validators import QualityResult

@pytest.mark.spark
def test_build_quality_gate_summary(spark: SparkSession):
    # Given
    test_date = date(2025, 1, 1)
    results = {
        "campaigns": QualityResult(passed_df=None, quarantined_df=None, passed_count=100, quarantined_count=5),
        "customers": QualityResult(passed_df=None, quarantined_df=None, passed_count=200, quarantined_count=10)
    }

    # When
    summary_df = build_quality_gate_summary(spark, test_date, results)
    
    # Then
    rows = summary_df.collect()
    assert len(rows) == 2
    
    # Check that rows have correct values
    campaigns_row = next(r for r in rows if r["table_name"] == "campaigns")
    assert campaigns_row["batch_date"] == test_date
    assert campaigns_row["passed_count"] == 100
    assert campaigns_row["quarantined_count"] == 5

    customers_row = next(r for r in rows if r["table_name"] == "customers")
    assert customers_row["batch_date"] == test_date
    assert customers_row["passed_count"] == 200
    assert customers_row["quarantined_count"] == 10
