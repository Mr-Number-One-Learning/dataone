import os
import pytest
from unittest.mock import MagicMock, patch
import uuid
import datetime

from dataone.lineage.tracker import LineageTracker, Dataset

@pytest.fixture
def mock_db_and_kafka(monkeypatch):
    """Mocks psycopg2.connect and kafka_producers."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    
    mock_connect = MagicMock(return_value=mock_conn)
    monkeypatch.setattr("psycopg2.connect", mock_connect)
    
    mock_kafka = MagicMock()
    monkeypatch.setattr("dataone.ingestion.kafka_producers.send", mock_kafka.send)
    monkeypatch.setattr("dataone.ingestion.kafka_producers.flush_producer", mock_kafka.flush_producer)
    
    return mock_connect, mock_cursor, mock_kafka

def test_lineage_tracker_db_start_run_ordering(mock_db_and_kafka):
    mock_connect, mock_cursor, _ = mock_db_and_kafka
    
    with LineageTracker(
        job_name="test_job",
        start_date="2026-01-01",
        end_date="2026-01-02"
    ) as tracker:
        pass
    
    # Assert start run was called with correct column ordering
    # VALUES (%s, %s, 'running', %s, %s)
    # Args order: self.run_id, self.job_name, self.start_date, self.end_date
    assert mock_cursor.execute.call_count >= 1
    start_call = mock_cursor.execute.call_args_list[0]
    sql = start_call[0][0]
    sql_args = start_call[0][1]
    
    assert "INSERT INTO _pipeline_runs" in sql
    assert "VALUES (%s, %s, 'running', %s, %s)" in sql
    assert sql_args == (tracker.run_id, "test_job", "2026-01-01", "2026-01-02")

def test_lineage_tracker_db_complete_run(mock_db_and_kafka):
    _, mock_cursor, _ = mock_db_and_kafka
    
    with LineageTracker(job_name="test_job") as tracker:
        tracker.add_output("silver.test_table", records_written=100, records_failed=5)
    
    assert mock_cursor.execute.call_count >= 2
    complete_call = mock_cursor.execute.call_args_list[1]
    sql = complete_call[0][0]
    sql_args = complete_call[0][1]
    
    assert "UPDATE _pipeline_runs" in sql
    assert "status = %s" in sql
    assert "rows_processed = %s" in sql
    assert "rows_quarantined = %s" in sql
    assert sql_args == ("success", 100, 5, None, tracker.run_id)

def test_lineage_tracker_db_complete_run_on_exception(mock_db_and_kafka):
    _, mock_cursor, _ = mock_db_and_kafka
    
    with pytest.raises(ValueError, match="something went wrong"):
        with LineageTracker(job_name="test_job") as tracker:
            raise ValueError("something went wrong")
            
    assert mock_cursor.execute.call_count >= 2
    complete_call = mock_cursor.execute.call_args_list[1]
    sql = complete_call[0][0]
    sql_args = complete_call[0][1]
    
    assert "UPDATE _pipeline_runs" in sql
    assert "status = %s" in sql
    assert "rows_processed = %s" in sql
    assert "rows_quarantined = %s" in sql
    assert sql_args == ("failed", 0, 0, "something went wrong", tracker.run_id)

def test_emit_openlineage_event_spec_compliance(mock_db_and_kafka):
    _, _, mock_kafka = mock_db_and_kafka
    
    with LineageTracker(
        job_name="test_job",
        start_date="2026-01-01",
        end_date="2026-01-02"
    ) as tracker:
        tracker.add_input("bronze.input_table")
        tracker.add_output("silver.output_table", records_written=42, records_failed=1)
        
    # We should have at least 2 sends: one for START, one for COMPLETE
    assert mock_kafka.send.call_count >= 2
    
    # Check COMPLETE event
    complete_call_args = mock_kafka.send.call_args_list[1][0]
    topic = complete_call_args[0]
    payload = complete_call_args[1]
    
    assert topic == "openlineage-events"
    assert payload["eventType"] == "COMPLETE"
    assert payload["schemaURL"] == "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
    
    # Run facets validation
    run_facets = payload["run"]["facets"]
    assert "nominalTime" in run_facets
    assert run_facets["nominalTime"]["nominalStartTime"] == "2026-01-01"
    assert run_facets["nominalTime"]["nominalEndTime"] == "2026-01-02"
    
    assert "processing_engine" in run_facets
    assert run_facets["processing_engine"]["name"] == "spark"
    assert run_facets["processing_engine"]["version"] == "3.5.1"
    
    # Inputs/outputs validation
    assert len(payload["inputs"]) == 1
    assert payload["inputs"][0]["name"] == "bronze.input_table"
    
    assert len(payload["outputs"]) == 1
    output = payload["outputs"][0]
    assert output["name"] == "silver.output_table"
    
    output_facets = output["facets"]
    assert "outputStatistics" in output_facets
    assert output_facets["outputStatistics"]["rowCount"] == 42
    
    assert "dataone_quality" in output_facets
    assert output_facets["dataone_quality"]["quarantinedCount"] == 1

def test_emit_openlineage_event_parent_facet(mock_db_and_kafka, monkeypatch):
    _, _, mock_kafka = mock_db_and_kafka
    
    monkeypatch.setenv("PARENT_RUN_ID", "parent-run-1234")
    monkeypatch.setenv("PARENT_JOB_NAME", "parent-job-prefect")
    
    with LineageTracker(job_name="test_job") as tracker:
        pass
        
    start_call_args = mock_kafka.send.call_args_list[0][0]
    payload = start_call_args[1]
    
    run_facets = payload["run"]["facets"]
    assert "parent" in run_facets
    assert run_facets["parent"]["run"]["runId"] == "parent-run-1234"
    assert run_facets["parent"]["job"]["name"] == "parent-job-prefect"
    assert run_facets["parent"]["job"]["namespace"] == "dataone"
