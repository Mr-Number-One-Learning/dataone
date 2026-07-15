"""
Tests for the Prefect orchestration flows and tasks.
Uses the task/flow .fn attributes to run raw Python functions without connecting to Prefect API.
"""
from __future__ import annotations

import pytest
import subprocess
import psycopg2

from dataone.orchestration import nightly_batch
from dataone.orchestration import cdc_poll
import os

# Disable Prefect API connectivity globally for tests to prevent hanging on server connection timeouts
os.environ["PREFECT_API_URL"] = ""
os.environ["PREFECT_API_KEY"] = ""



class _FakeResult:
    def __init__(self, returncode: int):
        self.returncode = returncode


def test_nightly_batch_job_runs_bronze_to_silver(monkeypatch):
    """Verifies that run_bronze_to_silver executes make run-batch with correct environment."""
    calls = []

    def fake_run(cmd, env=None):
        calls.append((cmd, env))
        return _FakeResult(0)

    monkeypatch.setattr(nightly_batch.subprocess, "run", fake_run)

    # Run task function directly using .fn
    nightly_batch.run_bronze_to_silver.fn(parent_run_id="fake-parent-id", parent_job_name="fake-parent-job")
    assert len(calls) == 1
    assert calls[0][0] == ["make", "run-batch"]
    assert "START_DATE" not in calls[0][1]
    assert calls[0][1]["PARENT_RUN_ID"] == "fake-parent-id"
    assert calls[0][1]["PARENT_JOB_NAME"] == "fake-parent-job"


def test_run_bronze_to_silver_with_backfill_dates(monkeypatch):
    """Verifies that start/end dates are passed as env vars."""
    calls = []

    def fake_run(cmd, env=None):
        calls.append((cmd, env))
        return _FakeResult(0)

    monkeypatch.setattr(nightly_batch.subprocess, "run", fake_run)

    # Run task function directly using .fn
    nightly_batch.run_bronze_to_silver.fn(
        parent_run_id="fake-parent-id",
        parent_job_name="fake-parent-job",
        start="2026-01-01",
        end="2026-01-10"
    )
    assert len(calls) == 1
    assert calls[0][1]["START_DATE"] == "2026-01-01"
    assert calls[0][1]["END_DATE"] == "2026-01-10"
    assert calls[0][1]["PARENT_RUN_ID"] == "fake-parent-id"
    assert calls[0][1]["PARENT_JOB_NAME"] == "fake-parent-job"


def test_run_bronze_to_silver_raises_on_failure(monkeypatch):
    """Tests that a non-zero exit code raises BatchJobFailed."""
    monkeypatch.setattr(nightly_batch.subprocess, "run", lambda cmd, env=None: _FakeResult(1))

    # Run task function directly using .fn
    with pytest.raises(nightly_batch.BatchJobFailed):
        nightly_batch.run_bronze_to_silver.fn(parent_run_id="fake-parent-id", parent_job_name="fake-parent-job")


def test_cdc_poll_flow_calls_init_and_tasks(monkeypatch):
    """Verifies cdc_poll flow calls tasks correctly."""
    init_called = [False]
    poll_calls = []

    # Mock database connections
    class FakeCursor:
        def execute(self, *args, **kwargs):
            pass
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()
        def close(self):
            pass

    monkeypatch.setattr(psycopg2, "connect", lambda dsn: FakeConn())
    monkeypatch.setattr(cdc_poll, "init_watermarks", cdc_poll.init_watermarks.fn)
    monkeypatch.setattr(cdc_poll, "poll_table", cdc_poll.poll_table.fn)
    monkeypatch.setattr(cdc_poll, "ensure_watermark_table", lambda conn: init_called.pop() or init_called.append(True))
    monkeypatch.setattr(cdc_poll, "poll_once", lambda conn, tbl, pk: poll_calls.append((tbl, pk)) or 10)

    # Run flow function directly using .fn
    cdc_poll.cdc_poll.fn()

    assert init_called == [True]
    assert len(poll_calls) == 2
    assert ("customers", "customer_id") in poll_calls
    assert ("orders", "order_id") in poll_calls


def test_run_stage_tasks(monkeypatch):
    """Verifies that individual stage tasks pass the STAGE env variable."""
    calls = []

    def fake_run(cmd, env=None):
        calls.append((cmd, env))
        return _FakeResult(0)

    monkeypatch.setattr(nightly_batch.subprocess, "run", fake_run)

    nightly_batch.run_ingest_bronze.fn(parent_run_id="id1", parent_job_name="job1")
    nightly_batch.run_standardize_silver.fn(parent_run_id="id2", parent_job_name="job2", start="2026-01-01", end="2026-01-02")
    nightly_batch.run_model_gold.fn(parent_run_id="id3", parent_job_name="job3")
    nightly_batch.run_clickhouse_sync.fn(parent_run_id="id4", parent_job_name="job4", start="2026-01-01", end="2026-01-02")

    assert len(calls) == 4

    assert calls[0][0] == ["make", "run-batch"]
    assert calls[0][1]["STAGE"] == "ingest_bronze"
    assert "START_DATE" not in calls[0][1]
    assert calls[0][1]["PARENT_RUN_ID"] == "id1"
    assert calls[0][1]["PARENT_JOB_NAME"] == "job1"

    assert calls[1][0] == ["make", "run-batch"]
    assert calls[1][1]["STAGE"] == "standardize_silver"
    assert calls[1][1]["START_DATE"] == "2026-01-01"
    assert calls[1][1]["END_DATE"] == "2026-01-02"
    assert calls[1][1]["PARENT_RUN_ID"] == "id2"
    assert calls[1][1]["PARENT_JOB_NAME"] == "job2"

    assert calls[2][0] == ["make", "run-batch"]
    assert calls[2][1]["STAGE"] == "model_gold"
    assert "START_DATE" not in calls[2][1]
    assert calls[2][1]["PARENT_RUN_ID"] == "id3"
    assert calls[2][1]["PARENT_JOB_NAME"] == "job3"

    assert calls[3][0] == ["make", "run-batch"]
    assert calls[3][1]["STAGE"] == "sync_clickhouse"
    assert calls[3][1]["START_DATE"] == "2026-01-01"
    assert calls[3][1]["END_DATE"] == "2026-01-02"
    assert calls[3][1]["PARENT_RUN_ID"] == "id4"
    assert calls[3][1]["PARENT_JOB_NAME"] == "job4"


def test_publish_run_summary(monkeypatch):
    """Verifies that publish_run_summary queries the DB and creates a markdown artifact in Prefect."""
    import datetime
    db_calls = []
    artifact_calls = []

    # Mock cursor and connection
    class FakeCursor:
        def execute(self, query, params=None):
            db_calls.append((query, params))

        def fetchall(self):
            return [
                (
                    "bronze_to_silver.ingest_bronze",
                    "success",
                    1000,
                    0,
                    None,
                    datetime.datetime.now(datetime.timezone.utc),
                ),
                (
                    "bronze_to_silver.standardize_silver",
                    "failed",
                    900,
                    100,
                    "Some schema violation",
                    datetime.datetime.now(datetime.timezone.utc),
                ),
            ]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    # Mock psycopg2 and prefect artifact API
    monkeypatch.setattr(psycopg2, "connect", lambda dsn: FakeConn())

    def fake_create_markdown_artifact(key, markdown, description=None):
        artifact_calls.append((key, markdown, description))

    # We mock create_markdown_artifact directly in prefect.artifacts module
    import prefect.artifacts
    monkeypatch.setattr(
        prefect.artifacts,
        "create_markdown_artifact",
        fake_create_markdown_artifact,
    )

    flow_start_time = datetime.datetime(2026, 7, 15, 0, 0, 0, tzinfo=datetime.timezone.utc)
    nightly_batch.publish_run_summary.fn(
        flow_run_id="test-flow-id", flow_start_time=flow_start_time
    )

    assert len(db_calls) == 1
    assert "WHERE start_time >= %s" in db_calls[0][0]
    assert db_calls[0][1][0] == flow_start_time

    assert len(artifact_calls) == 1
    key, markdown, description = artifact_calls[0]
    assert key == "nightly-etl-summary"
    assert "test-flow-id" in markdown
    assert "ingest_bronze" in markdown
    assert "✅ Success" in markdown
    assert "1,000" in markdown
    assert "standardize_silver" in markdown
    assert "❌ Failed" in markdown
    assert "Some schema violation" in markdown


