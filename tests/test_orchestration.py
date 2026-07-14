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
    nightly_batch.run_bronze_to_silver.fn()
    assert len(calls) == 1
    assert calls[0][0] == ["make", "run-batch"]
    assert "START_DATE" not in calls[0][1]


def test_run_bronze_to_silver_with_backfill_dates(monkeypatch):
    """Verifies that start/end dates are passed as env vars."""
    calls = []

    def fake_run(cmd, env=None):
        calls.append((cmd, env))
        return _FakeResult(0)

    monkeypatch.setattr(nightly_batch.subprocess, "run", fake_run)

    # Run task function directly using .fn
    nightly_batch.run_bronze_to_silver.fn(start="2026-01-01", end="2026-01-10")
    assert len(calls) == 1
    assert calls[0][1]["START_DATE"] == "2026-01-01"
    assert calls[0][1]["END_DATE"] == "2026-01-10"


def test_run_bronze_to_silver_raises_on_failure(monkeypatch):
    """Tests that a non-zero exit code raises BatchJobFailed."""
    monkeypatch.setattr(nightly_batch.subprocess, "run", lambda cmd, env=None: _FakeResult(1))

    # Run task function directly using .fn
    with pytest.raises(nightly_batch.BatchJobFailed):
        nightly_batch.run_bronze_to_silver.fn()


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

    nightly_batch.run_ingest_bronze.fn()
    nightly_batch.run_standardize_silver.fn(start="2026-01-01", end="2026-01-02")
    
    assert len(calls) == 2
    assert calls[0][0] == ["make", "run-batch"]
    assert calls[0][1]["STAGE"] == "ingest_bronze"
    assert "START_DATE" not in calls[0][1]
    
    assert calls[1][0] == ["make", "run-batch"]
    assert calls[1][1]["STAGE"] == "standardize_silver"
    assert calls[1][1]["START_DATE"] == "2026-01-01"
    assert calls[1][1]["END_DATE"] == "2026-01-02"

