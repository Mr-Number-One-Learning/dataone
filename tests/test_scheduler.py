"""
Tests for the orchestration scheduler. subprocess.run is monkeypatched —
no real spark-submit needed to test the retry/failure/success logic itself.
"""
from __future__ import annotations

from dataone.orchestration import scheduler


class _FakeResult:
    def __init__(self, returncode: int):
        self.returncode = returncode


def test_build_nightly_spark_submit_command_has_no_date_args() -> None:
    """Verifies that the nightly batch job command uses the dockerized make target."""
    cmd = scheduler.build_nightly_spark_submit_command()
    assert cmd == ["make", "run-batch"]
    assert "--start" not in cmd and "--end" not in cmd


def test_run_nightly_batch_job_success(monkeypatch):
    """Tests that a successful batch job execution completes without raising."""
    calls = []
    monkeypatch.setattr(scheduler.subprocess, "run", lambda cmd: calls.append(cmd) or _FakeResult(0))
    scheduler.run_nightly_batch_job()  # should not raise
    assert len(calls) == 1


def test_run_nightly_batch_job_raises_after_exhausting_retries(monkeypatch):
    """Tests that a persistently failing batch job eventually raises BatchJobFailed."""
    import pytest

    monkeypatch.setattr(scheduler.subprocess, "run", lambda cmd: _FakeResult(1))
    with pytest.raises(scheduler.BatchJobFailed):
        scheduler.run_nightly_batch_job()


def test_run_nightly_batch_job_retries_then_succeeds(monkeypatch):
    """Tests that the batch job succeeds if it recovers within the allowed retries."""
    attempts = {"n": 0}

    def flaky_run(cmd):
        attempts["n"] += 1
        return _FakeResult(1 if attempts["n"] < 3 else 0)

    monkeypatch.setattr(scheduler.subprocess, "run", flaky_run)
    scheduler.run_nightly_batch_job()  # should not raise
    assert attempts["n"] == 3


def test_scheduled_job_swallows_failure_instead_of_crashing_the_daemon(monkeypatch):
    """Tests that a failed scheduled job logs and swallows the error rather than crashing."""
    monkeypatch.setattr(scheduler.subprocess, "run", lambda cmd: _FakeResult(1))
    scheduler._scheduled_job()  # should NOT raise, even after retries are exhausted
