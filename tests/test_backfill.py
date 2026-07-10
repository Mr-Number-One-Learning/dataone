"""Tests for backfill.py's pure-Python pieces — date validation and
spark-submit command construction. The actual subprocess invocation in
main() isn't tested here (no spark-submit available), but everything that
can go wrong before the subprocess call is."""
from __future__ import annotations

import pytest

from dataone.batch.backfill import build_spark_submit_command, validate_date_range


def test_validate_date_range_accepts_valid_range():
    """Tests that a valid date range does not raise an error."""
    validate_date_range("2026-01-01", "2026-01-31")  # should not raise


def test_validate_date_range_accepts_equal_start_and_end():
    """Tests that a date range where start equals end is accepted."""
    validate_date_range("2026-01-15", "2026-01-15")  # should not raise


def test_validate_date_range_rejects_start_after_end():
    """Tests that a date range with start after end raises ValueError."""
    with pytest.raises(ValueError):
        validate_date_range("2026-02-01", "2026-01-01")


def test_validate_date_range_rejects_malformed_date():
    """Tests that a malformed date string raises ValueError."""
    with pytest.raises(ValueError):
        validate_date_range("not-a-date", "2026-01-01")


def test_build_spark_submit_command():
    """Tests that the spark-submit command is built with correct date arguments."""
    cmd = build_spark_submit_command("2026-01-01", "2026-01-31")
    # BATCH_JOB_PATH is resolved to an absolute path against the package
    # location (CWD-independent), so assert on shape, not the exact string.
    assert cmd[0] == "spark-submit"
    assert cmd[1].endswith("bronze_to_silver.py")
    assert cmd[2:] == ["--start", "2026-01-01", "--end", "2026-01-31"]
