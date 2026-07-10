"""
Backfill entry point: re-runs the batch job (bronze_to_silver.py) over an
arbitrary historical date range, as its own spark-submit invocation —
deliberately a separate process/job run from the regular nightly schedule,
matching how backfills are normally triggered as a distinct, manually
initiated run rather than folded into the same process as the nightly job.

Safe to re-run: every downstream write in the batch job is either an
idempotent partition-overwrite (silver/gold via write_overwrite_partitions)
or a true upsert-with-history (the SCD2 merge) — never a blind append,
except quarantine, which is meant to accumulate across runs.

Run: python -m dataone.batch.backfill --start 2026-01-01 --end 2026-01-31
"""
from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
from datetime import date, timedelta

from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

# Resolved relative to this file, not the CWD, so the backfill works no
# matter which directory it's launched from.
BATCH_JOB_PATH = str(pathlib.Path(__file__).resolve().parent / "bronze_to_silver.py")

# A hung spark-submit shouldn't wedge the backfill forever.
BACKFILL_JOB_TIMEOUT_SECONDS = 4 * 60 * 60


def validate_date_range(start: str, end: str) -> None:
    """Validates that a date range is well-formed and logical.

    Raises ValueError on a malformed date or if start > end. Pure Python,
    deliberately separated from main() so it's testable without spark-submit.

    Args:
        start (str): The start date as a YYYY-MM-DD string.
        end (str): The end date as a YYYY-MM-DD string.

    Raises:
        ValueError: If start is after end, or if dates are malformed.
    """
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    if start_d > end_d:
        raise ValueError(f"--start ({start}) must not be after --end ({end})")


def build_spark_submit_command(start: str, end: str) -> list[str]:
    """Builds the spark-submit command for a specific backfill window.

    Args:
        start (str): The start date as a YYYY-MM-DD string.
        end (str): The end date as a YYYY-MM-DD string.

    Returns:
        list[str]: The command components ready for subprocess.run.
    """
    return ["spark-submit", BATCH_JOB_PATH, "--start", start, "--end", end]


def chunk_date_range(start: str, end: str, chunk_days: int) -> list[tuple[str, str]]:
    """Splits a date range into consecutive windows.

    Split [start, end] into consecutive windows of at most chunk_days days.
    chunk_days <= 0 means "one window for the whole range". Pure Python and
    separated from main() for testability, like validate_date_range.

    Args:
        start (str): The start date as a YYYY-MM-DD string.
        end (str): The end date as a YYYY-MM-DD string.
        chunk_days (int): The maximum number of days per chunk.

    Returns:
        list[tuple[str, str]]: A list of (start_date, end_date) tuples.
    """
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    if chunk_days <= 0:
        return [(start, end)]
    windows: list[tuple[str, str]] = []
    cursor = start_d
    while cursor <= end_d:
        window_end = min(cursor + timedelta(days=chunk_days - 1), end_d)
        windows.append((cursor.isoformat(), window_end.isoformat()))
        cursor = window_end + timedelta(days=1)
    return windows


def main() -> None:
    """Main entry point for the backfill CLI.

    Parses command-line arguments, validates the date range, and iterates
    through chunks to run spark-submit for each window.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=0,
        help=(
            "Split the range into windows of N days, run one job per window. "
            "A failure stops at the failed window, so a wide backfill resumes "
            "from where it broke instead of redoing the whole range. "
            "0 (default) = single job for the whole range."
        ),
    )
    args = parser.parse_args()

    validate_date_range(args.start, args.end)

    for window_start, window_end in chunk_date_range(args.start, args.end, args.chunk_days):
        cmd = build_spark_submit_command(window_start, window_end)
        log.info("backfill.window_start", start=window_start, end=window_end, cmd=" ".join(cmd))
        try:
            result = subprocess.run(cmd, timeout=BACKFILL_JOB_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            log.error("backfill.window_timeout", start=window_start, end=window_end)
            sys.exit(1)
        if result.returncode != 0:
            log.error(
                "backfill.window_failed",
                start=window_start,
                end=window_end,
                returncode=result.returncode,
            )
            sys.exit(result.returncode)
        log.info("backfill.window_done", start=window_start, end=window_end)

    log.info("backfill.done")


if __name__ == "__main__":
    main()
