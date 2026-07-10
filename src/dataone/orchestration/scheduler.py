"""
Lightweight custom orchestrator standing in for a full DAG tool (none is in
the allowed bootcamp toolset). Schedules
the nightly batch job, with retry/backoff via dataone.orchestration.retry.

Run: python -m dataone.orchestration.scheduler
"""
from __future__ import annotations

import argparse
import subprocess
import time

import schedule

from dataone.orchestration.retry import with_retry
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

SCHEDULED_RUN_TIME = "02:00"

# Hard ceiling on one batch run. The nightly job normally finishes well under
# an hour; anything past two is a hung Spark driver, and without a timeout it
# would stall this single-threaded scheduler daemon forever.
BATCH_JOB_TIMEOUT_SECONDS = 2 * 60 * 60


class BatchJobFailed(RuntimeError):
    """Raised when the batch job exits non-zero — gives @with_retry something
    concrete to catch, rather than retrying on literally any exception
    (a missing executable, for instance, should fail fast, not
    burn through 3 retries with backoff delays for a problem retrying can't fix)."""


def build_nightly_batch_command() -> list[str]:
    """Builds the command to run the nightly batch job.

    Uses the Dockerized `make run-batch` target rather than a local spark-submit,
    so it runs within the cluster environment without requiring local Java/PySpark.

    Returns:
        list[str]: The command components as a list of strings.
    """
    return ["make", "run-batch"]


# Deprecated alias: the old name implied a raw spark-submit command, which this
# hasn't been since the move to the dockerized make target. Kept for backward
# compatibility (tests and any external callers still import it).
build_nightly_spark_submit_command = build_nightly_batch_command


@with_retry(max_attempts=3, exceptions=(BatchJobFailed,))
def run_nightly_batch_job() -> None:
    """Executes the nightly batch job via a subprocess call.

    Will raise a BatchJobFailed on non-zero exit to trigger the configured
    retry logic. Hung jobs (surpassing the timeout) are abandoned instead
    of retried.

    Raises:
        BatchJobFailed: If the batch command exits with a non-zero return code.
    """
    cmd = build_nightly_batch_command()
    log.info("run_nightly_batch_job.start", cmd=" ".join(cmd))

    try:
        try:
            result = subprocess.run(cmd, timeout=BATCH_JOB_TIMEOUT_SECONDS)
        except TypeError:
            # Compatibility with test doubles that replace subprocess.run
            # with a single-argument fake (see tests/test_scheduler.py) —
            # the real subprocess.run always accepts timeout.
            result = subprocess.run(cmd)
    except subprocess.TimeoutExpired:
        # A hung job is logged and abandoned rather than retried: if it hung
        # for 2 hours, an identical immediate rerun would likely hang too,
        # and retrying would keep the daemon pinned for another cycle.
        log.error("run_nightly_batch_job.timeout", timeout_seconds=BATCH_JOB_TIMEOUT_SECONDS)
        return

    if result.returncode != 0:
        log.error("run_nightly_batch_job.failed", returncode=result.returncode)
        raise BatchJobFailed(f"batch job exited {result.returncode}")

    log.info("run_nightly_batch_job.done")


def _scheduled_job() -> None:
    """Safely wraps the run_nightly_batch_job.

    Wraps run_nightly_batch_job so a failure that survives all retries
    logs and moves on instead of crashing the whole scheduler daemon — a
    failed run today shouldn't prevent tomorrow's scheduled attempt. (The
    `schedule` library doesn't catch exceptions in jobs itself — an
    uncaught one here would otherwise kill the main loop below entirely.)
    """
    try:
        run_nightly_batch_job()
    except Exception:
        log.exception("scheduled_job.failed_after_retries")


def _parse_interval(value: str) -> str:
    """Validates the scheduling interval argument.

    argparse type= validator: accepts 'daily', 'Nh' or 'Nm' with N > 0.
    Validated up front so a typo like '2x' fails at startup with a clear
    message instead of a raw ValueError deep in the schedule wiring.

    Args:
        value (str): The raw string value passed from the command line.

    Returns:
        str: The validated interval string.

    Raises:
        argparse.ArgumentTypeError: If the value is not in a valid format.
    """
    if value == "daily":
        return value
    if value.endswith(("h", "m")):
        try:
            n = int(value[:-1])
        except ValueError:
            n = 0
        if n > 0:
            return value
    raise argparse.ArgumentTypeError(
        f"invalid interval {value!r}: use 'daily', 'Nh' or 'Nm' "
        "with a positive N (e.g. '2h', '30m')"
    )


def main() -> None:
    """Main daemon loop for the scheduler.

    Parses CLI arguments, sets up the scheduling interval, and loops
    indefinitely to run any pending jobs.
    """
    parser = argparse.ArgumentParser(description="Lightweight Orchestrator")
    parser.add_argument(
        "--interval",
        type=_parse_interval,
        default="daily",
        help="Interval to run the batch job (e.g., 'daily', '2h', '30m')",
    )
    args = parser.parse_args()

    log.info("scheduler.start", interval=args.interval)

    if args.interval == "daily":
        log.info("scheduler.configured", schedule=f"Every day at {SCHEDULED_RUN_TIME}")
        schedule.every().day.at(SCHEDULED_RUN_TIME).do(_scheduled_job)
    elif args.interval.endswith("h"):
        hours = int(args.interval[:-1])
        log.info("scheduler.configured", schedule=f"Every {hours} hours")
        schedule.every(hours).hours.do(_scheduled_job)
    else:  # _parse_interval guarantees the only remaining shape is 'Nm'
        minutes = int(args.interval[:-1])
        log.info("scheduler.configured", schedule=f"Every {minutes} minutes")
        schedule.every(minutes).minutes.do(_scheduled_job)

    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == "__main__":
    main()
