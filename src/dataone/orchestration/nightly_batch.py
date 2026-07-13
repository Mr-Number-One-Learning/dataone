import os
import subprocess
from prefect import flow, task
class BatchJobFailed(RuntimeError):
    pass

@task(retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_bronze_to_silver(start: str | None = None, end: str | None = None) -> None:
    env = os.environ.copy()
    if start and end:
        env["START_DATE"] = start
        env["END_DATE"] = end
        
    cmd = ["make", "run-batch"]
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise BatchJobFailed(f"make run-batch exited {result.returncode}")

@flow(name="nightly-batch")
def nightly_batch() -> None:
    run_bronze_to_silver()

@flow(name="backfill")
def backfill(start: str, end: str) -> None:
    """On-demand, parameterized — replaces running backfill.py by hand."""
    run_bronze_to_silver(start=start, end=end)

if __name__ == "__main__":
    nightly_batch.serve(name="nightly", cron="0 2 * * *")
