import os
import subprocess
from prefect import flow, task

class BatchJobFailed(RuntimeError):
    pass

def _run_stage(stage_name: str, start: str | None = None, end: str | None = None) -> None:
    env = os.environ.copy()
    if start and end:
        env["START_DATE"] = start
        env["END_DATE"] = end
    env["STAGE"] = stage_name
    
    cmd = ["make", "run-batch"]
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise BatchJobFailed(f"make run-batch STAGE={stage_name} exited {result.returncode}")

@task(name="run-ingest-bronze", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_ingest_bronze(start: str | None = None, end: str | None = None) -> None:
    _run_stage("ingest_bronze", start, end)

@task(name="run-standardize-silver", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_standardize_silver(start: str | None = None, end: str | None = None) -> None:
    _run_stage("standardize_silver", start, end)

@task(name="run-model-gold", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_model_gold(start: str | None = None, end: str | None = None) -> None:
    _run_stage("model_gold", start, end)

@task(name="run-clickhouse-sync", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_clickhouse_sync(start: str | None = None, end: str | None = None) -> None:
    _run_stage("sync_clickhouse", start, end)

@task(name="run-bronze-to-silver", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
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
    run_ingest_bronze()
    run_standardize_silver()
    run_model_gold()
    run_clickhouse_sync()

@flow(name="backfill")
def backfill(start: str, end: str) -> None:
    """On-demand, parameterized — replaces running backfill.py by hand."""
    run_ingest_bronze(start=start, end=end)
    run_standardize_silver(start=start, end=end)
    run_model_gold(start=start, end=end)
    run_clickhouse_sync(start=start, end=end)

if __name__ == "__main__":
    nightly_batch.serve(name="nightly", cron="0 2 * * *")
