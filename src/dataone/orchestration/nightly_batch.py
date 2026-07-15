"""
Prefect orchestration flow for the nightly batch pipeline.

Wraps each Medallion stage (ingest_bronze → standardize_silver →
model_gold → sync_clickhouse) as its own Prefect task with independent
retry policies.

Bridges Prefect ↔ OpenLineage by passing ``PARENT_RUN_ID`` and
``PARENT_JOB_NAME`` to every child ``spark-submit`` process. The
``LineageTracker`` inside each Spark job reads these env vars and emits
a ``parent`` run facet in its OpenLineage events — so Marquez can draw
the orchestrator → job relationship without adding any Kafka dependency
to this module.
"""
import os
# Prevent Prefect client from routing local API requests through system HTTP proxies
os.environ["NO_PROXY"] = "localhost,127.0.0.1,dataone-prefect-server"
os.environ["no_proxy"] = "localhost,127.0.0.1,dataone-prefect-server"

import subprocess
import uuid

from prefect import flow, task


class BatchJobFailed(RuntimeError):
    pass


def _run_stage(
    stage_name: str,
    parent_run_id: str,
    parent_job_name: str,
    start: str | None = None,
    end: str | None = None,
) -> None:
    """Runs a single batch pipeline stage via ``make run-batch``.

    Injects ``PARENT_RUN_ID`` and ``PARENT_JOB_NAME`` so that the child
    Spark job's ``LineageTracker`` can link its OpenLineage events back
    to this Prefect flow run.
    """
    env = os.environ.copy()
    if start and end:
        env["START_DATE"] = start
        env["END_DATE"] = end
    env["STAGE"] = stage_name

    # Bridge: Prefect flow run → child Spark job's OpenLineage parent facet
    env["PARENT_RUN_ID"] = parent_run_id
    env["PARENT_JOB_NAME"] = parent_job_name

    cmd = ["make", "run-batch"]
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise BatchJobFailed(f"make run-batch STAGE={stage_name} exited {result.returncode}")


@task(name="run-ingest-bronze", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_ingest_bronze(
    parent_run_id: str,
    parent_job_name: str,
    start: str | None = None,
    end: str | None = None,
) -> None:
    _run_stage("ingest_bronze", parent_run_id, parent_job_name, start, end)


@task(name="run-standardize-silver", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_standardize_silver(
    parent_run_id: str,
    parent_job_name: str,
    start: str | None = None,
    end: str | None = None,
) -> None:
    _run_stage("standardize_silver", parent_run_id, parent_job_name, start, end)


@task(name="run-model-gold", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_model_gold(
    parent_run_id: str,
    parent_job_name: str,
    start: str | None = None,
    end: str | None = None,
) -> None:
    _run_stage("model_gold", parent_run_id, parent_job_name, start, end)


@task(name="run-clickhouse-sync", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_clickhouse_sync(
    parent_run_id: str,
    parent_job_name: str,
    start: str | None = None,
    end: str | None = None,
) -> None:
    _run_stage("sync_clickhouse", parent_run_id, parent_job_name, start, end)


@task(name="run-bronze-to-silver", retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def run_bronze_to_silver(
    parent_run_id: str,
    parent_job_name: str,
    start: str | None = None,
    end: str | None = None,
) -> None:
    """Runs the full (monolithic) pipeline — all stages in one spark-submit."""
    env = os.environ.copy()
    if start and end:
        env["START_DATE"] = start
        env["END_DATE"] = end
    env["PARENT_RUN_ID"] = parent_run_id
    env["PARENT_JOB_NAME"] = parent_job_name

    cmd = ["make", "run-batch"]
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise BatchJobFailed(f"make run-batch exited {result.returncode}")


@flow(name="nightly-batch")
def nightly_batch() -> None:
    """Nightly Medallion pipeline: Bronze → Silver → Gold → ClickHouse.

    Generates a unique ``run_id`` for this flow execution and propagates
    it to every child Spark job so their OpenLineage events carry a
    ``parent`` facet linking back to this orchestrator run.
    """
    flow_run_id = str(uuid.uuid4())
    flow_job_name = "nightly-batch"

    run_ingest_bronze(flow_run_id, flow_job_name)
    run_standardize_silver(flow_run_id, flow_job_name)
    run_model_gold(flow_run_id, flow_job_name)
    run_clickhouse_sync(flow_run_id, flow_job_name)


@flow(name="backfill")
def backfill(start: str, end: str) -> None:
    """On-demand, parameterized — replaces running backfill.py by hand."""
    flow_run_id = str(uuid.uuid4())
    flow_job_name = "backfill"

    run_ingest_bronze(start=start, end=end, parent_run_id=flow_run_id, parent_job_name=flow_job_name)
    run_standardize_silver(start=start, end=end, parent_run_id=flow_run_id, parent_job_name=flow_job_name)
    run_model_gold(start=start, end=end, parent_run_id=flow_run_id, parent_job_name=flow_job_name)
    run_clickhouse_sync(start=start, end=end, parent_run_id=flow_run_id, parent_job_name=flow_job_name)


if __name__ == "__main__":
    nightly_batch.serve(name="nightly", cron="0 2 * * *")
