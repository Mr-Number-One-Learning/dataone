"""
Data Lineage tracking and OpenLineage/Marquez integration module.

Handles two concerns per pipeline run:
1. Persisting run metadata to the Postgres ``_pipeline_runs`` audit table.
2. Emitting spec-compliant OpenLineage RunEvents to the Kafka
   ``openlineage-events`` topic, where the ``marquez-kafka-consumer``
   service asynchronously drains them into the Marquez API.

When the pipeline is invoked by a Prefect orchestrator flow, the flow sets
``PARENT_RUN_ID`` and ``PARENT_JOB_NAME`` environment variables so that
every child Spark job's OpenLineage events carry a ``parent`` run facet —
letting Marquez draw the orchestrator → job relationship automatically.
"""
from __future__ import annotations

import os
import uuid
import datetime
import contextlib
import psycopg2
from typing import List, Dict, Any, Optional

from dataone.config import postgres, kafka
from dataone.ingestion import kafka_producers
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# OpenLineage spec constants
# ---------------------------------------------------------------------------
_OL_SCHEMA_URL = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
)
_OL_PRODUCER = "https://github.com/Mr-Number-One-Learning/dataone"
_PROCESSING_ENGINE_NAME = "spark"
_PROCESSING_ENGINE_VERSION = "3.5.1"


class Dataset:
    def __init__(self, name: str, namespace: str = "dataone"):
        self.name = name
        self.namespace = namespace

    def to_dict(self) -> Dict[str, Any]:
        return {
            "namespace": self.namespace,
            "name": self.name
        }


class DatasetVersion:
    def __init__(self, dataset: Dataset, version: str):
        self.dataset = dataset
        self.version = version


class PipelineRun:
    def __init__(self, run_id: str, job_name: str, status: str = "running"):
        self.run_id = run_id
        self.job_name = job_name
        self.status = status
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        self.end_time: Optional[datetime.datetime] = None


class LineageTracker:
    """Context manager for pipeline execution runs.

    Handles:
    1. Postgres metadata logging (_pipeline_runs table).
    2. Emitting OpenLineage events to the Kafka openlineage-events topic.

    If ``PARENT_RUN_ID`` and ``PARENT_JOB_NAME`` environment variables are
    set (typically by the Prefect orchestrator), the emitted OpenLineage
    events will include a ``parent`` run facet so that Marquez can link the
    Spark job run back to the orchestrator flow run.
    """
    def __init__(
        self,
        job_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ):
        self.job_name = job_name
        self.run_id = str(uuid.uuid4())
        self.start_date = start_date
        self.end_date = end_date
        self.inputs: List[Dataset] = []
        self.outputs: List[Dict[str, Any]] = []
        self.status = "running"
        self.records_processed = 0
        self.records_quarantined = 0
        self.error_message: Optional[str] = None
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        self.end_time: Optional[datetime.datetime] = None

        # Prefect orchestrator passes these so child jobs can declare a
        # parent run in their OpenLineage events.
        self._parent_run_id: Optional[str] = os.environ.get("PARENT_RUN_ID")
        self._parent_job_name: Optional[str] = os.environ.get("PARENT_JOB_NAME")

    def __enter__(self) -> LineageTracker:
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        log.info("lineage.run.start", job=self.job_name, run_id=self.run_id)
        
        # 1. Postgres Start Log
        self._db_start_run()
        
        # 2. Emit Start Lineage Event
        self._emit_openlineage_event("START")
        
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = datetime.datetime.now(datetime.timezone.utc)
        
        if exc_type is None:
            self.status = "success"
        else:
            self.status = "failed"
            self.error_message = str(exc_val)
            log.error("lineage.run.error", job=self.job_name, run_id=self.run_id, error=self.error_message)

        # 1. Postgres Complete Log
        self._db_complete_run()
        
        # 2. Emit End Lineage Event
        event_type = "COMPLETE" if self.status == "success" else "FAIL"
        self._emit_openlineage_event(event_type)
        
        log.info("lineage.run.complete", job=self.job_name, run_id=self.run_id, status=self.status)

    def add_input(self, dataset_name: str):
        """Registers an input dataset for the pipeline run."""
        self.inputs.append(Dataset(dataset_name))

    def add_output(self, dataset_name: str, records_written: int = 0, records_failed: int = 0):
        """Registers an output dataset for the pipeline run."""
        self.outputs.append({
            "dataset": Dataset(dataset_name),
            "records_written": records_written,
            "records_failed": records_failed
        })
        self.records_processed += records_written
        self.records_quarantined += records_failed

    # ------------------------------------------------------------------
    # Postgres metadata persistence
    # ------------------------------------------------------------------

    def _db_start_run(self):
        try:
            with contextlib.closing(psycopg2.connect(postgres.dsn)) as conn:
                with conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO _pipeline_runs (run_id, job_name, status, date_range_start, date_range_end)
                        VALUES (%s, %s, 'running', %s, %s)
                        """,
                        (self.run_id, self.job_name, self.start_date, self.end_date),
                    )
        except Exception as e:
            log.warning("lineage.db_start_run.failed", error=str(e))

    def _db_complete_run(self):
        try:
            with contextlib.closing(psycopg2.connect(postgres.dsn)) as conn:
                with conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE _pipeline_runs
                        SET end_time = now(),
                            status = %s,
                            rows_processed = %s,
                            rows_quarantined = %s,
                            error_message = %s
                        WHERE run_id = %s
                        """,
                        (self.status, self.records_processed, self.records_quarantined, self.error_message, self.run_id),
                    )
        except Exception as e:
            log.warning("lineage.db_complete_run.failed", error=str(e))

    # ------------------------------------------------------------------
    # OpenLineage event emission
    # ------------------------------------------------------------------

    def _build_run_facets(self) -> Dict[str, Any]:
        """Builds the ``run.facets`` dict with standard OL facets.

        Includes:
        - ``nominalTime``: the date range this run covers (or start time
          if no explicit date range was provided).
        - ``processing_engine``: identifies PySpark 3.5.1.
        - ``parent`` (conditional): links this run to a Prefect
          orchestrator flow when ``PARENT_RUN_ID`` is set.
        """
        facets: Dict[str, Any] = {}

        # nominalTime — tells Marquez what logical time window this run covers
        nominal_start = self.start_date or self.start_time.isoformat()
        nominal_end = self.end_date or (
            self.end_time.isoformat() if self.end_time else nominal_start
        )
        facets["nominalTime"] = {
            "_producer": _OL_PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json#/$defs/NominalTimeRunFacet",
            "nominalStartTime": nominal_start,
            "nominalEndTime": nominal_end,
        }

        # processing_engine — identifies the compute engine
        facets["processing_engine"] = {
            "_producer": _OL_PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet",
            "version": _PROCESSING_ENGINE_VERSION,
            "name": _PROCESSING_ENGINE_NAME,
        }

        # parent — links to the Prefect orchestrator flow run (if present)
        if self._parent_run_id and self._parent_job_name:
            facets["parent"] = {
                "_producer": _OL_PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet",
                "run": {"runId": self._parent_run_id},
                "job": {
                    "namespace": "dataone",
                    "name": self._parent_job_name,
                },
            }

        return facets

    @staticmethod
    def _build_output_facets(out: Dict[str, Any]) -> Dict[str, Any]:
        """Builds spec-compliant output dataset facets.

        Uses the standard ``outputStatistics`` facet for ``rowCount`` and a
        custom ``dataone_quality`` facet for quarantine/failure counts that
        are not part of the OL spec.
        """
        facets: Dict[str, Any] = {
            "outputStatistics": {
                "_producer": _OL_PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json#/$defs/OutputStatisticsOutputDatasetFacet",
                "rowCount": out["records_written"],
            }
        }

        # Custom facet — quarantine counts are project-specific, not part
        # of the OL spec, so we namespace them under dataone_quality.
        if out["records_failed"] > 0:
            facets["dataone_quality"] = {
                "_producer": _OL_PRODUCER,
                "_schemaURL": "https://github.com/Mr-Number-One-Learning/dataone#dataone_quality",
                "quarantinedCount": out["records_failed"],
            }

        return facets

    def _emit_openlineage_event(self, event_type: str):
        """Emits a spec-compliant OpenLineage RunEvent to Kafka."""
        try:
            payload = {
                "eventType": event_type,
                "eventTime": (
                    self.start_time.isoformat()
                    if event_type == "START"
                    else self.end_time.isoformat()
                ),
                "schemaURL": _OL_SCHEMA_URL,
                "run": {
                    "runId": self.run_id,
                    "facets": self._build_run_facets(),
                },
                "job": {
                    "namespace": "dataone",
                    "name": self.job_name,
                    "facets": {},
                },
                "inputs": [inp.to_dict() for inp in self.inputs],
                "outputs": [
                    {
                        "namespace": out["dataset"].namespace,
                        "name": out["dataset"].name,
                        "facets": self._build_output_facets(out),
                    }
                    for out in self.outputs
                ],
                "producer": _OL_PRODUCER,
            }
            
            # Send to openlineage-events topic using confluent-kafka producer / kafka utility
            kafka_producers.send("openlineage-events", payload)
            kafka_producers.flush_producer()
        except Exception as e:
            log.warning("lineage.emit_openlineage_event.failed", error=str(e))
