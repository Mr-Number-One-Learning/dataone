"""
Data Lineage tracking and OpenLineage/Marquez integration module.
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

    def _db_start_run(self):
        try:
            with contextlib.closing(psycopg2.connect(postgres.dsn)) as conn:
                with conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO _pipeline_runs (run_id, job_name, status, date_range_start, date_range_end)
                        VALUES (%s, 'running', %s, %s, %s)
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

    def _emit_openlineage_event(self, event_type: str):
        """Emits an OpenLineage JSON payload to Kafka."""
        try:
            payload = {
                "eventType": event_type,
                "eventTime": self.start_time.isoformat() if event_type == "START" else self.end_time.isoformat(),
                "run": {
                    "runId": self.run_id,
                    "facets": {}
                },
                "job": {
                    "namespace": "dataone",
                    "name": self.job_name,
                    "facets": {}
                },
                "inputs": [inp.to_dict() for inp in self.inputs],
                "outputs": [
                    {
                        "namespace": out["dataset"].namespace,
                        "name": out["dataset"].name,
                        "facets": {
                            "outputStatistics": {
                                "rowCount": out["records_written"],
                                "failedCount": out["records_failed"]
                            }
                        }
                    }
                    for out in self.outputs
                ],
                "producer": "https://github.com/Mr-Number-One-Learning/dataone"
            }
            
            # Send to openlineage-events topic using confluent-kafka producer / kafka utility
            kafka_producers.send("openlineage-events", payload)
            kafka_producers.flush_producer()
        except Exception as e:
            log.warning("lineage.emit_openlineage_event.failed", error=str(e))
