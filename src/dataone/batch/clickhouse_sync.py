"""
ClickHouse mart loading (staging table + EXCHANGE TABLES swap), split out of
bronze_to_silver.py. bronze_to_silver re-exports these names.
"""

from __future__ import annotations

import time
import typing
import urllib.error
import urllib.request
from base64 import b64encode

from pyspark.sql import DataFrame

from dataone.config import clickhouse
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)


_CLICKHOUSE_HTTP_TIMEOUT_SECONDS = 60


_CLICKHOUSE_RETRIES = 3


def _clickhouse_query(query: str) -> None:
    """
    Fire a DDL/maintenance statement at ClickHouse over HTTP. Bounded by a
    timeout (a hung socket must not hang the whole batch run) and retried a
    few times with backoff — transient ClickHouse restarts shouldn't fail
    the nightly job. On an HTTP error the response body (ClickHouse puts the
    real error text there) is surfaced in the exception.
    """
    url = f"http://{clickhouse.host}:{clickhouse.port}/"
    auth_str = f"{clickhouse.user}:{clickhouse.password}"
    auth_b64 = b64encode(auth_str.encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth_b64}"}
    req = urllib.request.Request(url, data=query.encode("utf-8"), headers=headers, method="POST")

    last_exc: Exception | None = None
    for attempt in range(1, _CLICKHOUSE_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=_CLICKHOUSE_HTTP_TIMEOUT_SECONDS) as response:
                response.read()
            return
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            last_exc = RuntimeError(f"ClickHouse HTTP {exc.code}: {body}")
        except (urllib.error.URLError, TimeoutError) as exc:
            last_exc = exc
        log.warning("clickhouse.query.retry", attempt=attempt, error=str(last_exc))
        time.sleep(2**attempt)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("ClickHouse query failed after retries")


def _truncate_clickhouse_tables(table_names: typing.Iterable[str]) -> None:
    for t in table_names:
        try:
            _clickhouse_query(f"TRUNCATE TABLE IF EXISTS {clickhouse.db}.{t}")
            log.info("clickhouse.truncate.done", table=t)
        except Exception as e:
            log.error("clickhouse.truncate.failed", table=t, error=str(e))
            raise


def load_clickhouse_marts(gold_tables: dict[str, DataFrame]) -> None:
    """
    Mirrors each freshly computed gold DataFrame into its ClickHouse table
    for Grafana — same full-recompute-each-run idempotency as the Iceberg
    gold tables. We manually TRUNCATE first, then append via JDBC so the
    ClickHouse-side MergeTree table definition/engine settings survive
    (created in infra/docker/clickhouse/init/002_marts.sql) without Spark
    falling back to DROP/CREATE.
    """
    jdbc_url = f"jdbc:clickhouse://{clickhouse.host}:{clickhouse.port}/{clickhouse.db}"

    for table_name, df in gold_tables.items():
        staging = f"{table_name}_staging"
        try:
            # Step 1: Truncate and fill the STAGING table (target untouched if this fails)
            _truncate_clickhouse_tables([staging])
            (
                df.write.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", f"{clickhouse.db}.{staging}")
                .option("user", clickhouse.user)
                .option("password", clickhouse.password)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .mode("append")
                .save()
            )
            # Step 2: Atomic swap — either both succeed or neither takes effect
            _clickhouse_query(
                f"EXCHANGE TABLES {clickhouse.db}.{table_name} AND {clickhouse.db}.{staging}"
            )
            log.info("load_clickhouse_marts.done", table=table_name)
        except Exception as exc:
            log.error(
                "load_clickhouse_marts.failed", table=table_name, error=str(exc), exc_info=True
            )
            # Best-effort staging cleanup — target table is unaffected. Guarded
            # so a cleanup failure can't mask the original exception.
            try:
                _clickhouse_query(f"TRUNCATE TABLE IF EXISTS {clickhouse.db}.{staging}")
            except Exception:
                log.warning("load_clickhouse_marts.staging_cleanup_failed", table=staging)
            raise
