#!/usr/bin/env bash
# Starts the on-demand batch worker, submits the nightly ETL job, then stops
# the worker again to reclaim its 2GB.
set -euo pipefail

START_DATE="${1:-}"
END_DATE="${2:-}"

docker compose --profile core --profile batch up -d spark-worker-batch

ARGS=()
if [[ -n "$START_DATE" && -n "$END_DATE" ]]; then
  ARGS=(--start "$START_DATE" --end "$END_DATE")
fi

docker exec dataone-spark-worker-batch \
  /opt/spark/bin/spark-submit /opt/dataone/src/dataone/batch/bronze_to_silver.py "${ARGS[@]}"

# Same profiles as the `up` above — without them compose would not resolve
# the batch-profile service consistently.
docker compose --profile core --profile batch stop spark-worker-batch
