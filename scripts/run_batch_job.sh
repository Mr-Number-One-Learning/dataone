#!/usr/bin/env bash
# Thin wrapper kept for convenience: delegates to `make run-batch` so the
# spark-submit flags (master, memory, stage) live in exactly one place.
#
# Usage: scripts/run_batch_job.sh [START_DATE END_DATE]
set -euo pipefail

cd "$(dirname "$0")/.."

START_DATE="${1:-}"
END_DATE="${2:-}"

exec make run-batch ${START_DATE:+START_DATE="$START_DATE" END_DATE="$END_DATE"}
