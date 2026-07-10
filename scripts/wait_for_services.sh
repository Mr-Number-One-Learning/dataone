#!/usr/bin/env bash
# Blocks until core services report healthy/reachable — handy before running
# the generators or batch jobs right after `make up`.
set -euo pipefail

# Pick up POSTGRES_USER etc. so the checks match what compose started with.
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

MAX_ATTEMPTS=60   # 60 attempts x 2s = 2 minutes per service, then give up
SLEEP_SECONDS=2

wait_for() {
  local name="$1"
  shift
  local attempt=1
  echo "Waiting for ${name}..."
  until "$@" >/dev/null 2>&1; do
    if (( attempt >= MAX_ATTEMPTS )); then
      echo "ERROR: ${name} did not become ready after $((MAX_ATTEMPTS * SLEEP_SECONDS))s." >&2
      echo "       Check 'docker compose ps' and 'docker compose logs ${name,,}'." >&2
      exit 1
    fi
    attempt=$((attempt + 1))
    sleep "${SLEEP_SECONDS}"
  done
}

wait_for "Postgres" docker exec dataone-postgres pg_isready -U "${POSTGRES_USER:-dataone}"
wait_for "Kafka" docker exec dataone-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
wait_for "ClickHouse" curl -s "http://localhost:8123/ping"

echo "All core services are up."
