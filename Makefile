.PHONY: up down batch batch-stop run-batch seed stream-clickstream kafka-topics stream-job stream-cdc test test-spark test-iceberg coverage lint fmt logs ps clean query-dead-letters schedule stream-live-orders

# Make exports this to every recipe's shell on all platforms, so
# `python -m dataone....` resolves without an editable install.
export PYTHONPATH=src

# Overridable, e.g. `make run-batch BATCH_EXECUTOR_MEMORY=2000m`
BATCH_CONTAINER ?= dataone-spark-worker-batch
BATCH_DRIVER_MEMORY ?= 512m
BATCH_EXECUTOR_MEMORY ?= 4000m

up:
	docker compose --profile core up -d

down:
	docker compose --profile core --profile batch down

# `batch` actually runs the nightly ETL end-to-end (starts the on-demand
# worker, submits the job, stops the worker) — it is an alias for run-batch.
batch: run-batch

batch-stop:
	docker compose --profile core --profile batch stop spark-worker-batch

run-batch:
	docker compose --profile core --profile batch up -d spark-worker-batch
	docker exec $(BATCH_CONTAINER) /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --deploy-mode client \
	  --driver-memory $(BATCH_DRIVER_MEMORY) \
	  --executor-memory $(BATCH_EXECUTOR_MEMORY) \
	  --total-executor-cores 1 \
	  /opt/dataone/src/dataone/batch/bronze_to_silver.py \
	  $(if $(START_DATE),--start $(START_DATE) --end $(END_DATE),) \
	  $(if $(STAGE),--stage $(STAGE),)
	docker compose --profile core --profile batch stop spark-worker-batch

seed:
	python -m dataone.generators.orders_generator
	python -m dataone.generators.campaign_generator
	python -m dataone.generators.reviews_generator

stream-clickstream:
	python -m dataone.generators.clickstream_generator

kafka-topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic orders-cdc --partitions 1 --replication-factor 1
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic clickstream --partitions 1 --replication-factor 1

stream-job: kafka-topics
	docker compose exec spark-worker-streaming /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --deploy-mode client \
	  --driver-memory 512m \
	  --executor-memory 900m \
	  --total-executor-cores 1 \
	  /opt/dataone/src/dataone/streaming/structured_streaming_job.py

stream-cdc:
	python src/dataone/orchestration/cdc_poll.py

stream-live-orders:
	python -m dataone.generators.live_orders_generator

# realtime: kafka-topics stream-cdc stream-live-orders stream-clickstream

schedule:
	python src/dataone/orchestration/nightly_batch.py

test:
	pytest -v

test-spark:
	pytest -v -m spark

test-iceberg:
	pytest -v -m iceberg

coverage:
	pytest --cov=dataone --cov-report=term-missing

lint:
	ruff check src tests
	black --check src tests

fmt:
	black src tests
	ruff check --fix src tests

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

# Destroys ALL volumes (Postgres, Mongo, ClickHouse, the Iceberg lakehouse...).
# Asks first; skip the prompt with `make clean FORCE=1`.
clean:
	@if [ -z "$(FORCE)" ]; then \
	  printf "This deletes ALL dataone docker volumes (databases + lakehouse). Continue? [y/N] "; \
	  read ans; [ "$$ans" = "y" ] || [ "$$ans" = "Y" ] || { echo "Aborted."; exit 1; }; \
	fi
	docker compose --profile core --profile batch down -v

query-dead-letters:
	docker compose exec spark-worker-streaming /opt/spark/bin/spark-submit --master "local[*]" /opt/dataone/src/dataone/utils/query_dlq.py
