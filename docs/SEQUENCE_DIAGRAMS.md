# DataOne sequence diagrams (Mermaid)

Derived from `src/dataone/orchestration/nightly_batch.py`, `cdc_poll.py`,
`streaming/structured_streaming_job.py` and `batch/bronze_to_silver.py`.
Details not visible in those files (exact NiFi processors, topic names) are kept generic.

## 1. Nightly batch (Prefect → Spark → Iceberg → ClickHouse)

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Prefect schedule (0 2 * * *)
    participant Flow as Prefect flow: nightly-batch
    participant Make as make run-batch (STAGE=...)
    participant Spark as Spark job (bronze_to_silver)
    participant PG as Postgres (OLTP + _pipeline_runs)
    participant Mongo as MongoDB (reviews)
    participant Ice as Iceberg lakehouse (MinIO)
    participant CH as ClickHouse
    participant OL as OpenLineage / Marquez

    Cron->>Flow: trigger
    Flow->>Flow: generate flow_run_id
    loop for each stage: ingest_bronze, standardize_silver, model_gold, sync_clickhouse
        Flow->>Make: subprocess (env: STAGE, PARENT_RUN_ID, PARENT_JOB_NAME)
        Make->>Spark: spark-submit
        Spark->>OL: START event (parent run facet)
        alt ingest_bronze
            Spark->>Spark: read NiFi-staged campaign files
            Spark->>PG: read products, order_items
            Spark->>Mongo: read reviews
            Spark->>Ice: write bronze tables
        else standardize_silver
            Spark->>Ice: read bronze (CDC, reviews, ...)
            Spark->>Spark: quality gate (validators)
            Spark->>Ice: MERGE / overwritePartitions into silver
            Spark->>Ice: failing rows to quarantine
        else model_gold
            Spark->>Ice: read silver
            Spark->>Ice: write gold facts, dims, aggregates
            Spark->>Ice: write quality_gate_summary, quarantine_summary
        else sync_clickhouse
            Spark->>Ice: read gold
            Spark->>CH: TRUNCATE <table>_staging
            Spark->>CH: JDBC append into staging
            Spark->>CH: EXCHANGE TABLES target AND staging
        end
        Spark->>PG: record run in _pipeline_runs
        Spark->>OL: COMPLETE / FAIL event
        Spark-->>Make: exit code
        Make-->>Flow: exit code
        Note over Flow: non-zero exit raises BatchJobFailed, task retries 3x (10s, 30s, 60s)
    end
    Flow->>PG: query _pipeline_runs since flow start
    Flow->>Flow: publish Markdown artifact "nightly-etl-summary" (runs in finally)
```

## 2. Real-time path (CDC and clickstream via Kafka and Structured Streaming)

```mermaid
sequenceDiagram
    autonumber
    participant Poll as Prefect flow: cdc-poll (every 5s)
    participant PG as Postgres (customers, orders)
    participant Gen as Clickstream producer
    participant K as Kafka
    participant SS as Spark Structured Streaming
    participant Ice as Iceberg bronze
    participant CH as ClickHouse
    participant G as Grafana

    Poll->>PG: ensure watermark table
    loop each table (customers, orders)
        Poll->>PG: poll_once: rows newer than watermark
        Poll->>K: publish CDC events
        Poll->>PG: advance watermark
    end
    Gen->>K: publish clickstream events

    SS->>K: subscribe (orders CDC, clickstream)
    SS->>SS: parse_with_dead_letter against schema
    alt valid event
        SS->>Ice: append to bronze orders_cdc / clickstream (trigger 30s)
    else malformed event
        SS->>Ice: append to bronze.dead_letters
    end
    SS->>SS: live activity aggregate
    SS->>CH: write aggregate per micro-batch (trigger 1 min)
    SS->>SS: anomaly detector on clickstream
    SS->>K: publish to anomaly alerts topic
    CH->>K: Kafka-engine table consumes orders CDC
    CH->>CH: materialized views (orders/min, revenue estimate, status counts)
    G->>CH: query live dashboards
```

## 3. Review ingestion (NiFi → MongoDB → batch)

```mermaid
sequenceDiagram
    autonumber
    participant Gen as Reviews generator
    participant NiFi as Apache NiFi (ListenHTTP)
    participant Mongo as MongoDB
    participant Spark as Spark ingest_bronze
    participant Ice as Iceberg bronze

    Gen->>NiFi: HTTP POST /reviews (JSON)
    NiFi->>Mongo: PutMongo (insert)
    Note over NiFi,Mongo: replays can create duplicates, removed later by review_id dedupe in silver
    Spark->>Mongo: read reviews (nightly)
    Spark->>Ice: write bronze reviews
```

## 4. Backfill

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator
    participant Flow as Prefect flow: backfill(start, end)
    participant Make as make run-batch
    participant Spark as Spark job
    participant PG as Postgres _pipeline_runs

    Op->>Flow: call backfill(start, end) (not served by make schedule)
    loop ingest_bronze, standardize_silver, model_gold, sync_clickhouse
        Flow->>Make: env START_DATE, END_DATE, STAGE
        Make->>Spark: spark-submit for the date window
        Note over Spark: overwritePartitions makes reruns idempotent
    end
    Flow->>PG: read run history
    Flow->>Flow: publish run summary artifact
```

## 5. Data quality and quarantine (inside standardize_silver)

```mermaid
sequenceDiagram
    autonumber
    participant Spark as bronze_to_silver
    participant V as quality/validators.py
    participant Ice as Iceberg
    participant Gold as gold summary tables
    participant G as Grafana alerts

    Spark->>Ice: read bronze dataset
    Spark->>V: run_quality_gate(df, contract rules)
    V-->>Spark: passed_df, quarantined_df, counts
    Spark->>Ice: write passed rows to silver
    Spark->>Ice: write rejected rows to quarantine
    Spark->>Gold: build quality_gate_summary and quarantine_summary
    G->>Gold: read via ClickHouse, evaluate alert rules
```
