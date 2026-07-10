# Extends the official apache/spark image (NOT bitnami/spark — Bitnami moved
# its container images behind a paid subscription in late 2025) with the
# JARs our jobs actually need on
# the classpath: Iceberg's Spark runtime, the Postgres JDBC driver (used by
# Iceberg's JDBC catalog, backed by Postgres), the ClickHouse JDBC driver
# (used to load curated aggregates into the marts that back Grafana), and the
# Spark-Kafka connector (spark-sql-kafka + kafka-clients) baked in so the
# streaming job never has to download from Maven at startup.
#
# Versions pinned below were verified against Maven Central as current at
# build-time of this Dockerfile (mid-2026) — bump deliberately, not blindly,
# since Iceberg only maintains runtime jars for actively-supported Spark
# versions and these three artifacts need to stay mutually compatible.
FROM apache/spark:3.5.1-python3

ARG ICEBERG_VERSION=1.9.1
ARG POSTGRES_JDBC_VERSION=42.7.11
ARG CLICKHOUSE_JDBC_VERSION=0.9.8
ARG SPARK_KAFKA_VERSION=3.5.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG MONGO_SPARK_VERSION=10.3.0
ARG OPENLINEAGE_VERSION=1.13.1

# Simplification, not an oversight: this image is for local bootcamp/dev use
# on a single laptop, not a hardened multi-tenant deployment. Running as root
# avoids non-root volume-permission friction with the bind-mounted ./src and
# the lakehouse-data named volume. Revisit if this ever needs to run anywhere
# more sensitive than localhost.
USER root

RUN curl -fsSL -o "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
 && curl -fsSL -o "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
 && curl -fsSL -o "/opt/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
      "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
 && curl -fsSL -o "/opt/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
 && curl -fsSL -o "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
 && curl -fsSL -o "/opt/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
 && curl -fsSL -o "/opt/spark/jars/commons-pool2-2.12.0.jar" \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" \
 && curl -fsSL -o "/opt/spark/jars/mongo-spark-connector_2.12-${MONGO_SPARK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/${MONGO_SPARK_VERSION}/mongo-spark-connector_2.12-${MONGO_SPARK_VERSION}-all.jar" \
 && curl -fsSL -o "/opt/spark/jars/openlineage-spark_2.12-${OPENLINEAGE_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OPENLINEAGE_VERSION}/openlineage-spark_2.12-${OPENLINEAGE_VERSION}.jar"

# Python-side dependencies used by the job scripts themselves (pyspark itself
# already ships in the apache/spark:*-python3 base image — don't reinstall it).
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt
RUN python3 -m textblob.download_corpora
