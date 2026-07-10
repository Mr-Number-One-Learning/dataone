"""
Centralized configuration, loaded from environment variables (see .env.example).

Every module in the package should import its settings from here rather than
calling os.environ directly — keeps configuration discoverable in one place.
"""
from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

# .env ships Docker-network service names (postgres, mongodb, kafka, ...)
# because that's what code running INSIDE a container — e.g. the Spark jobs
# in streaming/ and batch/ — needs to resolve those services by their
# docker-compose service name. Code running on the HOST instead (the
# generator scripts invoked via `make seed`, or the CDC simulator run
# directly) can't resolve those names at all. Rather than maintain two
# separate .env files, auto-translate to localhost when not running inside a
# container — every service's port is already published to the host in
# docker-compose.yml, so localhost just works. An explicit override (anything
# other than the shipped service name) is left untouched.
_DOCKER_INTERNAL_HOSTNAMES = {"postgres", "mongodb", "kafka", "clickhouse", "nifi"}

# Host-published ports from docker-compose.yml, named here so the in-container
# -> host translation below isn't a scatter of bare literals. If a published
# port changes in docker-compose.yml, these are the only values to update.
_POSTGRES_CONTAINER_PORT = 5432
_POSTGRES_HOST_PORT = 5445
_KAFKA_CONTAINER_PORT = "29092"
_KAFKA_HOST_PORT = "9092"


def _in_docker() -> bool:
    """Detects whether the code is running inside a Docker container.

    Returns:
        bool: True if running inside Docker (checked via /.dockerenv), False otherwise.
    """
    # /.dockerenv is created by Docker's runtime; good enough for this
    # project since all containers here are plain Docker (docker-compose).
    # If detection ever misfires, set explicit hosts in .env — any value
    # other than the shipped service names bypasses translation entirely.
    return pathlib.Path("/.dockerenv").exists()


def _resolve_host(raw_host: str) -> str:
    """Translates Docker service names to localhost when running outside Docker.

    Args:
        raw_host (str): The raw hostname from the environment variables.

    Returns:
        str: 'localhost' if running on host and raw_host is a Docker internal hostname,
            otherwise returns raw_host unchanged.
    """
    if not _in_docker() and raw_host in _DOCKER_INTERNAL_HOSTNAMES:
        return "localhost"
    return raw_host


def _resolve_postgres_port(raw_host: str, raw_port: int) -> int:
    """Translates the Postgres port when running outside Docker.

    Args:
        raw_host (str): The raw postgres hostname.
        raw_port (int): The raw postgres port.

    Returns:
        int: The mapped host port if running outside Docker, else the raw_port.
    """
    if not _in_docker() and raw_host == "postgres" and raw_port == _POSTGRES_CONTAINER_PORT:
        return _POSTGRES_HOST_PORT
    return raw_port


def _resolve_kafka_port(raw_host: str, raw_port: str) -> str:
    """Translates the Kafka port when running outside Docker.

    Args:
        raw_host (str): The raw kafka hostname.
        raw_port (str): The raw kafka port.

    Returns:
        str: The mapped host port if running outside Docker, else the raw_port.
    """
    if not _in_docker() and raw_host == "kafka" and raw_port == _KAFKA_CONTAINER_PORT:
        return _KAFKA_HOST_PORT
    return raw_port


def _resolve_bootstrap_servers(raw: str) -> str:
    """Translates Kafka's comma-separated list of host:port pairs.

    Same host translation as _resolve_host, but for Kafka's comma-separated
    list of host:port pairs.

    Args:
        raw (str): The raw bootstrap servers string.

    Returns:
        str: The resolved bootstrap servers string.
    """
    parts = []
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            host, port = entry.rsplit(":", 1)
            parts.append(f"{_resolve_host(host)}:{_resolve_kafka_port(host, port)}")
        else:
            parts.append(_resolve_host(entry))
    return ",".join(parts)


@dataclass(frozen=True)
class PostgresConfig:
    host: str = _resolve_host(os.getenv("POSTGRES_HOST", "localhost"))
    port: int = _resolve_postgres_port(
        os.getenv("POSTGRES_HOST", "postgres"),
        int(os.getenv("POSTGRES_PORT", "5432"))
    )
    db: str = os.getenv("POSTGRES_DB", "dataone")
    user: str = os.getenv("POSTGRES_USER", "dataone")
    password: str = os.getenv("POSTGRES_PASSWORD", "changeme")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


@dataclass(frozen=True)
class MongoConfig:
    host: str = _resolve_host(os.getenv("MONGO_HOST", "localhost"))
    port: int = int(os.getenv("MONGO_PORT", "27017"))
    db: str = os.getenv("MONGO_DB", "dataone")
    user: str = os.getenv("MONGO_USER", "dataone")
    password: str = os.getenv("MONGO_PASSWORD", "changeme")

    @property
    def uri(self) -> str:
        return f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}"


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = _resolve_bootstrap_servers(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_orders_cdc: str = os.getenv("KAFKA_TOPIC_ORDERS_CDC", "orders-cdc")
    topic_clickstream: str = os.getenv("KAFKA_TOPIC_CLICKSTREAM", "clickstream")
    topic_anomaly_alerts: str = os.getenv("KAFKA_TOPIC_ANOMALY_ALERTS", "anomaly-alerts")


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str = _resolve_host(os.getenv("CLICKHOUSE_HOST", "localhost"))
    port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    db: str = os.getenv("CLICKHOUSE_DB", "dataone_marts")
    user: str = os.getenv("CLICKHOUSE_USER", "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "changeme")


@dataclass(frozen=True)
class NiFiConfig:
    host: str = _resolve_host(os.getenv("NIFI_HOST", "localhost"))
    api_port: int = int(os.getenv("NIFI_PORT", "8443"))
    username: str = os.getenv("NIFI_USERNAME", "admin")
    password: str = os.getenv("NIFI_PASSWORD", "changeme1234")
    # Separate from api_port (NiFi's own HTTPS UI/API) — this is the
    # plain-HTTP port a ListenHTTP processor listens on for the reviews
    # ingestion flow. See docs/NIFI_FLOWS_GUIDE.md.
    reviews_listen_port: int = int(os.getenv("NIFI_REVIEWS_LISTEN_PORT", "9900"))

    @property
    def reviews_ingest_url(self) -> str:
        return f"http://{self.host}:{self.reviews_listen_port}/reviews"


@dataclass(frozen=True)
class IcebergConfig:
    warehouse_path: str = os.getenv("ICEBERG_WAREHOUSE_PATH", "/data/lakehouse")
    catalog_name: str = os.getenv("ICEBERG_CATALOG_NAME", "dataone_catalog")


postgres = PostgresConfig()
mongo = MongoConfig()
kafka = KafkaConfig()
clickhouse = ClickHouseConfig()
nifi = NiFiConfig()
iceberg = IcebergConfig()
