"""
Shared Kafka producer factory used by the CDC simulator and the clickstream
generator. Centralized so retry/serialization config only lives in one place.

Sends are batched (linger_ms / batch_size) rather than flushed per message —
callers that need delivery guarantees at a boundary (end of a poll cycle,
shutdown) call flush_producer() once per batch instead.
"""
from __future__ import annotations

import atexit
import json
from functools import lru_cache

from kafka import KafkaProducer

from dataone.config import kafka
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

# Small linger + a decent batch size lets the client coalesce sends into far
# fewer network round-trips; per-message flush was the throughput bottleneck
# for the CDC + clickstream volume (roadmap Day 7-8 load test).
LINGER_MS = 50
BATCH_SIZE_BYTES = 64 * 1024


@lru_cache(maxsize=1)
def get_producer() -> KafkaProducer:
    """Gets or creates the shared KafkaProducer instance.

    Returns:
        KafkaProducer: The configured Kafka producer.
    """
    return KafkaProducer(
        bootstrap_servers=kafka.bootstrap_servers,
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=LINGER_MS,
        batch_size=BATCH_SIZE_BYTES,
    )


def _on_send_error(exc: BaseException) -> None:
    """Errback for the async send future.

    Without it, delivery failures on the background I/O thread would vanish 
    silently now that we no longer flush (and therefore no longer surface errors) 
    on every send.

    Args:
        exc (BaseException): The exception raised during delivery.
    """
    log.error("kafka_producer.delivery_failed", error=str(exc))


def send(topic: str, value: dict | bytes, key: str | None = None) -> None:
    """Sends a message asynchronously to Kafka.

    Non-blocking: attaches an errback instead of waiting on the future, so
    throughput stays high but failed deliveries still get logged.

    Args:
        topic (str): The destination Kafka topic.
        value (dict | bytes): The message payload.
        key (str | None, optional): An optional partition key. Defaults to None.
    """
    producer = get_producer()
    future = producer.send(topic, value=value, key=key.encode("utf-8") if key else None)
    # Non-blocking: attach an errback instead of waiting on the future, so
    # throughput stays high but failed deliveries still get logged.
    future.add_errback(_on_send_error)


def flush_producer() -> None:
    """Flushes pending messages if a producer exists.

    Callers invoke this once per batch / at shutdown rather than per message. 
    Guarded on the lru_cache so a flush never *creates* a producer (e.g., in 
    unit tests that stub out send()).
    """
    if get_producer.cache_info().currsize:
        get_producer().flush()


def _close_producer_at_exit() -> None:
    """Flushes and closes the producer on process exit.

    Drain anything still buffered by linger_ms before the process dies —
    otherwise the last partial batch would be lost on a clean exit.
    """
    if get_producer.cache_info().currsize:
        producer = get_producer()
        producer.flush()
        producer.close()


atexit.register(_close_producer_at_exit)
