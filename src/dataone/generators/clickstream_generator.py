"""
Synthetic clickstream event generator: continuously emits page-view / add-to-cart /
checkout events directly onto the Kafka 'clickstream' topic, at a configurable rate.

Run: python -m dataone.generators.clickstream_generator
"""
from __future__ import annotations

import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from dataone.config import kafka
from dataone.generators.domain import fetch_max_id
from dataone.ingestion.kafka_producers import flush_producer, send
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

GEN_MESSINESS_RATE = float(os.getenv("GEN_MESSINESS_RATE", "0.0"))

EVENTS_PER_SEC = int(os.getenv("GEN_CLICKSTREAM_EVENTS_PER_SEC", "200"))
SEED = int(os.getenv("GEN_SEED", "42"))

EVENT_TYPES = [
    "page_view", "add_to_cart", "remove_from_cart", "checkout_start", "checkout_complete",
]
EVENT_TYPE_WEIGHTS = [0.70, 0.15, 0.05, 0.06, 0.04]

# Fraction of events tied to a known (logged-in) customer rather than an
# anonymous guest. Deliberately heterogeneous — not every event carries a
# customer_id — which is realistic for clickstream and exercises nullable-
# field handling downstream in the streaming job and DQ gate.
LOGGED_IN_RATIO = 0.4

# Intentional data-quality noise, NOT bugs: the streaming job's dedup and the
# DQ gate need something to catch, so a small fraction of events are emitted
# twice (same event_id) or as malformed non-JSON bytes.
DUPLICATE_EVENT_RATE = 0.01
MALFORMED_EVENT_RATE = 0.01

HEARTBEAT_EVERY = 1_000

# Fallback ID ranges so this can run standalone for a quick demo even before
# orders_generator has seeded Postgres.
FALLBACK_MAX_CUSTOMER_ID = 1_000
FALLBACK_MAX_PRODUCT_ID = 200


def build_event(
    max_customer_id: int, max_product_id: int, session_id: str, logged_in: bool
) -> dict:
    """Builds a synthetic clickstream event.

    Args:
        max_customer_id (int): The maximum customer ID for logged-in users.
        max_product_id (int): The maximum product ID for random product assignment.
        session_id (str): The session ID for the event.
        logged_in (bool): Whether the event should include a customer ID.

    Returns:
        dict: A dictionary representing the event payload.
    """
    event = {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS)[0],
        "product_id": random.randint(1, max_product_id),
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if logged_in:
        event["customer_id"] = random.randint(1, max_customer_id)

    if random.random() < GEN_MESSINESS_RATE:
        noise_type = random.choice(["invalid_event_type", "null_session"])
        if noise_type == "invalid_event_type":
            event["event_type"] = random.choice(["search_clicked", "scroll", "hover", "abandoned_tab"])
        elif noise_type == "null_session":
            event["session_id"] = None

    return event


def _install_sigterm_handler() -> None:
    """Installs a SIGTERM handler to ensure graceful shutdown.

    Translate SIGTERM (docker stop, kill) into KeyboardInterrupt so both
    Ctrl-C and an orchestrated stop take the same graceful-shutdown path:
    flush the producer's buffered batch and log a summary before exiting.
    """

    def _raise_keyboard_interrupt(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)


def run(events_per_sec: int = EVENTS_PER_SEC) -> None:
    """Main loop for the clickstream generator.

    Args:
        events_per_sec (int, optional): The target generation rate in events per second. 
            Defaults to EVENTS_PER_SEC.
    """
    max_customer_id = fetch_max_id("customer_id", "customers", FALLBACK_MAX_CUSTOMER_ID)
    max_product_id = fetch_max_id("product_id", "products", FALLBACK_MAX_PRODUCT_ID)
    log.info(
        "clickstream_generator.run",
        events_per_sec=events_per_sec,
        max_customer_id=max_customer_id,
        max_product_id=max_product_id,
    )
    interval = 1.0 / events_per_sec if events_per_sec > 0 else 0

    _install_sigterm_handler()

    session_id = str(uuid.uuid4())
    session_event_budget = random.randint(3, 15)
    sent = 0

    try:
        while True:
            logged_in = random.random() < LOGGED_IN_RATIO
            event = build_event(max_customer_id, max_product_id, session_id, logged_in)

            rand_val = random.random()
            if rand_val < DUPLICATE_EVENT_RATE:
                # Same event_id twice — downstream dedup test noise.
                send(kafka.topic_clickstream, event, key=session_id)
                send(kafka.topic_clickstream, event, key=session_id)
                sent += 2
            elif rand_val < DUPLICATE_EVENT_RATE + MALFORMED_EVENT_RATE:
                # Deliberately broken payload — DQ gate test noise.
                send(kafka.topic_clickstream, b"{this is not valid json", key=session_id)
                sent += 1
            else:
                send(kafka.topic_clickstream, event, key=session_id)
                sent += 1

            session_event_budget -= 1
            if session_event_budget <= 0:
                session_id = str(uuid.uuid4())
                session_event_budget = random.randint(3, 15)

            if sent % HEARTBEAT_EVERY == 0:
                # Sends are batched (linger_ms) rather than flushed per
                # message; flushing at each heartbeat bounds how much a
                # crash could leave sitting in the client buffer.
                flush_producer()
                log.info("clickstream_generator.heartbeat", sent=sent)

            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        # Graceful shutdown: drain the producer buffer so the tail of the
        # stream isn't lost, then summarize what this run produced.
        flush_producer()
        log.info("clickstream_generator.shutdown", events_sent=sent)


if __name__ == "__main__":
    random.seed(SEED)
    run()
