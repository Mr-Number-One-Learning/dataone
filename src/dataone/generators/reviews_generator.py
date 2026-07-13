"""
Generates synthetic product review documents — simulates the API source type.

Two ingestion modes, switched via GEN_REVIEWS_INGEST_MODE:
  - "direct" (default): writes straight to MongoDB. Simple, no NiFi dependency.
  - "nifi": POSTs each review to NiFi's ListenHTTP endpoint, which lands it in
    MongoDB via PutMongo — exercises the actual ingestion architecture
    described in docs/NIFI_FLOWS_GUIDE.md — see that guide for the
    flow itself.

Run: python -m dataone.generators.reviews_generator
"""
from __future__ import annotations

import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import pymongo
import requests
from faker import Faker
from pymongo.errors import BulkWriteError

from dataone.config import mongo, nifi
from dataone.generators.domain import fetch_max_id
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

GEN_MESSINESS_RATE = float(os.getenv("GEN_MESSINESS_RATE", "0.0"))

SEED = int(os.getenv("GEN_SEED", "42"))
N_REVIEWS = int(os.getenv("GEN_REVIEWS_TOTAL", "5000"))
INGEST_MODE = os.getenv("GEN_REVIEWS_INGEST_MODE", "direct")
BATCH_SIZE = 1_000
REQUEST_TIMEOUT_SECONDS = 5
HEARTBEAT_EVERY = 500

FALLBACK_MAX_PRODUCT_ID = 200
FALLBACK_MAX_CUSTOMER_ID = 1_000

REVIEW_PHRASES_POSITIVE = [
    "Exceeded my expectations, would buy again.",
    "Great value for the price.",
    "Arrived quickly and works perfectly.",
    "Exactly as described, very happy with it.",
]
REVIEW_PHRASES_NEUTRAL = [
    "It's okay, does the job but nothing special.",
    "Average quality for the price point.",
    "Some minor issues but overall acceptable.",
]
REVIEW_PHRASES_NEGATIVE = [
    "Disappointed, did not match the description.",
    "Stopped working after a few weeks.",
    "Would not recommend, poor quality.",
]

RATING_VALUES = [1, 2, 3, 4, 5]
RATING_WEIGHTS = [0.05, 0.07, 0.13, 0.30, 0.45]

# Probabilities driving the *intentionally* variable document shape — see
# build_review() docstring.
ANONYMOUS_RATIO = 0.10
VERIFIED_FIELD_RATIO = 0.50
IMAGES_FIELD_RATIO = 0.30
VERIFIED_TRUE_RATIO = 0.85


def build_review(faker: Faker, max_product_id: int, max_customer_id: int) -> dict:
    """Builds a synthetic product review dictionary.

    Intentionally variable schema: not every review has the same fields.
    ~10% are anonymous (no customer_id), ~50% include a verified_purchase
    flag, ~30% include an images array. This is deliberate — it demonstrates
    MongoDB's schema flexibility honestly, instead of forcing every document
    into one fixed shape that would make Mongo's choice unjustified.

    Args:
        faker (Faker): The Faker instance for generating text.
        max_product_id (int): The maximum product ID for assignment.
        max_customer_id (int): The maximum customer ID for assignment.

    Returns:
        dict: A dictionary representing the review document.
    """
    rating = random.choices(RATING_VALUES, weights=RATING_WEIGHTS)[0]
    if rating >= 4:
        body = random.choice(REVIEW_PHRASES_POSITIVE)
    elif rating == 3:
        body = random.choice(REVIEW_PHRASES_NEUTRAL)
    else:
        body = random.choice(REVIEW_PHRASES_NEGATIVE)

    review = {
        "review_id": str(uuid.uuid4()),
        "product_id": random.randint(1, max_product_id),
        "rating": rating,
        "title": faker.sentence(nb_words=4).rstrip("."),
        "body": body,
        "submitted_at": (
            datetime.now(timezone.utc) - timedelta(days=random.randint(0, 400))
        ).isoformat(),
    }

    if random.random() < GEN_MESSINESS_RATE:
        noise_type = random.choice(["rating_high", "rating_low", "null_product"])
        if noise_type == "rating_high":
            review["rating"] = random.randint(6, 10)
        elif noise_type == "rating_low":
            review["rating"] = random.randint(-5, 0)
        elif noise_type == "null_product":
            review["product_id"] = None

    if random.random() > ANONYMOUS_RATIO:
        review["customer_id"] = random.randint(1, max_customer_id)

    if random.random() < VERIFIED_FIELD_RATIO:
        review["verified_purchase"] = random.random() < VERIFIED_TRUE_RATIO

    if random.random() < IMAGES_FIELD_RATIO:
        review["images"] = [
            f"https://cdn.dataone.example/reviews/{review['review_id']}/{i}.jpg"
            for i in range(random.randint(1, 3))
        ]

    return review


def _insert_batch(collection, batch: list[dict]) -> tuple[int, int]:
    """Inserts a batch of documents into MongoDB.

    Insert a batch, skipping documents whose review_id already exists.
    ordered=False lets Mongo attempt every document instead of aborting the
    whole batch at the first duplicate — that's what makes rerunning the
    seeder idempotent rather than all-or-nothing.

    Args:
        collection: The pymongo Collection object.
        batch (list[dict]): A list of review documents to insert.

    Returns:
        tuple[int, int]: A tuple containing (number_inserted, number_skipped).
    """
    try:
        collection.insert_many(batch, ordered=False)
        return len(batch), 0
    except BulkWriteError as exc:
        skipped = len(exc.details.get("writeErrors", []))
        return len(batch) - skipped, skipped


def push_to_mongo(n_reviews: int = N_REVIEWS) -> int:
    """Generates and pushes synthetic reviews directly to MongoDB.

    Args:
        n_reviews (int, optional): The number of reviews to generate. Defaults to N_REVIEWS.

    Returns:
        int: The number of reviews successfully inserted.
    """
    max_product_id = fetch_max_id("product_id", "products", FALLBACK_MAX_PRODUCT_ID)
    max_customer_id = fetch_max_id("customer_id", "customers", FALLBACK_MAX_CUSTOMER_ID)

    client = pymongo.MongoClient(mongo.uri)
    try:
        collection = client[mongo.db]["reviews"]
        # Unique index makes reseeding idempotent: a rerun no longer
        # duplicates the whole collection, it just skips existing review_ids.
        collection.create_index("review_id", unique=True)
        faker = Faker()

        batch: list[dict] = []
        written = 0
        skipped = 0
        for _ in range(n_reviews):
            batch.append(build_review(faker, max_product_id, max_customer_id))
            if len(batch) >= BATCH_SIZE:
                inserted, dup = _insert_batch(collection, batch)
                written += inserted
                skipped += dup
                batch = []
        if batch:
            inserted, dup = _insert_batch(collection, batch)
            written += inserted
            skipped += dup
    finally:
        client.close()

    if skipped:
        log.info("push_to_mongo.duplicates_skipped", skipped=skipped)
    log.info("push_to_mongo.done", rows=written, collection="reviews")
    return written


import time
def _post_review(review: dict) -> None:
    """POSTs a single review to NiFi via HTTP.

    Args:
        review (dict): The review document to send.
    """
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            resp = requests.post(nifi.reviews_ingest_url, json=review, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            return
        except requests.RequestException:
            if attempt == max_attempts - 1:
                raise
            time.sleep(2 ** attempt)


def push_via_nifi(n_reviews: int = N_REVIEWS) -> int:
    """Generates and pushes synthetic reviews to NiFi over HTTP.

    Routes reviews through the actual ingestion architecture instead of
    writing to MongoDB directly: one HTTP POST per review to NiFi's
    ListenHTTP processor, which lands each one in MongoDB via PutMongo (see
    docs/NIFI_FLOWS_GUIDE.md). Deliberately slower than push_to_mongo() — one
    request per review is the realistic tradeoff of an API-style ingestion
    path versus a bulk direct write, not an oversight.

    Args:
        n_reviews (int, optional): The number of reviews to generate. Defaults to N_REVIEWS.

    Returns:
        int: The number of reviews successfully pushed.
    """
    max_product_id = fetch_max_id("product_id", "products", FALLBACK_MAX_PRODUCT_ID)
    max_customer_id = fetch_max_id("customer_id", "customers", FALLBACK_MAX_CUSTOMER_ID)
    faker = Faker()

    written = 0
    for _ in range(n_reviews):
        review = build_review(faker, max_product_id, max_customer_id)
        _post_review(review)
        written += 1
        if written % HEARTBEAT_EVERY == 0:
            log.info("push_via_nifi.heartbeat", sent=written)

    log.info("push_via_nifi.done", rows=written, endpoint=nifi.reviews_ingest_url)
    return written


if __name__ == "__main__":
    # Seed Faker too (not just random) — titles come from Faker, so seeding
    # only random would leave review output partially non-deterministic.
    random.seed(SEED)
    Faker.seed(SEED)
    if INGEST_MODE == "nifi":
        push_via_nifi()
    elif INGEST_MODE == "direct":
        push_to_mongo()
    else:
        # Fail loudly on typos: silently falling back to direct-Mongo would
        # mask a misconfigured GEN_REVIEWS_INGEST_MODE and skip the NiFi path
        # someone thought they were exercising.
        raise ValueError(
            f"Unknown GEN_REVIEWS_INGEST_MODE {INGEST_MODE!r}: expected 'direct' or 'nifi'"
        )
