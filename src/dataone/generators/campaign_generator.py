"""
Generates synthetic marketing campaign CSV files into data/raw/campaigns/ for
NiFi to pick up — simulates the batch file-drop source type.

Run: python -m dataone.generators.campaign_generator
"""
from __future__ import annotations

import csv
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dataone.generators.domain import N_CAMPAIGNS
from dataone.utils.logging_config import get_logger

log = get_logger(__name__)

OUTPUT_DIR = Path("data/raw/campaigns")
SEED = int(os.getenv("GEN_SEED", "42"))
GEN_MESSINESS_RATE = float(os.getenv("GEN_MESSINESS_RATE", "0.0"))

CHANNELS = ["email", "social_media", "search_ads", "display_ads", "influencer", "affiliate"]
CAMPAIGN_THEMES = [
    "Summer Sale", "Flash Friday", "New Arrivals", "Loyalty Rewards", "Clearance",
    "Back to School", "Holiday Special", "Member Exclusive", "Free Shipping Weekend",
    "Category Spotlight",
]
FIELDNAMES = ["campaign_id", "name", "channel", "start_date", "end_date",
              "budget", "spend", "clicks", "conversions"]


def build_campaign_row(campaign_id: int, now: datetime | None = None) -> dict:
    """Builds a single synthetic marketing campaign row.

    Args:
        campaign_id (int): The assigned campaign ID.
        now (datetime | None, optional): The current UTC datetime for bounding 
            campaign dates. Defaults to None.

    Returns:
        dict: A dictionary representing the campaign row.
    """
    now = now or datetime.now(timezone.utc)
    start_date = now - timedelta(days=random.randint(0, 365))
    duration_days = random.randint(3, 21)
    end_date = start_date + timedelta(days=duration_days)

    budget = round(random.uniform(500, 25_000), 2)
    # Spend tracks budget but isn't an exact mirror — under/over-spend happens
    # in real campaigns, and a downstream join purely on equality would be a
    # quiet bug, which is the point of generating it this way.
    spend = round(budget * random.uniform(0.70, 1.05), 2)
    clicks = random.randint(200, 50_000)
    conversions = int(clicks * random.uniform(0.01, 0.08))

    # Inject deliberate noise to test the Quarantine Layer
    if random.random() < GEN_MESSINESS_RATE:
        noise_type = random.choice(["negative_budget", "negative_spend", "null_id"])
        if noise_type == "negative_budget":
            budget = -budget
        elif noise_type == "negative_spend":
            spend = -spend
        elif noise_type == "null_id":
            campaign_id = None

    return {
        "campaign_id": campaign_id,
        "name": f"{random.choice(CAMPAIGN_THEMES)} {start_date.year}",
        "channel": random.choice(CHANNELS),
        "start_date": start_date.date().isoformat(),
        "end_date": end_date.date().isoformat(),
        "budget": budget,
        "spend": spend,
        "clicks": clicks,
        "conversions": conversions,
    }


def generate_campaign_file(
    n_campaigns: int = N_CAMPAIGNS,
    output_dir: Path = OUTPUT_DIR,
    seed: int | None = SEED,
) -> Path:
    """Generates a CSV file containing synthetic marketing campaigns.

    Seed here, not only under __main__, so programmatic callers get the
    deterministic output this generator documents. n_campaigns defaults to
    the shared N_CAMPAIGNS because orders_generator attributes orders to
    campaign IDs 1..N_CAMPAIGNS — see generators/domain.py.

    Args:
        n_campaigns (int, optional): The number of campaigns to generate. 
            Defaults to N_CAMPAIGNS.
        output_dir (Path, optional): The directory to write the CSV file to. 
            Defaults to OUTPUT_DIR.
        seed (int | None, optional): The random seed for deterministic generation. 
            Defaults to SEED.

    Returns:
        Path: The path to the generated CSV file.
    """
    if seed is not None:
        random.seed(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"campaigns_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.csv"

    with filename.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for campaign_id in range(1, n_campaigns + 1):
            writer.writerow(build_campaign_row(campaign_id))

    log.info("generate_campaign_file.done", path=str(filename), rows=n_campaigns)
    return filename


if __name__ == "__main__":
    generate_campaign_file()
