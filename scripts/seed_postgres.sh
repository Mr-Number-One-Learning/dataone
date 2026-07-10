#!/usr/bin/env bash
# Convenience wrapper around `make seed` that waits for services first.
# Same generator set and ordering as the `seed` Makefile target.
set -euo pipefail

# src layout: the dataone package lives under src/, so `python -m dataone....`
# needs it on the path (mirrors `export PYTHONPATH=src` in the Makefile).
export PYTHONPATH=src

./scripts/wait_for_services.sh
python -m dataone.generators.orders_generator
python -m dataone.generators.campaign_generator
python -m dataone.generators.reviews_generator
# NOTE: the clickstream generator is deliberately NOT started here — it runs
# continuously and would be orphaned by a backgrounded `&`. Start it in its
# own terminal via `make stream-clickstream` when you want live events.
