"""
Wiring tests for the stage entrypoints in dataone.batch.bronze_to_silver.

No Spark, Iceberg or Kafka needed: every collaborator is replaced by a mock, so
these check *which* steps run, in what order, and what gets passed between them
(the part that was duplicated across the staged and full-pipeline paths).
"""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock

import pytest

from dataone.batch import bronze_to_silver as b


class FakeTracker:
    instances: list[FakeTracker] = []

    def __init__(self, job_name, start=None, end=None):
        self.job_name = job_name
        self.inputs: list[str] = []
        self.outputs: list[tuple] = []
        FakeTracker.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def add_input(self, name):
        self.inputs.append(name)

    def add_output(self, name, **kwargs):
        self.outputs.append((name, kwargs))


@pytest.fixture(autouse=True)
def fake_lineage(monkeypatch):
    FakeTracker.instances = []
    monkeypatch.setattr("dataone.lineage.tracker.LineageTracker", FakeTracker)


@pytest.fixture
def calls(monkeypatch):
    """Replaces every helper with a mock that records call order."""
    order: list[str] = []
    spark = MagicMock(name="spark")
    session = MagicMock()
    session.__enter__.return_value = spark
    session.__exit__.return_value = False
    monkeypatch.setattr(b, "_stage_session", lambda job: session)
    monkeypatch.setattr(b, "F", MagicMock(name="F"))  # F.col needs a live SparkContext

    def rec(name, ret=None):
        def _fn(*a, **k):
            order.append(name)
            return ret

        mock = MagicMock(side_effect=_fn)
        mock.ret = ret  # what the stub returns (side_effect wins over return_value)
        monkeypatch.setattr(b, name, mock)
        return mock

    def quality(n):
        return MagicMock(name=f"quality_{n}", passed_df=MagicMock(name=f"passed_{n}"))

    def cached(n):
        return MagicMock(name=f"cached_{n}")

    mocks = {
        "ingest_campaigns_to_bronze": rec("ingest_campaigns_to_bronze", 1),
        "ingest_reviews_to_bronze": rec("ingest_reviews_to_bronze", 2),
        "ingest_products_to_bronze": rec("ingest_products_to_bronze", 3),
        "ingest_order_items_to_bronze": rec("ingest_order_items_to_bronze", 4),
        "read_bronze_tables": rec(
            "read_bronze_tables",
            {
                "campaigns": MagicMock(),
                "cdc": MagicMock(),
                "reviews": MagicMock(),
                "clickstream": MagicMock(),
            },
        ),
        "standardize_campaigns": rec("standardize_campaigns", quality("campaigns")),
        "standardize_customers": rec("standardize_customers", quality("customers")),
        "standardize_orders": rec("standardize_orders", quality("orders")),
        "standardize_reviews": rec("standardize_reviews", (quality("reviews"), cached("reviews"))),
        "standardize_clickstream": rec(
            "standardize_clickstream", (quality("clickstream"), cached("clickstream"))
        ),
        "standardize_products": rec("standardize_products", quality("products")),
        "standardize_order_items": rec("standardize_order_items", quality("order_items")),
        "apply_scd2_merge": rec("apply_scd2_merge"),
        "build_and_write_fact_order_items": rec(
            "build_and_write_fact_order_items", (cached("fact"), quality("fact"))
        ),
        "build_and_write_gold_marts": rec("build_and_write_gold_marts"),
        "sync_gold_to_clickhouse": rec("sync_gold_to_clickhouse"),
        "_read_silver": rec("_read_silver", MagicMock(name="silver_df")),
        "_latest_per_key": rec("_latest_per_key", MagicMock(name="latest_campaigns")),
    }
    return order, mocks, spark


ARGS = argparse.Namespace(start=None, end=None, stage=None)


@pytest.mark.parametrize(
    "stage, fn",
    [
        ("ingest_bronze", "run_ingest_bronze"),
        ("standardize_silver", "run_standardize_silver"),
        ("model_gold", "run_model_gold"),
        ("sync_clickhouse", "run_sync_clickhouse"),
    ],
)
def test_main_dispatches_each_stage(monkeypatch, stage, fn):
    stub = MagicMock()
    monkeypatch.setitem(b._STAGES, stage, stub)
    full = MagicMock()
    monkeypatch.setattr(b, "run_full_pipeline", full)
    monkeypatch.setattr("sys.argv", ["bronze_to_silver.py", "--stage", stage])

    b.main()

    stub.assert_called_once()
    full.assert_not_called()


def test_main_without_stage_runs_full_pipeline(monkeypatch):
    full = MagicMock()
    monkeypatch.setattr(b, "run_full_pipeline", full)
    monkeypatch.setattr("sys.argv", ["bronze_to_silver.py"])

    b.main()

    full.assert_called_once()


def test_stage_session_stops_spark_and_reraises(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr(b, "build_spark_session", lambda name: spark)
    monkeypatch.setattr(b, "bootstrap_lakehouse", lambda s: None)

    with pytest.raises(RuntimeError, match="boom"):
        with b._stage_session("job"):
            raise RuntimeError("boom")

    spark.stop.assert_called_once()


def test_stage_session_stops_spark_on_success(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr(b, "build_spark_session", lambda name: spark)
    monkeypatch.setattr(b, "bootstrap_lakehouse", lambda s: None)

    with b._stage_session("job") as got:
        assert got is spark

    spark.stop.assert_called_once()


def test_standardize_silver_stage_unpersists_reviews_and_clickstream(calls):
    order, mocks, _ = calls

    b.run_standardize_silver(ARGS)

    # Cached frames returned by the helpers are released by the stage.
    mocks["standardize_reviews"].ret[1].unpersist.assert_called_once()
    mocks["standardize_clickstream"].ret[1].unpersist.assert_called_once()
    assert order == [
        "read_bronze_tables",
        "standardize_campaigns",
        "standardize_customers",
        "standardize_orders",
        "standardize_reviews",
        "standardize_clickstream",
        "standardize_products",
        "standardize_order_items",
    ]
    assert FakeTracker.instances[0].job_name == "bronze_to_silver.standardize_silver"
    assert FakeTracker.instances[0].inputs[0] == "bronze.orders_cdc"


def test_model_gold_stage_passes_no_inmemory_silver_quality(calls):
    order, mocks, _ = calls

    b.run_model_gold(ARGS)

    marts_args = mocks["build_and_write_gold_marts"].call_args.args
    quality_by_table = marts_args[-1]
    assert quality_by_table["fact_order_items"] is not None
    assert all(
        quality_by_table[t] is None
        for t in ("campaigns", "customers", "orders", "products", "reviews", "clickstream")
    )
    # campaigns come from the latest-per-key silver table, not a passed_df
    assert marts_args[-2] is mocks["_latest_per_key"].ret
    assert order.index("apply_scd2_merge") < order.index("build_and_write_fact_order_items")
    assert order.index("build_and_write_fact_order_items") < order.index(
        "build_and_write_gold_marts"
    )


def test_full_pipeline_runs_everything_once_in_order(calls):
    order, mocks, _ = calls

    b.run_full_pipeline(ARGS)

    stages = [n for n in order if not n.startswith("_")]
    assert stages == [
        "ingest_campaigns_to_bronze",
        "ingest_reviews_to_bronze",
        "ingest_products_to_bronze",
        "ingest_order_items_to_bronze",
        "read_bronze_tables",
        "standardize_campaigns",
        "standardize_customers",
        "standardize_orders",
        "apply_scd2_merge",
        "standardize_products",
        "standardize_order_items",
        "build_and_write_fact_order_items",
        "standardize_reviews",
        "standardize_clickstream",
        "build_and_write_gold_marts",
        "sync_gold_to_clickhouse",
    ]
    assert FakeTracker.instances[0].job_name == "bronze_to_silver"


def test_full_pipeline_feeds_real_quality_results_and_passed_campaigns(calls):
    _, mocks, _ = calls

    b.run_full_pipeline(ARGS)

    marts_args = mocks["build_and_write_gold_marts"].call_args.args
    quality_by_table = marts_args[-1]
    campaigns_quality = mocks["standardize_campaigns"].ret
    assert quality_by_table["campaigns"] is campaigns_quality
    assert all(v is not None for v in quality_by_table.values())
    # campaigns for the marts are the quality-gated (passed) frame
    assert marts_args[-2] is campaigns_quality.passed_df


def test_gold_table_names_cover_every_mart_written():
    # sync_clickhouse mirrors exactly: the 13 marts built by
    # build_and_write_gold_marts + fact_order_items + dim_date.
    assert len(b.GOLD_TABLE_NAMES) == 15
    assert b.GOLD_TABLE_NAMES[-2:] == ["fact_order_items", "dim_date"]
    assert len(set(b.GOLD_TABLE_NAMES)) == 15
