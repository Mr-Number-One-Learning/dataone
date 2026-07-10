"""
Tests for the hand-authored Grafana dashboard JSON. Can't validate
Grafana-specific semantics without a real instance (e.g. whether a query
actually returns data), but JSON validity, datasource UID consistency with
datasources.yml, table/metric name consistency with what was actually built,
and panel grid non-overlap are all checkable offline.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

DASHBOARDS_DIR = Path(__file__).parent.parent / "infra" / "grafana" / "dashboards"
DATASOURCES_PATH = Path(__file__).parent.parent / "infra" / "grafana" / "provisioning" / "datasources" / "datasources.yml"

EXPECTED_DATASOURCE_UIDS = {"dataone-clickhouse", "dataone-prometheus"}
EXPECTED_CLICKHOUSE_TABLES = {
    "daily_sales",
    "top_products",
    "customer_segments",
    "conversion_rate",
    "campaign_effectiveness",
    "live_activity",
}


def _load(name: str) -> dict:
    return json.loads((DASHBOARDS_DIR / name).read_text())


def _panel_rects(dashboard: dict) -> list[tuple[int, int, int, int]]:
    """(x, y, x+w, y+h) for every panel — used for overlap checking."""
    rects = []
    for panel in dashboard["panels"]:
        g = panel["gridPos"]
        rects.append((g["x"], g["y"], g["x"] + g["w"], g["y"] + g["h"]))
    return rects


def _rects_overlap(a: tuple, b: tuple) -> bool:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    return ax1 < bx2 and bx1 < ax2 and ay1 < by2 and by1 < ay2


def test_datasources_yml_defines_the_uids_the_dashboards_reference():
    content = DATASOURCES_PATH.read_text()
    for uid in EXPECTED_DATASOURCE_UIDS:
        assert f"uid: {uid}" in content, f"datasources.yml doesn't define uid: {uid}"


ALL_DASHBOARDS = ["business_kpis.json", "ops_overview.json", "data_quality.json"]


def test_all_dashboards_are_valid_json_with_panels():
    for name in ALL_DASHBOARDS:
        d = _load(name)
        assert d["panels"], f"{name} has no panels"
        assert d["uid"]


def test_dashboard_panels_dont_overlap():
    for name in ALL_DASHBOARDS:
        rects = _panel_rects(_load(name))
        for i in range(len(rects)):
            for j in range(i + 1, len(rects)):
                assert not _rects_overlap(rects[i], rects[j]), (
                    f"{name}: panels {i} and {j} overlap"
                )


def test_business_kpis_queries_every_clickhouse_mart_table():
    d = _load("business_kpis.json")
    all_sql = " ".join(
        target.get("rawSql", "")
        for panel in d["panels"]
        for target in panel["targets"]
    )
    for table in EXPECTED_CLICKHOUSE_TABLES:
        assert re.search(rf"\bFROM {table}\b", all_sql), f"no panel queries {table}"


def test_business_kpis_panels_use_the_clickhouse_datasource():
    d = _load("business_kpis.json")
    for panel in d["panels"]:
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == "dataone-clickhouse"


def test_ops_overview_panels_use_the_prometheus_datasource():
    d = _load("ops_overview.json")
    for panel in d["panels"]:
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == "dataone-prometheus"


def test_data_quality_panels_use_the_clickhouse_datasource():
    d = _load("data_quality.json")
    for panel in d["panels"]:
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == "dataone-clickhouse"


def test_data_quality_queries_the_quarantine_summary_table():
    d = _load("data_quality.json")
    all_sql = " ".join(
        target.get("rawSql", "")
        for panel in d["panels"]
        for target in panel["targets"]
    )
    assert re.search(r"\bFROM quarantine_summary\b", all_sql), (
        "no data_quality panel queries quarantine_summary"
    )


def test_panel_ids_are_unique_within_each_dashboard():
    for name in ALL_DASHBOARDS:
        d = _load(name)
        ids = [p["id"] for p in d["panels"]]
        assert len(ids) == len(set(ids)), f"{name} has duplicate panel ids"
