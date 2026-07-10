"""
Offline checks for the provisioned Grafana alert rules
(infra/grafana/provisioning/alerting/rules.yaml). Can't evaluate the rules
without a running Grafana, but YAML validity, datasource UID consistency
with datasources.yml, alert-state completeness, and the exact kafka-exporter
metric spelling are all checkable at review time.
"""
from __future__ import annotations

from pathlib import Path

import yaml

ALERTING_DIR = Path(__file__).parent.parent / "infra" / "grafana" / "provisioning" / "alerting"
RULES_PATH = ALERTING_DIR / "rules.yaml"
DATASOURCES_PATH = (
    Path(__file__).parent.parent
    / "infra"
    / "grafana"
    / "provisioning"
    / "datasources"
    / "datasources.yml"
)

# The Grafana expression pseudo-datasource — always valid, never provisioned.
EXPRESSION_UID = "__expr__"


def _load_rules() -> dict:
    return yaml.safe_load(RULES_PATH.read_text())


def _provisioned_datasource_uids() -> set[str]:
    doc = yaml.safe_load(DATASOURCES_PATH.read_text())
    return {ds["uid"] for ds in doc["datasources"]}


def _all_rules(doc: dict):
    for group in doc["groups"]:
        yield from group["rules"]


def test_rules_yaml_is_valid_yaml_with_groups():
    doc = _load_rules()
    assert doc["groups"], "rules.yaml defines no alert rule groups"
    assert all(group["rules"] for group in doc["groups"])


def test_every_query_datasource_uid_is_provisioned():
    provisioned = _provisioned_datasource_uids()
    for rule in _all_rules(_load_rules()):
        for query in rule["data"]:
            uid = query["datasourceUid"]
            if uid == EXPRESSION_UID:
                continue
            assert uid in provisioned, (
                f"rule {rule['uid']!r} references datasource UID {uid!r}, "
                f"which datasources.yml does not provision ({sorted(provisioned)})"
            )


def test_every_rule_declares_no_data_and_exec_err_states():
    for rule in _all_rules(_load_rules()):
        assert rule.get("noDataState"), f"rule {rule['uid']!r} missing noDataState"
        assert rule.get("execErrState"), f"rule {rule['uid']!r} missing execErrState"


def test_kafka_lag_metric_uses_the_real_exporter_name():
    """danielqsj/kafka-exporter exposes kafka_consumergroup_lag_sum — the
    plausible-looking kafka_consumer_group_lag_sum does not exist and would
    make the alert silently never fire. Guards against regression."""
    raw = RULES_PATH.read_text()
    assert "kafka_consumer_group_lag_sum" not in raw, (
        "rules.yaml uses kafka_consumer_group_lag_sum — the real "
        "kafka-exporter metric is kafka_consumergroup_lag_sum"
    )
    assert "kafka_consumergroup_lag_sum" in raw

def test_high_quarantine_rate_uses_quality_gate_summary():
    rules = list(_all_rules(_load_rules()))
    rule = next(r for r in rules if r["uid"] == "high_quarantine_rate")
    assert rule["paused"] is False
    sql_a = rule["data"][0]["model"]["rawSql"]
    sql_b = rule["data"][1]["model"]["rawSql"]
    assert "quality_gate_summary" in sql_a
    assert "quality_gate_summary" in sql_b
    assert "daily_sales" not in sql_b

def test_kafka_consumer_lag_old_is_paused_and_stopgap_exists():
    rules = list(_all_rules(_load_rules()))
    old_rule = next(r for r in rules if r["uid"] == "kafka_consumer_lag_old")
    assert old_rule["paused"] is True
    
    stopgap_rule = next(r for r in rules if r["uid"] == "kafka_consumer_lag_stopgap")
    assert stopgap_rule["paused"] is False
    expr = stopgap_rule["data"][0]["model"]["expr"]
    assert "kafka_topic_partition_current_offset" in expr

def test_contact_point_provisioning_exists():
    """Alert rules route nowhere without a provisioned contact point and a
    notification policy pointing at it."""
    cp_path = ALERTING_DIR / "contactpoints.yaml"
    assert cp_path.exists(), "no provisioned contact point file (contactpoints.yaml)"
    doc = yaml.safe_load(cp_path.read_text())
    names = {cp["name"] for cp in doc["contactPoints"]}
    assert names, "contactpoints.yaml defines no contact points"
    for policy in doc["policies"]:
        assert policy["receiver"] in names, (
            f"notification policy routes to {policy['receiver']!r}, "
            f"which is not a defined contact point ({sorted(names)})"
        )
