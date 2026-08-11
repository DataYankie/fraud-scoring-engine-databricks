"""Tests for fraud_scoring_engine.config."""

import pytest

from fraud_scoring_engine.config import (
    behavioral_features_table,
    fraud_alerts_table,
    get_catalog,
    get_schema,
    get_volume_name,
    table_name,
    train_features_table,
    transaction_identities_table,
    transactions_table,
    volume_root,
)


def test_defaults() -> None:
    assert get_catalog() == "fraud"
    assert get_schema() == "bronze"
    assert get_volume_name() == "data"
    assert volume_root() == "/Volumes/fraud/bronze/data"
    assert transactions_table() == "fraud.bronze.transactions"
    assert transaction_identities_table() == "fraud.bronze.transaction_identities"
    assert train_features_table() == "fraud.bronze.train_features"
    assert behavioral_features_table() == "fraud.bronze.behavioral_features"
    assert fraud_alerts_table() == "fraud.bronze.fraud_alerts"


def test_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FRAUD_CATALOG", "acme")
    monkeypatch.setenv("FRAUD_SCHEMA", "silver")
    monkeypatch.setenv("FRAUD_VOLUME", "ieee")
    assert volume_root() == "/Volumes/acme/silver/ieee"
    assert table_name("transactions") == "acme.silver.transactions"
