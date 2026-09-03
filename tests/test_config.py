"""Tests for fraud_scoring_engine.config."""

from pathlib import Path

import pytest

from fraud_scoring_engine.config import (
    bronze_table,
    get_bronze_schema,
    get_catalog,
    get_gold_schema,
    get_mlflow_artifact_root,
    get_mlflow_tracking_uri,
    get_silver_schema,
    get_volume_name,
    gold_table,
    silver_table,
    table_name,
    volume_root,
)

pytestmark = pytest.mark.unit


def test_defaults() -> None:
    assert get_catalog() == "fraud"
    assert get_bronze_schema() == "bronze"
    assert get_silver_schema() == "silver"
    assert get_gold_schema() == "gold"
    assert get_volume_name() == "data"
    assert volume_root() == "/Volumes/fraud/bronze/data"
    assert bronze_table("transactions") == "fraud.bronze.transactions"
    assert silver_table("transactions") == "fraud.silver.transactions"
    assert silver_table("transaction_identities") == "fraud.silver.transaction_identities"
    assert bronze_table("train_features") == "fraud.bronze.train_features"
    assert gold_table("behavioral_features") == "fraud.gold.behavioral_features"
    assert gold_table("fraud_alerts") == "fraud.gold.fraud_alerts"
    assert get_mlflow_tracking_uri() == "databricks"


def test_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FRAUD_CATALOG", "acme")
    monkeypatch.setenv("FRAUD_BRONZE_SCHEMA", "raw")
    monkeypatch.setenv("FRAUD_SILVER_SCHEMA", "cleaned")
    monkeypatch.setenv("FRAUD_GOLD_SCHEMA", "marts")
    monkeypatch.setenv("FRAUD_VOLUME", "ieee")
    assert volume_root() == "/Volumes/acme/raw/ieee"
    assert table_name("transactions", schema="raw") == "acme.raw.transactions"
    assert silver_table("transactions") == "acme.cleaned.transactions"
    assert gold_table("behavioral_features") == "acme.marts.behavioral_features"


def test_legacy_fraud_schema_sets_bronze(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FRAUD_SCHEMA", "legacy_bronze")
    monkeypatch.delenv("FRAUD_BRONZE_SCHEMA", raising=False)
    assert get_bronze_schema() == "legacy_bronze"
    assert volume_root() == "/Volumes/fraud/legacy_bronze/data"


def test_get_mlflow_tracking_uri_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "file:///tmp/mlruns")
    assert get_mlflow_tracking_uri() == "file:///tmp/mlruns"


def test_get_mlflow_artifact_root_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    monkeypatch.setenv("MLFLOW_ARTIFACT_ROOT", str(artifact_root))
    assert get_mlflow_artifact_root() == artifact_root


def test_get_mlflow_artifact_root_databricks_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    monkeypatch.delenv("MLFLOW_ARTIFACT_ROOT", raising=False)
    assert get_mlflow_artifact_root() == Path("/Volumes/fraud/bronze/data/mlartifacts")
