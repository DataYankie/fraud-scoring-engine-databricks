"""Tests for fraud_scoring_engine.config."""

from pathlib import Path

import pytest

from fraud_scoring_engine.config import (
    behavioral_features_table,
    fraud_alerts_table,
    get_catalog,
    get_mlflow_artifact_root,
    get_mlflow_tracking_uri,
    get_schema,
    get_volume_name,
    table_name,
    train_features_table,
    transaction_identities_table,
    transactions_table,
    volume_root,
)

pytestmark = pytest.mark.unit


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
    assert get_mlflow_tracking_uri() == "databricks"


def test_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FRAUD_CATALOG", "acme")
    monkeypatch.setenv("FRAUD_SCHEMA", "silver")
    monkeypatch.setenv("FRAUD_VOLUME", "ieee")
    assert volume_root() == "/Volumes/acme/silver/ieee"
    assert table_name("transactions") == "acme.silver.transactions"


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
