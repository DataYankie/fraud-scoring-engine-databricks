"""Databricks Unity Catalog, Volume, and MLflow configuration."""

from __future__ import annotations

import os
from pathlib import Path


def get_catalog() -> str:
    """Return the Unity Catalog name (default ``fraud``)."""
    return os.environ.get("FRAUD_CATALOG", "fraud")


def get_schema() -> str:
    """Return the schema name (default ``bronze``)."""
    return os.environ.get("FRAUD_SCHEMA", "bronze")


def get_volume_name() -> str:
    """Return the Volume name under the catalog/schema (default ``data``)."""
    return os.environ.get("FRAUD_VOLUME", "data")


def volume_root() -> str:
    """Return the Volume root path ``/Volumes/{catalog}/{schema}/{volume}``."""
    return f"/Volumes/{get_catalog()}/{get_schema()}/{get_volume_name()}"


def table_name(table: str) -> str:
    """Return a fully qualified table name ``{catalog}.{schema}.{table}``."""
    return f"{get_catalog()}.{get_schema()}.{table}"


def transactions_table() -> str:
    """Return the fully qualified transactions table name."""
    return table_name("transactions")


def transaction_identities_table() -> str:
    """Return the fully qualified transaction_identities table name."""
    return table_name("transaction_identities")


def train_features_table() -> str:
    """Return the fully qualified train_features table name."""
    return table_name("train_features")


def behavioral_features_table() -> str:
    """Return the fully qualified behavioral_features table name."""
    return table_name("behavioral_features")


def fraud_alerts_table() -> str:
    """Return the fully qualified fraud_alerts table name."""
    return table_name("fraud_alerts")


def get_mlflow_tracking_uri() -> str:
    """Return the MLflow tracking URI.

    Reads ``MLFLOW_TRACKING_URI``. Defaults to ``databricks`` so workspace
    experiments work without extra configuration.
    """
    return os.environ.get("MLFLOW_TRACKING_URI", "databricks")


def get_mlflow_artifact_root() -> Path:
    """Return the directory used for MLflow artifact storage.

    Reads ``MLFLOW_ARTIFACT_ROOT``. On Databricks Runtime, defaults to
    ``{volume_root}/mlartifacts``. Otherwise defaults to ``{repo_root}/mlartifacts``.
    """
    if path := os.environ.get("MLFLOW_ARTIFACT_ROOT"):
        return Path(path)
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return Path(volume_root()) / "mlartifacts"
    return Path(__file__).resolve().parents[2] / "mlartifacts"
