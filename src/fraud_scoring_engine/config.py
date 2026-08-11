"""Databricks Unity Catalog and Volume configuration."""

from __future__ import annotations

import os


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
