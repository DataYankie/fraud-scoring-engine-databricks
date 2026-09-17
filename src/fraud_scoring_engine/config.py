"""Databricks Unity Catalog, Volume, and MLflow configuration.

Medallion architecture uses one catalog with three schemas:

* **bronze** - landing Volume + source-aligned Delta tables
* **silver** - cleaned / conformed transaction entities
* **gold** - behavioral features and serving-oriented tables

Qualified table names are built with ``bronze_table``, ``silver_table``, and
``gold_table``, so the layer is always explicit at the call site.
"""

from __future__ import annotations

import os
from pathlib import Path


def get_catalog() -> str:
    """Return the Unity Catalog name (default ``fraud_dev``)."""
    return os.environ.get("FRAUD_CATALOG", "fraud_dev")


def get_bronze_schema() -> str:
    """Return the bronze schema name (default ``bronze``).

    ``FRAUD_BRONZE_SCHEMA`` wins when set; otherwise ``FRAUD_SCHEMA`` is used
    for backward compatibility with earlier single-schema setups.
    """
    return os.environ.get("FRAUD_BRONZE_SCHEMA") or os.environ.get("FRAUD_SCHEMA", "bronze")


def get_silver_schema() -> str:
    """Return the silver schema name (default ``silver``)."""
    return os.environ.get("FRAUD_SILVER_SCHEMA", "silver")


def get_gold_schema() -> str:
    """Return the gold schema name (default ``gold``)."""
    return os.environ.get("FRAUD_GOLD_SCHEMA", "gold")


def get_volume_name() -> str:
    """Return the Volume name under the catalog/bronze schema (default ``data``)."""
    return os.environ.get("FRAUD_VOLUME", "data")


def volume_root() -> str:
    """Return the Volume root path ``/Volumes/{catalog}/{bronze}/{volume}``.

    Raw CSVs land under ``{volume_root}/raw/``. The Volume stays in bronze so
    landing files remain with the source-aligned layer.
    """
    return f"/Volumes/{get_catalog()}/{get_bronze_schema()}/{get_volume_name()}"


def table_name(table: str, *, schema: str) -> str:
    """Return a fully qualified table name ``{catalog}.{schema}.{table}``."""
    return f"{get_catalog()}.{schema}.{table}"


def bronze_table(table: str) -> str:
    """Return a fully qualified bronze table name."""
    return table_name(table, schema=get_bronze_schema())


def silver_table(table: str) -> str:
    """Return a fully qualified silver table name."""
    return table_name(table, schema=get_silver_schema())


def gold_table(table: str) -> str:
    """Return a fully qualified gold table name."""
    return table_name(table, schema=get_gold_schema())


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
