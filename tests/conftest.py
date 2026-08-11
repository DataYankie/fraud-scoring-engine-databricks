"""Shared pytest fixtures for fraud-scoring-engine tests."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

UC_ENV_KEYS = (
    "FRAUD_CATALOG",
    "FRAUD_SCHEMA",
    "FRAUD_VOLUME",
)


@pytest.fixture(autouse=True)
def isolate_uc_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove UC-related env vars so tests do not leak state."""
    for key in UC_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def uc_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Set canonical FRAUD_* values for config tests."""
    values = {
        "FRAUD_CATALOG": "fraud",
        "FRAUD_SCHEMA": "bronze",
        "FRAUD_VOLUME": "data",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[object]:
    """Session-scoped local SparkSession with Delta Lake support."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("fraud-scoring-engine-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:  # noqa: BLE001 - surface as skip when JVM missing
        pytest.skip(f"Spark/Delta session unavailable: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze_tables(spark, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Configure test catalog/schema and create bronze Delta tables."""
    monkeypatch.setenv("FRAUD_CATALOG", "spark_catalog")
    monkeypatch.setenv("FRAUD_SCHEMA", "fraud_test")
    monkeypatch.setenv("FRAUD_VOLUME", "data")

    from fraud_scoring_engine.delta.schema import ensure_bronze_tables

    ensure_bronze_tables(spark)
    yield spark
    spark.sql("DROP SCHEMA IF EXISTS spark_catalog.fraud_test CASCADE")
