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
    """Session-scoped SparkSession with Delta Lake support.
    
    Uses existing Databricks session when available, otherwise creates local session.
    """
    pytest.importorskip("pyspark")
    import os
    from pyspark.sql import SparkSession

    # Check if running in Databricks Spark Connect environment
    is_databricks = "SPARK_REMOTE" in os.environ

    if is_databricks:
        # In Databricks: use getOrCreate without master specification
        # Spark Connect doesn't allow setting master or accessing sparkContext
        try:
            session = SparkSession.builder.appName("fraud-scoring-engine-tests").getOrCreate()
            # Note: sparkContext.setLogLevel() not supported on Spark Connect
            yield session
        except Exception as exc:
            pytest.skip(f"Databricks Spark session unavailable: {exc}")
    else:
        # Local development: create local session with Delta
        pytest.importorskip("delta")
        from delta import configure_spark_with_delta_pip

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
    import os
    import uuid
    
    # Check if running in Databricks with Unity Catalog
    is_databricks = "SPARK_REMOTE" in os.environ
    
    if is_databricks:
        # Use a dedicated test catalog (create it if it doesn't exist)
        # Or use workspace catalog for ephemeral test data
        test_catalog = "workspace"  # workspace catalog is for temporary/test data
        monkeypatch.setenv("FRAUD_CATALOG", test_catalog)
        
        # Create a unique schema per test run to avoid collisions
        test_schema = f"fraud_test_{uuid.uuid4().hex[:8]}"
        monkeypatch.setenv("FRAUD_SCHEMA", test_schema)
    else:
        # Local development uses spark_catalog
        monkeypatch.setenv("FRAUD_CATALOG", "spark_catalog")
        monkeypatch.setenv("FRAUD_SCHEMA", "fraud_test")
    monkeypatch.setenv("FRAUD_VOLUME", "data")

    from fraud_scoring_engine.delta.schema import ensure_bronze_tables
    from fraud_scoring_engine.config import get_catalog, get_schema

    cleanup_catalog = get_catalog()
    cleanup_schema = get_schema()
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {cleanup_catalog}.{cleanup_schema} CASCADE")
    except Exception:
        pass

    ensure_bronze_tables(spark)
    yield spark
    
    # Cleanup: ALWAYS drop the test schema to avoid leaving test data
    cleanup_catalog = get_catalog()
    cleanup_schema = get_schema()
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {cleanup_catalog}.{cleanup_schema} CASCADE")
    except Exception as e:
        # Log cleanup failure but don't fail the test
        # In CI/CD, you may want to fail here to catch cleanup issues
        import warnings
        warnings.warn(f"Failed to cleanup test schema {cleanup_catalog}.{cleanup_schema}: {e}")
