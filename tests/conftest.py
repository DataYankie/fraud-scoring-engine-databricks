"""Shared pytest fixtures for fraud-scoring-engine tests."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

UC_ENV_KEYS = (
    "FRAUD_CATALOG",
    "FRAUD_SCHEMA",
    "FRAUD_BRONZE_SCHEMA",
    "FRAUD_SILVER_SCHEMA",
    "FRAUD_GOLD_SCHEMA",
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
        "FRAUD_BRONZE_SCHEMA": "bronze",
        "FRAUD_SILVER_SCHEMA": "silver",
        "FRAUD_GOLD_SCHEMA": "gold",
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
        # In Databricks: use the active session (available when running in-process)
        try:
            # Get the active session - available since pytest runs in the same process
            active = SparkSession.getActiveSession()
            if active is not None:
                yield active
            else:
                # This shouldn't happen when running in-process, but provide a clear error
                pytest.skip("No active Spark session found. Ensure tests run in the same process as the job.")
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


def _drop_schema(spark, catalog: str, schema: str) -> None:
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
    except Exception:
        pass


@pytest.fixture
def medallion_tables(spark, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Configure test catalog/schemas and create bronze, silver, and gold Delta tables."""
    import os
    import uuid

    # Check if running in Databricks with Unity Catalog
    is_databricks = "SPARK_REMOTE" in os.environ

    if is_databricks:
        # Use a dedicated test catalog (create it if it doesn't exist)
        # Or use workspace catalog for ephemeral test data
        test_catalog = "workspace"  # workspace catalog is for temporary/test data
        monkeypatch.setenv("FRAUD_CATALOG", test_catalog)

        # Create unique schemas per test run to avoid collisions
        suffix = uuid.uuid4().hex[:8]
        monkeypatch.setenv("FRAUD_BRONZE_SCHEMA", f"fraud_test_{suffix}_bronze")
        monkeypatch.setenv("FRAUD_SILVER_SCHEMA", f"fraud_test_{suffix}_silver")
        monkeypatch.setenv("FRAUD_GOLD_SCHEMA", f"fraud_test_{suffix}_gold")
    else:
        # Local development uses spark_catalog
        monkeypatch.setenv("FRAUD_CATALOG", "spark_catalog")
        monkeypatch.setenv("FRAUD_BRONZE_SCHEMA", "fraud_test_bronze")
        monkeypatch.setenv("FRAUD_SILVER_SCHEMA", "fraud_test_silver")
        monkeypatch.setenv("FRAUD_GOLD_SCHEMA", "fraud_test_gold")
    monkeypatch.setenv("FRAUD_VOLUME", "data")

    from fraud_scoring_engine.config import (
        get_bronze_schema,
        get_catalog,
        get_gold_schema,
        get_silver_schema,
    )
    from fraud_scoring_engine.delta.schema import ensure_medallion_tables

    catalog = get_catalog()
    schemas = (get_bronze_schema(), get_silver_schema(), get_gold_schema())
    for schema in schemas:
        _drop_schema(spark, catalog, schema)

    ensure_medallion_tables(spark)
    yield spark

    # Cleanup: ALWAYS drop the test schemas to avoid leaving test data
    catalog = get_catalog()
    schemas = (get_bronze_schema(), get_silver_schema(), get_gold_schema())
    for schema in schemas:
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
        except Exception as e:
            import warnings

            warnings.warn(f"Failed to cleanup test schema {catalog}.{schema}: {e}")


@pytest.fixture
def test_data_path(spark, medallion_tables, tmp_path: Path) -> Path:
    """Provide a data path accessible to Spark.
    
    On serverless (Spark Connect): returns a Unity Catalog Volume path.
    On local development: returns tmp_path (local filesystem works).
    """
    import os
    
    is_databricks = "SPARK_REMOTE" in os.environ
    
    if is_databricks:
        from fraud_scoring_engine.config import get_catalog, get_bronze_schema, get_volume_name
        
        catalog = get_catalog()
        schema = get_bronze_schema()
        volume = get_volume_name()
        
        # Create the volume for test data
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
        
        # Return the volume path
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/test_data"
        return Path(volume_path)
    else:
        # Local development: tmp_path works fine
        return tmp_path
