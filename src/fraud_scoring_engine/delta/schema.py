"""Delta table schemas and bronze bootstrap for Unity Catalog."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from fraud_scoring_engine.config import (
    behavioral_features_table,
    fraud_alerts_table,
    get_catalog,
    get_schema,
    train_features_table,
    transaction_identities_table,
    transactions_table,
)

TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  transaction_id BIGINT,
  derived_user_id STRING,
  is_fraud INT,
  transaction_amt DOUBLE,
  product_cd STRING,
  transaction_dt INT,
  transaction_at TIMESTAMP,
  card1 DOUBLE,
  card2 DOUBLE,
  card3 DOUBLE,
  card4 STRING,
  card5 DOUBLE,
  card6 STRING,
  p_emaildomain STRING,
  r_emaildomain STRING,
  addr1 DOUBLE,
  addr2 DOUBLE,
  dist1 DOUBLE,
  dist2 DOUBLE
) USING DELTA
{location_clause}"""

TRANSACTION_IDENTITIES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  transaction_id BIGINT,
  id_30 STRING,
  id_31 STRING,
  device_type STRING,
  device_info STRING
) USING DELTA
{location_clause}"""

FRAUD_ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  id BIGINT,
  transaction_id BIGINT,
  predicted_ml_prob DOUBLE,
  decision STRING,
  ai_analyst_reason STRING
) USING DELTA
{location_clause}"""

BEHAVIORAL_FEATURES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  transaction_id BIGINT,
  is_fraud INT,
  velocity_1h INT,
  cumulative_spend_24h DOUBLE,
  avg_amount_ratio_30d DOUBLE,
  avg_amount_ratio_90d DOUBLE
) USING DELTA
{location_clause}"""

# Wide IEEE feature matrix schema is set on first overwrite ingest.
TRAIN_FEATURES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  TransactionID BIGINT
) USING DELTA
{location_clause}"""


def _local_table_location_base(spark: SparkSession) -> Path | None:
    """Return filesystem base for external Delta tables in local spark_catalog mode."""
    if get_catalog() != "spark_catalog":
        return None
    base = Path(spark.conf.get("spark.sql.warehouse.dir", "/tmp/spark-warehouse")) / get_schema()
    base.mkdir(parents=True, exist_ok=True)
    return base


def _location_clause(base: Path | None, subdir: str) -> str:
    if base is None:
        return ""
    return f"LOCATION '{(base / subdir).as_posix()}'"


def _create_bronze_table(
    spark: SparkSession,
    ddl: str,
    table_name: str,
    subdir: str,
) -> None:
    base = _local_table_location_base(spark)
    spark.sql(
        ddl.format(
            table=table_name,
            location_clause=_location_clause(base, subdir),
        )
    )


def ensure_bronze_tables(spark: SparkSession) -> None:
    """Create the bronze schema and Delta tables if they do not exist."""
    catalog = get_catalog()
    schema = get_schema()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    _create_bronze_table(spark, TRANSACTIONS_DDL, transactions_table(), "transactions")
    _create_bronze_table(
        spark, TRANSACTION_IDENTITIES_DDL, transaction_identities_table(), "transaction_identities"
    )
    _create_bronze_table(spark, FRAUD_ALERTS_DDL, fraud_alerts_table(), "fraud_alerts")
    _create_bronze_table(
        spark, BEHAVIORAL_FEATURES_DDL, behavioral_features_table(), "behavioral_features"
    )
    _create_bronze_table(spark, TRAIN_FEATURES_DDL, train_features_table(), "train_features")
