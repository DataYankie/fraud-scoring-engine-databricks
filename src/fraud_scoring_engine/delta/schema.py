"""Delta table schemas and create-if-missing helpers for Unity Catalog."""

from __future__ import annotations

from pyspark.sql import SparkSession  # type: ignore

from fraud_scoring_engine.config import (
    bronze_table,
    get_bronze_schema,
    get_catalog,
    get_gold_schema,
    get_silver_schema,
    gold_table,
    silver_table,
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
"""

TRANSACTION_IDENTITIES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  transaction_id BIGINT,
  id_30 STRING,
  id_31 STRING,
  device_type STRING,
  device_info STRING
) USING DELTA
"""

FRAUD_ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  id BIGINT,
  transaction_id BIGINT,
  predicted_ml_prob DOUBLE,
  decision STRING,
  ai_analyst_reason STRING
) USING DELTA
"""

BEHAVIORAL_FEATURES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  transaction_id BIGINT,
  is_fraud INT,
  velocity_1h INT,
  cumulative_spend_24h DOUBLE,
  avg_amount_ratio_30d DOUBLE,
  avg_amount_ratio_90d DOUBLE
) USING DELTA
"""

# Wide IEEE feature matrix schema is set on first append/merge ingest (mergeSchema).
TRAIN_FEATURES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  TransactionID BIGINT
) USING DELTA
"""


def _ensure_schema(spark: SparkSession, schema: str) -> None:
    catalog = get_catalog()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def ensure_bronze_tables(spark: SparkSession) -> None:
    """Create the bronze schema and source-aligned Delta tables if missing."""
    _ensure_schema(spark, get_bronze_schema())
    spark.sql(TRANSACTIONS_DDL.format(table=bronze_table("transactions")))
    spark.sql(TRANSACTION_IDENTITIES_DDL.format(table=bronze_table("transaction_identities")))
    spark.sql(TRAIN_FEATURES_DDL.format(table=bronze_table("train_features")))


def ensure_silver_tables(spark: SparkSession) -> None:
    """Create the silver schema and cleaned entity Delta tables if missing."""
    _ensure_schema(spark, get_silver_schema())
    spark.sql(TRANSACTIONS_DDL.format(table=silver_table("transactions")))
    spark.sql(TRANSACTION_IDENTITIES_DDL.format(table=silver_table("transaction_identities")))


def ensure_gold_tables(spark: SparkSession) -> None:
    """Create the gold schema and feature/serving Delta tables if missing."""
    _ensure_schema(spark, get_gold_schema())
    spark.sql(BEHAVIORAL_FEATURES_DDL.format(table=gold_table("behavioral_features")))
    spark.sql(FRAUD_ALERTS_DDL.format(table=gold_table("fraud_alerts")))


def ensure_medallion_tables(spark: SparkSession) -> None:
    """Create bronze, silver, and gold schemas/tables if they do not exist."""
    ensure_bronze_tables(spark)
    ensure_silver_tables(spark)
    ensure_gold_tables(spark)
