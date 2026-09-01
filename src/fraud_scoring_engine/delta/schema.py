"""Delta table schemas and bronze bootstrap for Unity Catalog."""

from __future__ import annotations

from pyspark.sql import SparkSession # type: ignore

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


def ensure_bronze_tables(spark: SparkSession) -> None:
    """Create the bronze schema and Delta tables if they do not exist."""
    catalog = get_catalog()
    schema = get_schema()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    spark.sql(TRANSACTIONS_DDL.format(table=transactions_table()))
    spark.sql(TRANSACTION_IDENTITIES_DDL.format(table=transaction_identities_table()))
    spark.sql(FRAUD_ALERTS_DDL.format(table=fraud_alerts_table()))
    spark.sql(BEHAVIORAL_FEATURES_DDL.format(table=behavioral_features_table()))
    spark.sql(TRAIN_FEATURES_DDL.format(table=train_features_table()))
