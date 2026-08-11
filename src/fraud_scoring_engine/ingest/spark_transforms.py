"""Spark transforms for IEEE-CIS Delta ingest."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fraud_scoring_engine.ingest.columns import FEATURE_EXCLUDE_COLUMNS
from fraud_scoring_engine.ingest.transforms import (
    IDENTITY_COLUMNS,
    USER_ID_COMPONENTS,
)


def _ieee_epoch_lit() -> F.Column:
    """IEEE TransactionDT epoch: 2017-11-30 00:00:00 UTC."""
    return F.lit("2017-11-30 00:00:00").cast("timestamp")


def _safe_component(name: str, available: set[str]) -> F.Column:
    """Convert a CSV cell to a stable hash component string (null → empty)."""
    if name not in available:
        return F.lit("")
    col = F.col(name)
    as_str = col.cast("string")
    return (
        F.when(col.isNull(), F.lit(""))
        .when(as_str.isin("NaN", "nan", "None", "null"), F.lit(""))
        .otherwise(as_str)
    )


def derived_user_id_column(available_columns: set[str]) -> F.Column:
    """MD5 hex digest over card/address components joined by ``_``."""
    parts = [_safe_component(name, available_columns) for name in USER_ID_COMPONENTS]
    uid_string = F.concat_ws("_", *parts)
    return F.md5(uid_string)


def prepare_transactions_spark(merged: DataFrame) -> DataFrame:
    """Return operational transaction columns ready for Delta MERGE."""
    cols = set(merged.columns)
    user_id = derived_user_id_column(cols)

    def trunc_str(name: str, max_len: int) -> F.Column:
        if name not in cols:
            return F.lit(None).cast("string")
        return F.substring(F.col(name).cast("string"), 1, max_len)

    def opt_double(name: str) -> F.Column:
        if name not in cols:
            return F.lit(None).cast("double")
        return F.col(name).cast("double")

    return merged.select(
        F.col("TransactionID").cast("long").alias("transaction_id"),
        user_id.alias("derived_user_id"),
        F.col("isFraud").cast("int").alias("is_fraud"),
        F.col("TransactionAmt").cast("double").alias("transaction_amt"),
        trunc_str("ProductCD", 10).alias("product_cd"),
        F.col("TransactionDT").cast("int").alias("transaction_dt"),
        F.from_unixtime(
            F.unix_timestamp(_ieee_epoch_lit()) + F.col("TransactionDT").cast("long")
        )
        .cast("timestamp")
        .alias("transaction_at"),
        opt_double("card1").alias("card1"),
        opt_double("card2").alias("card2"),
        opt_double("card3").alias("card3"),
        trunc_str("card4", 50).alias("card4"),
        opt_double("card5").alias("card5"),
        trunc_str("card6", 50).alias("card6"),
        trunc_str("P_emaildomain", 100).alias("p_emaildomain"),
        trunc_str("R_emaildomain", 100).alias("r_emaildomain"),
        opt_double("addr1").alias("addr1"),
        opt_double("addr2").alias("addr2"),
        opt_double("dist1").alias("dist1"),
        opt_double("dist2").alias("dist2"),
    )


def prepare_identities_spark(merged: DataFrame) -> DataFrame:
    """Return identity rows for transactions with at least one identity field."""
    cols = set(merged.columns)
    present = [name for name in IDENTITY_COLUMNS if name in cols]
    empty_schema = (
        "transaction_id LONG, id_30 STRING, id_31 STRING, "
        "device_type STRING, device_info STRING"
    )
    if not present:
        return merged.sparkSession.createDataFrame([], empty_schema)

    mask = None
    for name in present:
        condition = F.col(name).isNotNull()
        mask = condition if mask is None else (mask | condition)

    subset = merged.filter(mask)

    def trunc(name: str, alias: str, max_len: int) -> F.Column:
        if name not in cols:
            return F.lit(None).cast("string").alias(alias)
        return F.substring(F.col(name).cast("string"), 1, max_len).alias(alias)

    return subset.select(
        F.col("TransactionID").cast("long").alias("transaction_id"),
        trunc("id_30", "id_30", 100),
        trunc("id_31", "id_31", 100),
        trunc("DeviceType", "device_type", 50),
        trunc("DeviceInfo", "device_info", 100),
    )


def split_train_features_spark(merged: DataFrame) -> DataFrame:
    """Drop operational CSV columns; keep ``TransactionID`` as join key."""
    drop_cols = [col for col in FEATURE_EXCLUDE_COLUMNS if col in merged.columns]
    features = merged.drop(*drop_cols) if drop_cols else merged
    if "TransactionID" not in features.columns:
        msg = "Train feature frame must include TransactionID as join key"
        raise ValueError(msg)
    return features


def count_identity_rows_spark(merged: DataFrame) -> int:
    """Count merged rows that include at least one identity field."""
    cols = set(merged.columns)
    present = [name for name in IDENTITY_COLUMNS if name in cols]
    if not present:
        return 0
    mask = None
    for name in present:
        condition = F.col(name).isNotNull()
        mask = condition if mask is None else (mask | condition)
    return merged.filter(mask).count()
