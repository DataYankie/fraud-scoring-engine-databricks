"""Assemble training datasets from medallion Delta tables.

This module loads, joins, and fills the transaction, identity, and behavioral
features needed for model experiments, producing merged frames for training and
evaluation workflows.

Sources:
* bronze ``train_features`` - wide IEEE static columns
* silver ``transactions`` / ``transaction_identities`` - cleaned entities
* on-the-fly (or gold) behavioral rolling features
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from fraud_scoring_engine.config import bronze_table, silver_table
from fraud_scoring_engine.features import (
    BEHAVIORAL_FEATURE_COLUMNS,
    DEFAULT_BEHAVIORAL_FEATURES,
    compute_transaction_features_dataframe,
)
from fraud_scoring_engine.spark_session import get_spark

# Operational + identity columns stored in Delta and used as model features.
TRANSACTION_MODEL_COLUMNS: tuple[str, ...] = (
    "transaction_amt",
    "product_cd",
    "card1",
    "card2",
    "card3",
    "card4",
    "card5",
    "card6",
    "p_emaildomain",
    "r_emaildomain",
    "addr1",
    "addr2",
    "dist1",
    "dist2",
    "id_30",
    "id_31",
    "device_type",
    "device_info",
)

# ``transaction_dt`` is loaded for time-based splits, not as a model feature.
TRANSACTION_TRAINING_COLUMNS: tuple[str, ...] = (
    "transaction_id",
    "is_fraud",
    "transaction_dt",
    *TRANSACTION_MODEL_COLUMNS,
)

_BEHAVIORAL_DEFAULTS = asdict(DEFAULT_BEHAVIORAL_FEATURES)


def load_transaction_model_columns(
    spark: SparkSession | None = None,
    *,
    transaction_ids: Iterable[int] | None = None,
) -> pd.DataFrame:
    """Load Delta modeling columns for transactions (with identity join).

    Args:
        spark: Active Spark session. Defaults to :func:`get_spark`.
        transaction_ids: Optional subset of ``transaction_id`` values to load.

    Returns:
        DataFrame keyed by ``transaction_id`` with ``is_fraud``,
        ``transaction_dt`` (for time-based splits), and
        :data:`TRANSACTION_MODEL_COLUMNS`.
    """
    session = spark if spark is not None else get_spark()
    joined = session.table(silver_table("transactions")).join(
        session.table(silver_table("transaction_identities")),
        on="transaction_id",
        how="left",
    )
    selected = joined.orderBy("transaction_at", "transaction_id").select(
        *TRANSACTION_TRAINING_COLUMNS,
    )
    if transaction_ids is not None:
        ids = list({int(transaction_id) for transaction_id in transaction_ids})
        if not ids:
            return pd.DataFrame(columns=list(TRANSACTION_TRAINING_COLUMNS))
        selected = selected.filter(F.col("transaction_id").isin(ids))

    rows = selected.toPandas()
    if rows.empty:
        return pd.DataFrame(columns=list(TRANSACTION_TRAINING_COLUMNS))
    return rows


def load_static_features(
    spark: SparkSession | None = None,
    *,
    transaction_ids: Iterable[int] | None = None,
    limit: int | None = None,
    table: str | None = None,
) -> pd.DataFrame:
    """Load static IEEE feature columns from the train_features Delta table.

    Args:
        spark: Active Spark session. Defaults to :func:`get_spark`.
        transaction_ids: Optional subset of rows to retain after loading.
        limit: Optional cap on rows, applied in table order before filtering.
        table: Optional fully qualified table override.

    Returns:
        Static feature DataFrame keyed by ``transaction_id``.
    """
    session = spark if spark is not None else get_spark()
    target = table or bronze_table("train_features")
    static = session.table(target).toPandas()
    if "TransactionID" in static.columns:
        static = static.rename(columns={"TransactionID": "transaction_id"})

    if limit is not None:
        static = static.head(limit)

    if transaction_ids is not None:
        ids = {int(transaction_id) for transaction_id in transaction_ids}
        static = static.loc[static["transaction_id"].isin(ids)].copy()

    return static


def _merge_behavioral_features(
    frame: pd.DataFrame,
    behavioral: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join behavioral feature columns and fill missing values."""
    if behavioral.empty:
        behavioral_features = pd.DataFrame(
            columns=["transaction_id", *BEHAVIORAL_FEATURE_COLUMNS],
        )
    else:
        behavioral_features = behavioral[["transaction_id", *BEHAVIORAL_FEATURE_COLUMNS]]

    merged = frame.merge(behavioral_features, on="transaction_id", how="left")
    for column, default in _BEHAVIORAL_DEFAULTS.items():
        if default is None:
            continue
        merged[column] = merged[column].fillna(default)
    return merged


def build_training_frame(
    spark: SparkSession | None = None,
    *,
    limit: int | None = 10_000,
    train_features: str | None = None,
) -> pd.DataFrame:
    """Build a unified training DataFrame from medallion Delta tables.

    Static IEEE columns come from bronze ``train_features``. Operational and
    identity columns are read from silver ``transactions`` /
    ``transaction_identities``. Rolling behavioral features are computed on the
    fly from silver history and left-joined so every matched transaction is kept.

    Row scope follows the train_features slice: when ``limit`` is set, the first
    ``limit`` rows from that table define ``transaction_id`` values. Behavioral
    features are computed over all ingested transactions, then filtered to that
    ID set so rolling windows stay correct.

    Args:
        spark: Active Spark session. Defaults to :func:`get_spark`.
        limit: Maximum rows to include, based on train_features table order.
            ``None`` uses every row in the table.
        train_features: Optional fully qualified train_features table override.

    Returns:
        Merged training frame with ``transaction_id``, ``is_fraud``, operational
        columns, behavioral features, and static IEEE columns.

    Raises:
        ValueError: If the train_features slice is empty or IDs do not match
            the transactions table.
    """
    session = spark if spark is not None else get_spark()
    features_table = train_features or bronze_table("train_features")

    static = load_static_features(session, limit=limit, table=features_table)
    if static.empty:
        msg = (
            f"No training rows found in {features_table} (limit={limit!r}). "
            "Run ingest to populate train_features."
        )
        raise ValueError(msg)

    transaction_ids = static["transaction_id"].astype(int).tolist()
    operational = load_transaction_model_columns(
        session,
        transaction_ids=transaction_ids,
    )
    behavioral = compute_transaction_features_dataframe(
        session,
        limit=None,
        transaction_ids=transaction_ids,
    )

    merged = static.merge(operational, on="transaction_id", how="inner")
    if merged.empty:
        msg = (
            f"No rows matched between {features_table} and {silver_table('transactions')} "
            f"for {len(transaction_ids)} train_features transaction_id(s). "
            "Ensure ingest loaded the same transaction slice into Delta."
        )
        raise ValueError(msg)

    return _merge_behavioral_features(merged, behavioral)
