"""Column sets for Delta table ingestion vs train feature export."""

from __future__ import annotations

import pandas as pd

# CSV columns loaded into Delta ``transactions`` (+ ``TransactionID`` join key).
TRANSACTION_USECOLS = [
    "TransactionID",
    "isFraud",
    "TransactionAmt",
    "TransactionDT",
    "ProductCD",
    "card1",
    "card2",
    "card3",
    "card4",
    "card5",
    "card6",
    "P_emaildomain",
    "R_emaildomain",
    "addr1",
    "addr2",
    "dist1",
    "dist2",
]

# CSV columns loaded into Delta ``transaction_identities``.
IDENTITY_USECOLS = [
    "TransactionID",
    "id_30",
    "id_31",
    "DeviceType",
    "DeviceInfo",
]

TRANSACTION_DTYPES: dict[str, str] = {
    "TransactionID": "int32",
    "TransactionDT": "int32",
    "isFraud": "int8",
}

# Columns stored in operational Delta tables and excluded from train_features.
# ``TransactionID`` is kept in train_features as the join key.
FEATURE_EXCLUDE_COLUMNS = frozenset(
    (set(TRANSACTION_USECOLS) - {"TransactionID"})
    | {col for col in IDENTITY_USECOLS if col != "TransactionID"}
)

# Backwards-compatible alias.
PARQUET_EXCLUDE_COLUMNS = FEATURE_EXCLUDE_COLUMNS


def database_columns(merged: pd.DataFrame) -> list[str]:
    """Return ordered CSV columns required for operational table ingestion."""
    columns: list[str] = []
    for name in TRANSACTION_USECOLS:
        if name in merged.columns:
            columns.append(name)
    for name in IDENTITY_USECOLS:
        if name != "TransactionID" and name in merged.columns:
            columns.append(name)
    return columns


def split_train_features(merged: pd.DataFrame) -> pd.DataFrame:
    """Return columns not stored in operational tables (plus ``TransactionID``).

    Args:
        merged: Full train transaction + identity DataFrame.

    Returns:
        Feature matrix for ML training with one row per transaction.
    """
    drop_cols = [col for col in FEATURE_EXCLUDE_COLUMNS if col in merged.columns]
    features = merged.drop(columns=drop_cols).copy()
    if "TransactionID" not in features.columns:
        msg = "Train feature frame must include TransactionID as join key"
        raise ValueError(msg)
    return features


def split_parquet_features(merged: pd.DataFrame) -> pd.DataFrame:
    """Alias for :func:`split_train_features`."""
    return split_train_features(merged)
