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

# Columns stored in operational Delta tables and excluded from train_features.
# ``TransactionID`` is kept in train_features as the join key.
FEATURE_EXCLUDE_COLUMNS = frozenset(
    (set(TRANSACTION_USECOLS) - {"TransactionID"})
    | {col for col in IDENTITY_USECOLS if col != "TransactionID"}
)


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
