"""Spark/Delta tests for fraud_scoring_engine.ingest.loader."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from fraud_scoring_engine.config import (
    train_features_table,
    transaction_identities_table,
    transactions_table,
)
from fraud_scoring_engine.ingest.loader import ingest_train_transactions
from fraud_scoring_engine.ingest.transforms import generate_user_id_from_components


def _write_sample_csvs(raw_dir: Path) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    transactions = pd.DataFrame(
        [
            {
                "TransactionID": 2987000,
                "isFraud": 0,
                "TransactionAmt": 68.5,
                "TransactionDT": 86400,
                "ProductCD": "W",
                "card1": 13926.0,
                "card2": None,
                "card3": 150.0,
                "card4": "discover",
                "card5": 142.0,
                "card6": "credit",
                "P_emaildomain": "gmail.com",
                "R_emaildomain": None,
                "addr1": 315.0,
                "addr2": 87.0,
                "dist1": 19.0,
                "dist2": None,
                "V1": 0.1,
            },
            {
                "TransactionID": 2987001,
                "isFraud": 1,
                "TransactionAmt": 29.0,
                "TransactionDT": 86401,
                "ProductCD": "W",
                "card1": 2755.0,
                "card2": 404.0,
                "card3": 150.0,
                "card4": "mastercard",
                "card5": 102.0,
                "card6": "credit",
                "P_emaildomain": None,
                "R_emaildomain": None,
                "addr1": 325.0,
                "addr2": 87.0,
                "dist1": None,
                "dist2": None,
                "V1": 0.2,
            },
        ]
    )
    identities = pd.DataFrame(
        [
            {
                "TransactionID": 2987000,
                "id_30": "Windows 10",
                "id_31": "chrome 63.0",
                "DeviceType": "desktop",
                "DeviceInfo": "Windows",
                "id_01": 1.0,
            }
        ]
    )
    transactions.to_csv(raw_dir / "train_transaction.csv", index=False)
    identities.to_csv(raw_dir / "train_identity.csv", index=False)


@pytest.mark.spark
def test_ingest_merges_transactions_identities_and_features(
    bronze_tables,
    tmp_path: Path,
) -> None:
    spark = bronze_tables
    raw = tmp_path / "raw"
    _write_sample_csvs(raw)

    result = ingest_train_transactions(
        spark=spark,
        data_dir=raw,
        ensure_tables=False,
    )

    assert result.rows_read == 2
    assert result.rows_merged == 2
    assert result.identities_merged == 1
    assert result.feature_rows == 2
    assert result.features_table == train_features_table()

    txns = spark.table(transactions_table()).orderBy("transaction_id").collect()
    assert len(txns) == 2
    assert txns[0]["transaction_id"] == 2987000
    expected_uid = generate_user_id_from_components(
        {
            "card1": 13926.0,
            "card2": None,
            "card3": 150.0,
            "card4": "discover",
            "card5": 142.0,
            "card6": "credit",
            "addr1": 315.0,
            "addr2": 87.0,
        }
    )
    assert txns[0]["derived_user_id"] == expected_uid

    identities = spark.table(transaction_identities_table()).collect()
    assert len(identities) == 1
    assert identities[0]["device_type"] == "desktop"

    features = spark.table(train_features_table())
    assert "TransactionID" in features.columns
    assert "V1" in features.columns
    assert "isFraud" not in features.columns
    assert features.count() == 2


@pytest.mark.spark
def test_ingest_merge_is_idempotent(bronze_tables, tmp_path: Path) -> None:
    spark = bronze_tables
    raw = tmp_path / "raw"
    _write_sample_csvs(raw)

    ingest_train_transactions(spark=spark, data_dir=raw, ensure_tables=False)
    ingest_train_transactions(spark=spark, data_dir=raw, ensure_tables=False)

    assert spark.table(transactions_table()).count() == 2
    assert spark.table(transaction_identities_table()).count() == 1


@pytest.mark.spark
def test_ingest_dry_run_writes_nothing(bronze_tables, tmp_path: Path) -> None:
    spark = bronze_tables
    raw = tmp_path / "raw"
    _write_sample_csvs(raw)

    result = ingest_train_transactions(
        spark=spark,
        data_dir=raw,
        dry_run=True,
        ensure_tables=False,
    )
    assert result.rows_read == 2
    assert result.rows_merged == 0
    assert spark.table(transactions_table()).count() == 0
