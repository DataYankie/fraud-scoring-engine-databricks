"""Spark/Delta tests for fraud_scoring_engine.ingest.loader."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from fraud_scoring_engine.config import bronze_table, silver_table
from fraud_scoring_engine.ingest.loader import ingest_train_transactions
from fraud_scoring_engine.ingest.transforms import generate_user_id_from_components

pytestmark = [pytest.mark.integration, pytest.mark.spark]


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


def _write_extra_transaction_csv(raw_dir: Path) -> None:
    """Replace sample CSVs with a single new transaction for append tests."""
    transactions = pd.DataFrame(
        [
            {
                "TransactionID": 2987002,
                "isFraud": 0,
                "TransactionAmt": 15.0,
                "TransactionDT": 86402,
                "ProductCD": "W",
                "card1": 1000.0,
                "card2": None,
                "card3": 150.0,
                "card4": "visa",
                "card5": 142.0,
                "card6": "debit",
                "P_emaildomain": None,
                "R_emaildomain": None,
                "addr1": 300.0,
                "addr2": 87.0,
                "dist1": 10.0,
                "dist2": None,
                "V1": 0.3,
            }
        ]
    )
    identities = pd.DataFrame([{"TransactionID": 2987002}])
    transactions.to_csv(raw_dir / "train_transaction.csv", index=False)
    identities.to_csv(raw_dir / "train_identity.csv", index=False)


def test_ingest_merges_transactions_identities_and_features(
    medallion_tables,
    tmp_path: Path,
) -> None:
    spark = medallion_tables
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
    assert result.features_table == bronze_table("train_features")
    assert result.silver_transactions == 2
    assert result.silver_identities == 1

    assert spark.table(bronze_table("transactions")).count() == 2
    txns = spark.table(silver_table("transactions")).orderBy("transaction_id").collect()
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

    identities = spark.table(silver_table("transaction_identities")).collect()
    assert len(identities) == 1
    assert identities[0]["device_type"] == "desktop"

    features = spark.table(bronze_table("train_features"))
    feature_cols = features.columns  # Cache to avoid repeated Spark Connect RPC
    assert "TransactionID" in feature_cols
    assert "V1" in feature_cols
    assert "isFraud" not in feature_cols
    assert features.count() == 2


def test_ingest_merge_is_idempotent(medallion_tables, tmp_path: Path) -> None:
    spark = medallion_tables
    raw = tmp_path / "raw"
    _write_sample_csvs(raw)

    ingest_train_transactions(spark=spark, data_dir=raw, ensure_tables=False)
    ingest_train_transactions(spark=spark, data_dir=raw, ensure_tables=False)

    assert spark.table(silver_table("transactions")).count() == 2
    assert spark.table(silver_table("transaction_identities")).count() == 1
    assert spark.table(bronze_table("train_features")).count() == 2


def test_ingest_features_merge_appends_new_transactions(
    medallion_tables,
    tmp_path: Path,
) -> None:
    spark = medallion_tables
    raw = tmp_path / "raw"
    _write_sample_csvs(raw)

    ingest_train_transactions(spark=spark, data_dir=raw, ensure_tables=False)
    assert spark.table(bronze_table("train_features")).count() == 2

    _write_extra_transaction_csv(raw)
    ingest_train_transactions(spark=spark, data_dir=raw, ensure_tables=False)

    features = spark.table(bronze_table("train_features"))
    assert features.count() == 3
    ids = {row["TransactionID"] for row in features.select("TransactionID").collect()}
    assert ids == {2987000, 2987001, 2987002}
    # Verify no duplicates created
    assert features.select("TransactionID").distinct().count() == 3


def test_ingest_features_merge_updates_existing_records(
    medallion_tables,
    tmp_path: Path,
) -> None:
    """Verify merge updates existing records rather than duplicating."""
    spark = medallion_tables
    raw = tmp_path / "raw"
    _write_sample_csvs(raw)

    ingest_train_transactions(spark=spark, data_dir=raw, ensure_tables=False)

    # Modify CSV with same TransactionID but different feature value
    transactions = pd.DataFrame(
        [
            {
                "TransactionID": 2987000,  # Same ID as first record
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
                "V1": 0.999,  # Changed from 0.1
            }
        ]
    )
    identities = pd.DataFrame([{"TransactionID": 2987000}])
    transactions.to_csv(raw / "train_transaction.csv", index=False)
    identities.to_csv(raw / "train_identity.csv", index=False)

    ingest_train_transactions(spark=spark, data_dir=raw, ensure_tables=False)

    features = spark.table(bronze_table("train_features"))
    assert features.count() == 2  # Still 2, not 3

    # Verify the feature value was updated (V1 is a feature column)
    row = features.filter("TransactionID = 2987000").collect()[0]
    assert row["V1"] == 0.999


def test_ingest_dry_run_writes_nothing(medallion_tables, tmp_path: Path) -> None:
    spark = medallion_tables
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
    assert spark.table(bronze_table("transactions")).count() == 0
    assert spark.table(silver_table("transactions")).count() == 0


def test_ingest_skip_silver_leaves_silver_empty(medallion_tables, tmp_path: Path) -> None:
    spark = medallion_tables
    raw = tmp_path / "raw"
    _write_sample_csvs(raw)

    result = ingest_train_transactions(
        spark=spark,
        data_dir=raw,
        ensure_tables=False,
        promote_silver=False,
    )
    assert result.rows_merged == 2
    assert result.silver_transactions == 0
    assert spark.table(bronze_table("transactions")).count() == 2
    assert spark.table(silver_table("transactions")).count() == 0
