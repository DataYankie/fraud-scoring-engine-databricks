"""Tests for fraud_scoring_engine.training.dataset."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from spark_helpers import write_delta_table
from fraud_scoring_engine.config import (
    train_features_table,
    transaction_identities_table,
    transactions_table,
)
from fraud_scoring_engine.training.dataset import (
    TRANSACTION_TRAINING_COLUMNS,
    build_training_frame,
    load_static_features,
    load_transaction_model_columns,
)

pytestmark = [pytest.mark.integration, pytest.mark.spark]

BASE_TIME = datetime(2017, 12, 1, 12, 0, 0)


def _txn_row(transaction_id: int, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "transaction_id": transaction_id,
        "derived_user_id": "a" * 32,
        "is_fraud": 0,
        "transaction_amt": 50.0,
        "product_cd": "W",
        "transaction_dt": 0,
        "transaction_at": BASE_TIME,
        "card1": 100.0,
        "card2": None,
        "card3": None,
        "card4": "visa",
        "card5": None,
        "card6": None,
        "p_emaildomain": None,
        "r_emaildomain": None,
        "addr1": None,
        "addr2": None,
        "dist1": None,
        "dist2": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def seeded_spark(bronze_tables):
    spark = bronze_tables
    write_delta_table(
        spark,
        transactions_table(),
        [
            _txn_row(101, derived_user_id="a" * 32, is_fraud=0, transaction_amt=50.0, card1=100.0),
            _txn_row(
                102,
                derived_user_id="b" * 32,
                is_fraud=1,
                transaction_amt=75.0,
                product_cd="C",
                transaction_dt=60,
                transaction_at=BASE_TIME.replace(minute=1),
                card1=200.0,
                card4="mastercard",
            ),
        ],
    )
    write_delta_table(
        spark,
        transaction_identities_table(),
        [
            {
                "transaction_id": 101,
                "id_30": "Windows 10",
                "id_31": None,
                "device_type": "desktop",
                "device_info": None,
            }
        ],
    )
    write_delta_table(
        spark,
        train_features_table(),
        [
            {"TransactionID": 101, "C1": 1.0, "D1": 10.0},
            {"TransactionID": 102, "C1": 2.0, "D1": 20.0},
        ],
    )
    return spark


def test_load_transaction_model_columns_returns_delta_features(seeded_spark) -> None:
    dataframe = load_transaction_model_columns(
        seeded_spark,
        transaction_ids=[101, 102],
    )

    assert list(dataframe.columns) == list(TRANSACTION_TRAINING_COLUMNS)
    assert len(dataframe) == 2
    assert set(dataframe["transaction_id"]) == {101, 102}
    assert dataframe.loc[dataframe["transaction_id"] == 101, "card1"].iloc[0] == 100.0
    assert dataframe.loc[dataframe["transaction_id"] == 101, "id_30"].iloc[0] == "Windows 10"
    assert pd.isna(dataframe.loc[dataframe["transaction_id"] == 102, "id_30"].iloc[0])


def test_load_static_features_filters_by_transaction_ids(seeded_spark) -> None:
    loaded = load_static_features(seeded_spark, transaction_ids=[102, 101])

    assert set(loaded["transaction_id"]) == {101, 102}
    assert "C1" in loaded.columns
    assert "D1" in loaded.columns


def test_build_training_frame_merges_all_sources(seeded_spark) -> None:
    dataframe = build_training_frame(seeded_spark, limit=None)

    assert len(dataframe) == 2
    assert "card1" in dataframe.columns
    assert "C1" in dataframe.columns
    assert "velocity_1h" in dataframe.columns
    assert "is_fraud" in dataframe.columns
    assert "transaction_dt" in dataframe.columns
    assert set(dataframe["transaction_id"]) == {101, 102}


def test_build_training_frame_respects_limit(seeded_spark) -> None:
    dataframe = build_training_frame(seeded_spark, limit=1)

    assert len(dataframe) == 1
    assert dataframe["transaction_id"].iloc[0] == 101


def test_compute_transaction_features_dataframe_preserves_transaction_id_order(
    seeded_spark,
) -> None:
    from fraud_scoring_engine.features import compute_transaction_features_dataframe

    dataframe = compute_transaction_features_dataframe(
        seeded_spark,
        transaction_ids=[102, 101],
    )

    assert list(dataframe["transaction_id"]) == [102, 101]


def test_build_training_frame_raises_when_train_features_is_empty(bronze_tables) -> None:
    with pytest.raises(ValueError, match="No training rows found"):
        build_training_frame(bronze_tables)


def test_build_training_frame_raises_when_transactions_have_no_matching_ids(
    seeded_spark,
) -> None:
    write_delta_table(
        seeded_spark,
        train_features_table(),
        [{"TransactionID": 999, "C1": 1.0}],
    )

    with pytest.raises(ValueError, match="No rows matched between"):
        build_training_frame(seeded_spark)


def test_build_training_frame_keeps_rows_when_behavioral_features_missing(
    seeded_spark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def empty_behavioral(
        spark,
        *,
        limit: int | None = 10_000,
        table: str | None = None,
        transaction_ids: list[int] | None = None,
    ) -> pd.DataFrame:
        del spark, limit, table, transaction_ids
        return pd.DataFrame(
            columns=[
                "transaction_id",
                "is_fraud",
                "velocity_1h",
                "cumulative_spend_24h",
                "avg_amount_ratio_30d",
                "avg_amount_ratio_90d",
            ]
        )

    monkeypatch.setattr(
        "fraud_scoring_engine.training.dataset.compute_transaction_features_dataframe",
        empty_behavioral,
    )

    dataframe = build_training_frame(seeded_spark)

    assert len(dataframe) == 2
    assert set(dataframe["is_fraud"]) == {0, 1}
    assert list(dataframe["velocity_1h"]) == [0, 0]
    assert pd.isna(dataframe["avg_amount_ratio_30d"]).all()
