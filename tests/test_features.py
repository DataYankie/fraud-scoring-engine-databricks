"""Tests for fraud_scoring_engine.features (Spark / Delta)."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from spark_helpers import write_delta_table
from fraud_scoring_engine.config import transactions_table
from fraud_scoring_engine.features import (
    compute_avg_amount_ratio,
    compute_cumulative_spend,
    compute_transaction_features,
    compute_transaction_features_dataframe,
    compute_transaction_features_from_row,
    compute_velocity,
)

USER_A = "a" * 32
USER_B = "b" * 32
BASE_TIME = datetime(2017, 12, 1, 12, 0, 0)


def _seed_transactions(spark, rows: list[dict]) -> None:
    write_delta_table(spark, transactions_table(), rows)


@pytest.fixture
def seeded_spark(bronze_tables):
    """Seed transactions for USER_A with controlled times and amounts."""
    spark = bronze_tables
    rows = [
        {
            "transaction_id": 1,
            "derived_user_id": USER_A,
            "is_fraud": 0,
            "transaction_amt": 10.0,
            "product_cd": "W",
            "transaction_dt": 0,
            "transaction_at": BASE_TIME - timedelta(days=45),
            "card1": None,
            "card2": None,
            "card3": None,
            "card4": None,
            "card5": None,
            "card6": None,
            "p_emaildomain": None,
            "r_emaildomain": None,
            "addr1": None,
            "addr2": None,
            "dist1": None,
            "dist2": None,
        },
        {
            "transaction_id": 2,
            "derived_user_id": USER_A,
            "is_fraud": 0,
            "transaction_amt": 20.0,
            "product_cd": "W",
            "transaction_dt": 0,
            "transaction_at": BASE_TIME - timedelta(minutes=20),
            "card1": None,
            "card2": None,
            "card3": None,
            "card4": None,
            "card5": None,
            "card6": None,
            "p_emaildomain": None,
            "r_emaildomain": None,
            "addr1": None,
            "addr2": None,
            "dist1": None,
            "dist2": None,
        },
        {
            "transaction_id": 3,
            "derived_user_id": USER_A,
            "is_fraud": 0,
            "transaction_amt": 30.0,
            "product_cd": "W",
            "transaction_dt": 0,
            "transaction_at": BASE_TIME - timedelta(minutes=10),
            "card1": None,
            "card2": None,
            "card3": None,
            "card4": None,
            "card5": None,
            "card6": None,
            "p_emaildomain": None,
            "r_emaildomain": None,
            "addr1": None,
            "addr2": None,
            "dist1": None,
            "dist2": None,
        },
        {
            "transaction_id": 4,
            "derived_user_id": USER_A,
            "is_fraud": 0,
            "transaction_amt": 2000.0,
            "product_cd": "W",
            "transaction_dt": 0,
            "transaction_at": BASE_TIME,
            "card1": None,
            "card2": None,
            "card3": None,
            "card4": None,
            "card5": None,
            "card6": None,
            "p_emaildomain": None,
            "r_emaildomain": None,
            "addr1": None,
            "addr2": None,
            "dist1": None,
            "dist2": None,
        },
        {
            "transaction_id": 5,
            "derived_user_id": USER_A,
            "is_fraud": 0,
            "transaction_amt": 40.0,
            "product_cd": "W",
            "transaction_dt": 0,
            "transaction_at": BASE_TIME,
            "card1": None,
            "card2": None,
            "card3": None,
            "card4": None,
            "card5": None,
            "card6": None,
            "p_emaildomain": None,
            "r_emaildomain": None,
            "addr1": None,
            "addr2": None,
            "dist1": None,
            "dist2": None,
        },
    ]
    _seed_transactions(spark, rows)
    return spark


@pytest.mark.spark
def test_first_transaction_has_zero_velocity_and_spend(bronze_tables) -> None:
    spark = bronze_tables
    _seed_transactions(
        spark,
        [
            {
                "transaction_id": 100,
                "derived_user_id": USER_B,
                "is_fraud": 0,
                "transaction_amt": 50.0,
                "product_cd": "W",
                "transaction_dt": 0,
                "transaction_at": BASE_TIME,
                "card1": None,
                "card2": None,
                "card3": None,
                "card4": None,
                "card5": None,
                "card6": None,
                "p_emaildomain": None,
                "r_emaildomain": None,
                "addr1": None,
                "addr2": None,
                "dist1": None,
                "dist2": None,
            }
        ],
    )

    assert (
        compute_velocity(
            spark,
            derived_user_id=USER_B,
            transaction_at=BASE_TIME,
            window_hours=1,
            transaction_id=100,
        )
        == 0
    )
    assert (
        compute_cumulative_spend(
            spark,
            derived_user_id=USER_B,
            transaction_at=BASE_TIME,
            window_hours=24,
            transaction_id=100,
        )
        == 0.0
    )
    assert (
        compute_avg_amount_ratio(
            spark,
            derived_user_id=USER_B,
            transaction_at=BASE_TIME,
            transaction_amt=50.0,
            window_days=30,
            transaction_id=100,
        )
        is None
    )


@pytest.mark.spark
def test_velocity_counts_prior_transactions_in_last_hour(seeded_spark) -> None:
    assert (
        compute_velocity(
            seeded_spark,
            derived_user_id=USER_A,
            transaction_at=BASE_TIME,
            window_hours=1,
            transaction_id=4,
        )
        == 2
    )


@pytest.mark.spark
def test_cumulative_spend_sums_prior_24h_only(seeded_spark) -> None:
    assert (
        compute_cumulative_spend(
            seeded_spark,
            derived_user_id=USER_A,
            transaction_at=BASE_TIME,
            window_hours=24,
            transaction_id=4,
        )
        == 50.0
    )


@pytest.mark.spark
def test_avg_amount_ratio_spikes_for_large_current_amount(seeded_spark) -> None:
    ratio_30d = compute_avg_amount_ratio(
        seeded_spark,
        derived_user_id=USER_A,
        transaction_at=BASE_TIME,
        transaction_amt=2000.0,
        window_days=30,
        transaction_id=4,
    )
    ratio_90d = compute_avg_amount_ratio(
        seeded_spark,
        derived_user_id=USER_A,
        transaction_at=BASE_TIME,
        transaction_amt=2000.0,
        window_days=90,
        transaction_id=4,
    )

    assert ratio_30d == pytest.approx(2000.0 / 25.0)
    assert ratio_90d == pytest.approx(2000.0 / 20.0)


@pytest.mark.spark
def test_null_derived_user_id_returns_safe_defaults(bronze_tables) -> None:
    spark = bronze_tables
    assert (
        compute_velocity(
            spark,
            derived_user_id=None,
            transaction_at=BASE_TIME,
            window_hours=1,
            transaction_id=200,
        )
        == 0
    )
    assert (
        compute_cumulative_spend(
            spark,
            derived_user_id=None,
            transaction_at=BASE_TIME,
            window_hours=24,
            transaction_id=200,
        )
        == 0.0
    )
    assert (
        compute_avg_amount_ratio(
            spark,
            derived_user_id=None,
            transaction_at=BASE_TIME,
            transaction_amt=100.0,
            window_days=30,
            transaction_id=200,
        )
        is None
    )


@pytest.mark.spark
def test_same_timestamp_excludes_current_transaction_id(seeded_spark) -> None:
    assert (
        compute_velocity(
            seeded_spark,
            derived_user_id=USER_A,
            transaction_at=BASE_TIME,
            window_hours=1,
            transaction_id=5,
        )
        == 3
    )
    assert (
        compute_cumulative_spend(
            seeded_spark,
            derived_user_id=USER_A,
            transaction_at=BASE_TIME,
            window_hours=24,
            transaction_id=5,
        )
        == 2050.0
    )


@pytest.mark.spark
def test_compute_transaction_features_aggregator(seeded_spark) -> None:
    features = compute_transaction_features(
        seeded_spark,
        derived_user_id=USER_A,
        transaction_at=BASE_TIME,
        transaction_amt=2000.0,
        transaction_id=4,
    )

    assert features.velocity_1h == 2
    assert features.cumulative_spend_24h == 50.0
    assert features.avg_amount_ratio_30d == pytest.approx(2000.0 / 25.0)
    assert features.avg_amount_ratio_90d == pytest.approx(2000.0 / 20.0)


@pytest.mark.spark
def test_compute_transaction_features_from_row(seeded_spark) -> None:
    features = compute_transaction_features_from_row(
        seeded_spark,
        transaction_id=4,
        derived_user_id=USER_A,
        transaction_at=BASE_TIME,
        transaction_amt=2000.0,
    )

    assert features.velocity_1h == 2
    assert features.cumulative_spend_24h == 50.0


@pytest.mark.spark
def test_compute_transaction_features_dataframe_matches_row_by_row(seeded_spark) -> None:
    dataframe = compute_transaction_features_dataframe(seeded_spark, limit=None)

    assert len(dataframe) == 5

    for _, row in dataframe.iterrows():
        txn = (
            seeded_spark.table(transactions_table())
            .filter(f"transaction_id = {int(row['transaction_id'])}")
            .collect()[0]
        )
        expected = compute_transaction_features(
            seeded_spark,
            derived_user_id=txn["derived_user_id"],
            transaction_at=txn["transaction_at"],
            transaction_amt=float(txn["transaction_amt"]),
            transaction_id=int(txn["transaction_id"]),
        )
        assert row["velocity_1h"] == expected.velocity_1h
        assert row["cumulative_spend_24h"] == expected.cumulative_spend_24h
        if expected.avg_amount_ratio_30d is None:
            assert row["avg_amount_ratio_30d"] is None or pd.isna(row["avg_amount_ratio_30d"])
        else:
            assert row["avg_amount_ratio_30d"] == pytest.approx(expected.avg_amount_ratio_30d)
        if expected.avg_amount_ratio_90d is None:
            assert row["avg_amount_ratio_90d"] is None or pd.isna(row["avg_amount_ratio_90d"])
        else:
            assert row["avg_amount_ratio_90d"] == pytest.approx(expected.avg_amount_ratio_90d)


@pytest.mark.spark
def test_compute_transaction_features_dataframe_respects_limit(seeded_spark) -> None:
    dataframe = compute_transaction_features_dataframe(seeded_spark, limit=2)

    assert len(dataframe) == 2
    assert list(dataframe["transaction_id"]) == [1, 2]


@pytest.mark.spark
def test_compute_transaction_features_dataframe_preserves_transaction_id_order(
    seeded_spark,
) -> None:
    dataframe = compute_transaction_features_dataframe(
        seeded_spark,
        transaction_ids=[4, 1, 2],
    )

    assert list(dataframe["transaction_id"]) == [4, 1, 2]
