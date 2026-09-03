"""Spark/Delta tests for bronze → silver promotion."""

from __future__ import annotations

from datetime import datetime

import pytest

from spark_helpers import write_delta_table
from fraud_scoring_engine.config import bronze_table, silver_table
from fraud_scoring_engine.silver import promote_bronze_to_silver

pytestmark = [pytest.mark.integration, pytest.mark.spark]

BASE_TIME = datetime(2017, 12, 1, 12, 0, 0)


def test_promote_cleans_blank_strings_and_drops_invalid_rows(medallion_tables) -> None:
    spark = medallion_tables
    write_delta_table(
        spark,
        bronze_table("transactions"),
        [
            {
                "transaction_id": 1,
                "derived_user_id": "a" * 32,
                "is_fraud": 0,
                "transaction_amt": 10.0,
                "product_cd": "W",
                "transaction_dt": 0,
                "transaction_at": BASE_TIME,
                "card1": 1.0,
                "card2": None,
                "card3": None,
                "card4": "  ",
                "card5": None,
                "card6": None,
                "p_emaildomain": "",
                "r_emaildomain": None,
                "addr1": None,
                "addr2": None,
                "dist1": None,
                "dist2": None,
            },
            {
                "transaction_id": 2,
                "derived_user_id": "b" * 32,
                "is_fraud": 0,
                "transaction_amt": 20.0,
                "product_cd": "C",
                "transaction_dt": 1,
                "transaction_at": None,
                "card1": 2.0,
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
            },
        ],
    )
    write_delta_table(
        spark,
        bronze_table("transaction_identities"),
        [
            {
                "transaction_id": 1,
                "id_30": "",
                "id_31": "chrome",
                "device_type": "desktop",
                "device_info": "  ",
            },
            {
                "transaction_id": None,
                "id_30": "Windows",
                "id_31": None,
                "device_type": None,
                "device_info": None,
            },
        ],
    )

    result = promote_bronze_to_silver(spark=spark, ensure_tables=False)
    assert result.transactions_written == 1
    assert result.identities_written == 1

    txn = spark.table(silver_table("transactions")).collect()[0]
    assert txn["transaction_id"] == 1
    assert txn["card4"] is None
    assert txn["p_emaildomain"] is None

    identity = spark.table(silver_table("transaction_identities")).collect()[0]
    assert identity["transaction_id"] == 1
    assert identity["id_30"] is None
    assert identity["device_info"] is None
    assert identity["id_31"] == "chrome"
