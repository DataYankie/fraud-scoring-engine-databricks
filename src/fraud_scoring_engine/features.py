"""Spark/Delta queries for per-transaction fraud scoring features."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

import pandas as pd
from pyspark.sql import DataFrame, SparkSession 
from pyspark.sql import functions as F 

from fraud_scoring_engine.config import gold_table, silver_table
from fraud_scoring_engine.delta.schema import ensure_gold_tables, ensure_silver_tables
from fraud_scoring_engine.spark_session import get_spark


def _prior_filter(
    spark: SparkSession,
    *,
    derived_user_id: str,
    transaction_at: datetime,
    window: timedelta,
    transaction_id: int | None = None,
    table: str | None = None,
) -> DataFrame:
    """Return prior transactions for a user within a rolling window.

    Defaults to the silver ``transactions`` table (cleaned entities).
    """
    target = table or silver_table("transactions")
    window_start = transaction_at - window
    df = spark.table(target).filter(
        (F.col("derived_user_id") == derived_user_id)
        & (F.col("transaction_at") >= F.lit(window_start))
    )
    if transaction_id is not None:
        df = df.filter(
            (F.col("transaction_at") < F.lit(transaction_at))
            | (
                (F.col("transaction_at") == F.lit(transaction_at))
                & (F.col("transaction_id") < F.lit(transaction_id))
            )
        )
    else:
        df = df.filter(F.col("transaction_at") < F.lit(transaction_at))
    return df


def compute_velocity(
    spark: SparkSession,
    *,
    derived_user_id: str | None,
    transaction_at: datetime,
    window_hours: int,
    transaction_id: int | None = None,
    table: str | None = None,
) -> int:
    """Count prior transactions by a user within a rolling hour window."""
    if derived_user_id is None:
        return 0
    return _prior_filter(
        spark,
        derived_user_id=derived_user_id,
        transaction_at=transaction_at,
        window=timedelta(hours=window_hours),
        transaction_id=transaction_id,
        table=table,
    ).count()


def compute_cumulative_spend(
    spark: SparkSession,
    *,
    derived_user_id: str | None,
    transaction_at: datetime,
    window_hours: int,
    transaction_id: int | None = None,
    table: str | None = None,
) -> float:
    """Sum prior transaction amounts by a user within a rolling hour window."""
    if derived_user_id is None:
        return 0.0
    total = _prior_filter(
        spark,
        derived_user_id=derived_user_id,
        transaction_at=transaction_at,
        window=timedelta(hours=window_hours),
        transaction_id=transaction_id,
        table=table,
    ).agg(F.coalesce(F.sum("transaction_amt"), F.lit(0.0))).collect()[0][0]
    return float(total)


def compute_avg_amount_ratio(
    spark: SparkSession,
    *,
    derived_user_id: str | None,
    transaction_at: datetime,
    transaction_amt: float,
    window_days: int,
    transaction_id: int | None = None,
    table: str | None = None,
) -> float | None:
    """Compute the ratio of current amount to prior average amount."""
    if derived_user_id is None:
        return None
    avg_amount = _prior_filter(
        spark,
        derived_user_id=derived_user_id,
        transaction_at=transaction_at,
        window=timedelta(days=window_days),
        transaction_id=transaction_id,
        table=table,
    ).agg(F.avg("transaction_amt")).collect()[0][0]
    if avg_amount is None:
        return None
    avg_value = float(avg_amount)
    if avg_value == 0:
        return None
    return transaction_amt / avg_value


@dataclass(frozen=True)
class TransactionFeatures:
    """Aggregated fraud scoring features for a single transaction."""

    velocity_1h: int
    cumulative_spend_24h: float
    avg_amount_ratio_30d: float | None
    avg_amount_ratio_90d: float | None


@dataclass(frozen=True)
class _TxnRow:
    """Minimal transaction fields used for bulk feature computation."""

    transaction_id: int
    derived_user_id: str | None
    transaction_at: datetime
    transaction_amt: float
    is_fraud: int | None


BEHAVIORAL_FEATURE_COLUMNS: tuple[str, ...] = (
    "velocity_1h",
    "cumulative_spend_24h",
    "avg_amount_ratio_30d",
    "avg_amount_ratio_90d",
)

_NULL_FEATURES = TransactionFeatures(
    velocity_1h=0,
    cumulative_spend_24h=0.0,
    avg_amount_ratio_30d=None,
    avg_amount_ratio_90d=None,
)

DEFAULT_BEHAVIORAL_FEATURES = _NULL_FEATURES


def _compute_bulk_user_group(rows: list[_TxnRow]) -> dict[int, TransactionFeatures]:
    """Compute features for a single user's chronologically sorted transactions."""
    n = len(rows)
    result: dict[int, TransactionFeatures] = {}
    left_1h = 0
    left_24h = 0
    left_30d = 0
    left_90d = 0
    sum_24h = 0.0
    sum_30d = 0.0
    sum_90d = 0.0

    for i in range(n):
        at = rows[i].transaction_at
        amt = float(rows[i].transaction_amt)

        while left_1h < i and rows[left_1h].transaction_at < at - timedelta(hours=1):
            left_1h += 1

        while left_24h < i and rows[left_24h].transaction_at < at - timedelta(hours=24):
            sum_24h -= float(rows[left_24h].transaction_amt)
            left_24h += 1

        while left_30d < i and rows[left_30d].transaction_at < at - timedelta(days=30):
            sum_30d -= float(rows[left_30d].transaction_amt)
            left_30d += 1

        while left_90d < i and rows[left_90d].transaction_at < at - timedelta(days=90):
            sum_90d -= float(rows[left_90d].transaction_amt)
            left_90d += 1

        count_30d = i - left_30d
        avg_30d = (sum_30d / count_30d) if count_30d > 0 else None
        ratio_30d = (amt / avg_30d) if avg_30d else None

        count_90d = i - left_90d
        avg_90d = (sum_90d / count_90d) if count_90d > 0 else None
        ratio_90d = (amt / avg_90d) if avg_90d else None

        result[rows[i].transaction_id] = TransactionFeatures(
            velocity_1h=i - left_1h,
            cumulative_spend_24h=sum_24h,
            avg_amount_ratio_30d=ratio_30d,
            avg_amount_ratio_90d=ratio_90d,
        )

        sum_24h += amt
        sum_30d += amt
        sum_90d += amt

    return result


def _fetch_all_transaction_rows(
    spark: SparkSession,
    *,
    table: str | None = None,
) -> list[_TxnRow]:
    """Load all transactions in chronological order from silver by default."""
    target = table or silver_table("transactions")
    rows = (
        spark.table(target)
        .orderBy("transaction_at", "transaction_id")
        .select(
            "transaction_id",
            "derived_user_id",
            "transaction_at",
            "transaction_amt",
            "is_fraud",
        )
        .collect()
    )
    return [
        _TxnRow(
            transaction_id=int(row["transaction_id"]),
            derived_user_id=row["derived_user_id"],
            transaction_at=row["transaction_at"],
            transaction_amt=float(row["transaction_amt"]),
            is_fraud=row["is_fraud"],
        )
        for row in rows
    ]


def _compute_features_by_transaction_id(rows: list[_TxnRow]) -> dict[int, TransactionFeatures]:
    """Compute rolling features for all rows grouped by ``derived_user_id``."""
    groups: dict[str, list[_TxnRow]] = defaultdict(list)
    feature_by_id: dict[int, TransactionFeatures] = {}

    for row in rows:
        if row.derived_user_id is None:
            feature_by_id[row.transaction_id] = _NULL_FEATURES
        else:
            groups[row.derived_user_id].append(row)

    for user_rows in groups.values():
        user_rows.sort(key=lambda row: (row.transaction_at, row.transaction_id))
        feature_by_id.update(_compute_bulk_user_group(user_rows))

    return feature_by_id


def _features_to_record(row: _TxnRow, features: TransactionFeatures) -> dict[str, object]:
    """Flatten a transaction row and its features into a training record."""
    record = asdict(features)
    record["transaction_id"] = row.transaction_id
    record["is_fraud"] = row.is_fraud
    return record


def compute_transaction_features_dataframe(
    spark: SparkSession | None = None,
    *,
    limit: int | None = 10_000,
    table: str | None = None,
    transaction_ids: Iterable[int] | None = None,
) -> pd.DataFrame:
    """Compute rolling behavioral features for many transactions efficiently.

    Loads transactions from Delta, computes features in memory using per-user
    sliding windows, then returns rows filtered by ``transaction_ids`` or
    truncated to ``limit`` in chronological order.
    """
    session = spark if spark is not None else get_spark()
    rows = _fetch_all_transaction_rows(session, table=table)
    empty_columns = [
        "transaction_id",
        "is_fraud",
        "velocity_1h",
        "cumulative_spend_24h",
        "avg_amount_ratio_30d",
        "avg_amount_ratio_90d",
    ]
    if not rows:
        return pd.DataFrame(columns=empty_columns)

    feature_by_id = _compute_features_by_transaction_id(rows)
    if transaction_ids is not None:
        row_by_id = {row.transaction_id: row for row in rows}
        export_rows = [
            row_by_id[int(transaction_id)]
            for transaction_id in transaction_ids
            if int(transaction_id) in row_by_id
        ]
    elif limit is None:
        export_rows = rows
    else:
        export_rows = rows[:limit]
    records = [
        _features_to_record(row, feature_by_id[row.transaction_id])
        for row in export_rows
    ]
    return pd.DataFrame.from_records(records)


def write_behavioral_features(
    spark: SparkSession | None = None,
    *,
    limit: int | None = 10_000,
    table: str | None = None,
    output_table: str | None = None,
    ensure_tables: bool = True,
) -> int:
    """Compute behavioral features and overwrite the gold Delta output table.

    Reads cleaned transactions from silver (or ``table``) and writes to gold
    ``behavioral_features`` (or ``output_table``).

    Returns:
        Number of rows written.
    """
    session = spark if spark is not None else get_spark()
    if ensure_tables:
        ensure_silver_tables(session)
        ensure_gold_tables(session)

    dataframe = compute_transaction_features_dataframe(
        session,
        limit=limit,
        table=table,
    )
    target = output_table or gold_table("behavioral_features")
    if dataframe.empty:
        session.createDataFrame([], schema=session.table(target).schema).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(target)
        return 0

    spark_df = session.createDataFrame(dataframe)
    (
        spark_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    return len(dataframe)


def compute_transaction_features(
    spark: SparkSession,
    *,
    derived_user_id: str | None,
    transaction_at: datetime,
    transaction_amt: float,
    transaction_id: int | None = None,
    table: str | None = None,
) -> TransactionFeatures:
    """Compute all configured fraud scoring features for a transaction."""
    return TransactionFeatures(
        velocity_1h=compute_velocity(
            spark,
            derived_user_id=derived_user_id,
            transaction_at=transaction_at,
            window_hours=1,
            transaction_id=transaction_id,
            table=table,
        ),
        cumulative_spend_24h=compute_cumulative_spend(
            spark,
            derived_user_id=derived_user_id,
            transaction_at=transaction_at,
            window_hours=24,
            transaction_id=transaction_id,
            table=table,
        ),
        avg_amount_ratio_30d=compute_avg_amount_ratio(
            spark,
            derived_user_id=derived_user_id,
            transaction_at=transaction_at,
            transaction_amt=transaction_amt,
            window_days=30,
            transaction_id=transaction_id,
            table=table,
        ),
        avg_amount_ratio_90d=compute_avg_amount_ratio(
            spark,
            derived_user_id=derived_user_id,
            transaction_at=transaction_at,
            transaction_amt=transaction_amt,
            window_days=90,
            transaction_id=transaction_id,
            table=table,
        ),
    )


def compute_transaction_features_from_row(
    spark: SparkSession,
    *,
    transaction_id: int,
    derived_user_id: str | None,
    transaction_at: datetime,
    transaction_amt: float,
    table: str | None = None,
) -> TransactionFeatures:
    """Compute features from explicit transaction field values."""
    return compute_transaction_features(
        spark,
        derived_user_id=derived_user_id,
        transaction_at=transaction_at,
        transaction_amt=transaction_amt,
        transaction_id=transaction_id,
        table=table,
    )
