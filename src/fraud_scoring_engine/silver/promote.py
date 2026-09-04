"""Promote bronze Delta tables into cleaned silver entities."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession  # type: ignore
from pyspark.sql import functions as F # type: ignore

from fraud_scoring_engine.config import bronze_table, silver_table
from fraud_scoring_engine.delta.schema import ensure_silver_tables
from fraud_scoring_engine.spark_session import get_spark

TRANSACTION_STRING_COLUMNS = (
    "derived_user_id",
    "product_cd",
    "card4",
    "card6",
    "p_emaildomain",
    "r_emaildomain",
)

IDENTITY_STRING_COLUMNS = (
    "id_30",
    "id_31",
    "device_type",
    "device_info",
)


@dataclass(frozen=True)
class PromoteResult:
    """Summary counts from a bronze → silver promotion."""

    transactions_written: int
    identities_written: int


def _blank_strings_to_null(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    """Replace empty / whitespace-only strings with null for listed columns."""
    available = set(df.columns)
    for name in columns:
        if name not in available:
            continue
        col = F.col(name)
        df = df.withColumn(
            name,
            F.when(col.isNull() | (F.trim(col.cast("string")) == ""), F.lit(None)).otherwise(col),
        )
    return df


def clean_transactions_spark(df: DataFrame) -> DataFrame:
    """Apply silver conformance rules to bronze transactions.

    Keeps nulls as nulls (no ML imputation). Drops rows missing keys or
    timestamps and normalizes blank strings to null.
    """
    cleaned = _blank_strings_to_null(df, TRANSACTION_STRING_COLUMNS)
    return cleaned.filter(
        F.col("transaction_id").isNotNull() & F.col("transaction_at").isNotNull()
    )


def clean_identities_spark(df: DataFrame) -> DataFrame:
    """Apply silver conformance rules to bronze transaction identities."""
    cleaned = _blank_strings_to_null(df, IDENTITY_STRING_COLUMNS)
    return cleaned.filter(F.col("transaction_id").isNotNull())


def _overwrite_table(df: DataFrame, table: str) -> int:
    count = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    return count


def promote_bronze_to_silver(
    *,
    spark: SparkSession | None = None,
    dry_run: bool = False,
    ensure_tables: bool = True,
) -> PromoteResult:
    """Copy cleaned bronze transactions/identities into silver Delta tables.

    Args:
        spark: Optional SparkSession; defaults to :func:`get_spark`.
        dry_run: If True, report counts only; do not write silver tables.
        ensure_tables: If True, create silver schema/tables when missing.

    Returns:
        Row counts written (or that would be written on dry run).
    """
    session = spark if spark is not None else get_spark()
    if ensure_tables and not dry_run:
        ensure_silver_tables(session)

    transactions = clean_transactions_spark(session.table(bronze_table("transactions")))
    identities = clean_identities_spark(session.table(bronze_table("transaction_identities")))
    txn_count = transactions.count()
    id_count = identities.count()

    if dry_run:
        print(
            f"Dry run: would promote {txn_count} transactions and "
            f"{id_count} identities to silver"
        )
        return PromoteResult(transactions_written=txn_count, identities_written=id_count)

    txn_written = _overwrite_table(transactions, silver_table("transactions"))
    id_written = _overwrite_table(identities, silver_table("transaction_identities"))
    print(
        f"Silver promote complete: transactions={txn_written}, identities={id_written}"
    )
    return PromoteResult(transactions_written=txn_written, identities_written=id_written)
