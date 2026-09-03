"""Orchestrate IEEE CSV ingestion into Unity Catalog Delta tables."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession # type: ignore

from fraud_scoring_engine.config import bronze_table
from fraud_scoring_engine.delta.schema import ensure_bronze_tables, ensure_medallion_tables
from fraud_scoring_engine.ingest.paths import IeeeDataPaths, ieee_data_paths
from fraud_scoring_engine.ingest.spark_transforms import (
    count_identity_rows_spark,
    prepare_identities_spark,
    prepare_transactions_spark,
    split_train_features_spark,
)
from fraud_scoring_engine.silver import promote_bronze_to_silver
from fraud_scoring_engine.spark_session import get_spark

TRANSACTION_MERGE_COLUMNS = (
    "transaction_id",
    "derived_user_id",
    "is_fraud",
    "transaction_amt",
    "product_cd",
    "transaction_dt",
    "transaction_at",
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
)

IDENTITY_MERGE_COLUMNS = (
    "transaction_id",
    "id_30",
    "id_31",
    "device_type",
    "device_info",
)


@dataclass(frozen=True)
class IngestResult:
    """Summary counts from an ingest run."""

    rows_read: int
    rows_merged: int
    identities_merged: int
    feature_rows: int
    features_table: str | None
    silver_transactions: int
    silver_identities: int


def _read_csv(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required CSV not found: {path}")
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(path))
    )


def load_merged_train_spark(
    spark: SparkSession,
    transaction_path: Path,
    identity_path: Path,
    *,
    limit: int | None = None,
) -> DataFrame:
    """Load and left-join train transaction and identity CSVs with Spark."""
    transactions = _read_csv(spark, transaction_path)
    if limit is not None:
        transactions = transactions.limit(limit)
    identities = _read_csv(spark, identity_path)
    return transactions.join(identities, on="TransactionID", how="left")


def _merge_delta(
    spark: SparkSession,
    source: DataFrame,
    *,
    target_table: str,
    key: str,
    columns: tuple[str, ...],
) -> int:
    """MERGE ``source`` into ``target_table`` on ``key``; return source row count."""
    count = source.count()
    if count == 0:
        return 0

    view = f"_ingest_source_{target_table.replace('.', '_')}"
    source.createOrReplaceTempView(view)

    set_clause = ", ".join(f"t.{col} = s.{col}" for col in columns if col != key)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"s.{col}" for col in columns)

    spark.sql(
        f"""
        MERGE INTO {target_table} AS t
        USING {view} AS s
        ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )
    return count


def _merge_features(spark: SparkSession, features: DataFrame, table: str) -> int:
    """MERGE ``features`` into ``table`` on ``TransactionID``; return source row count."""
    columns = tuple(features.columns)
    return _merge_delta(
        spark,
        features,
        target_table=table,
        key="TransactionID",
        columns=columns,
    )


def _write_features(spark: SparkSession, features: DataFrame, table: str) -> int:
    """Upsert IEEE feature rows into ``table``.

    Appends with ``mergeSchema`` when the target table is empty; otherwise
    MERGEs on ``TransactionID``. Returns the source row count.
    """
    count = features.count()
    if count == 0:
        return 0

    if spark.table(table).count() == 0:
        (
            features.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table)
        )
        return count

    return _merge_features(spark, features, table)


def ingest_train_transactions(
    *,
    spark: SparkSession | None = None,
    data_dir: Path | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    skip_tables: bool = False,
    skip_features: bool = False,
    promote_silver: bool = True,
    ensure_tables: bool = True,
) -> IngestResult:
    """Load train IEEE data from Volume CSVs into bronze Delta tables.

    Operational columns are MERGEd into bronze ``transactions`` /
    ``transaction_identities``. Remaining CSV columns (plus ``TransactionID``)
    are MERGEd into bronze ``train_features``. When ``promote_silver`` is True
    and operational tables were written, cleaned copies are promoted to silver.

    Args:
        spark: Optional SparkSession; defaults to :func:`get_spark`.
        data_dir: Optional override for Volume ``raw/``.
        limit: If set, process only the first ``limit`` transaction rows.
        dry_run: If True, report counts only; do not write Delta tables.
        skip_tables: If True, write train_features only.
        skip_features: If True, MERGE operational tables only.
        promote_silver: If True, promote bronze operational tables to silver
            after a successful non-dry-run table MERGE.
        ensure_tables: If True, create medallion schemas/tables when missing.

    Returns:
        Summary counts for the ingest run.
    """
    if skip_tables and skip_features:
        msg = "At least one of Delta table ingest or train_features write must be enabled"
        raise ValueError(msg)

    session = spark if spark is not None else get_spark()
    paths: IeeeDataPaths = ieee_data_paths(data_dir)

    if ensure_tables and not dry_run:
        if promote_silver and not skip_tables:
            ensure_medallion_tables(session)
        else:
            ensure_bronze_tables(session)

    merged = load_merged_train_spark(
        session,
        paths.train_transaction,
        paths.train_identity,
        limit=limit,
    )
    rows_read = merged.count()

    feature_rows = 0
    features_table_name: str | None = None
    rows_merged = 0
    identities_merged = 0
    silver_transactions = 0
    silver_identities = 0

    if not skip_features:
        features = split_train_features_spark(merged)
        feature_rows = features.count()
        features_table_name = bronze_table("train_features")
        if not dry_run:
            _write_features(session, features, features_table_name)

    if dry_run:
        identity_count = count_identity_rows_spark(merged)
        print(
            f"Dry run: {rows_read} transactions, {identity_count} with identity data, "
            f"{feature_rows} train_features rows"
        )
        if features_table_name is not None:
            print(f"train_features target: {features_table_name}")
        return IngestResult(
            rows_read=rows_read,
            rows_merged=0,
            identities_merged=identity_count,
            feature_rows=feature_rows,
            features_table=features_table_name,
            silver_transactions=0,
            silver_identities=0,
        )

    if not skip_tables:
        transactions_df = prepare_transactions_spark(merged)
        identities_df = prepare_identities_spark(merged)
        rows_merged = _merge_delta(
            session,
            transactions_df,
            target_table=bronze_table("transactions"),
            key="transaction_id",
            columns=TRANSACTION_MERGE_COLUMNS,
        )
        identities_merged = _merge_delta(
            session,
            identities_df,
            target_table=bronze_table("transaction_identities"),
            key="transaction_id",
            columns=IDENTITY_MERGE_COLUMNS,
        )
        if promote_silver:
            promote = promote_bronze_to_silver(
                spark=session,
                ensure_tables=ensure_tables,
            )
            silver_transactions = promote.transactions_written
            silver_identities = promote.identities_written

    parts = [
        f"read={rows_read}",
        f"merged={rows_merged}",
        f"identities={identities_merged}",
    ]
    if not skip_features:
        parts.append(f"feature_rows={feature_rows}")
        parts.append(f"features_table={features_table_name}")
    if silver_transactions or silver_identities:
        parts.append(f"silver_transactions={silver_transactions}")
        parts.append(f"silver_identities={silver_identities}")
    print(f"Ingest complete: {', '.join(parts)}")

    return IngestResult(
        rows_read=rows_read,
        rows_merged=rows_merged,
        identities_merged=identities_merged,
        feature_rows=feature_rows,
        features_table=features_table_name,
        silver_transactions=silver_transactions,
        silver_identities=silver_identities,
    )
