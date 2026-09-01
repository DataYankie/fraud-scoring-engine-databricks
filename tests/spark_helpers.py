"""Shared Spark/Delta helpers for tests."""

from __future__ import annotations


def write_delta_table(spark, table: str, rows: list[dict]) -> None:
    """Write rows to a Delta table using the table schema when possible.

    PySpark cannot infer types for all-null columns; use the existing table
    schema for those cases. Fall back to pandas inference when rows include
    columns beyond the table DDL (e.g. train_features C1/D1 on first write).
    """
    import pandas as pd

    table_schema = spark.table(table).schema
    if not rows:
        df = spark.createDataFrame([], schema=table_schema)
    else:
        schema_names = {field.name for field in table_schema.fields}
        row_names = set(rows[0].keys())
        if row_names <= schema_names:
            df = spark.createDataFrame(rows, schema=table_schema)
        else:
            df = spark.createDataFrame(pd.DataFrame(rows))

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
