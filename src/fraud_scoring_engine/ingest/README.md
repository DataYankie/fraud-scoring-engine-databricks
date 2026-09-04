# ingest

**Purpose:** IEEE-CIS transaction CSV loading, column mapping, and Spark transforms
for bronze ingest (and optional silver promotion) into Delta tables.

## Contents

- `loader.py` - main ingest entry point (`ingest_train_transactions`) and `IngestResult`
- `columns.py` - operational column definitions and mappings
- `paths.py` - Volume and raw CSV path helpers
- `transforms.py` - pandas-side data transformations
- `spark_transforms.py` - Spark DataFrame transforms for MERGE operations

## Related

- [fraud_scoring_engine/README.md](../README.md)
- [delta/README.md](../delta/README.md) - target table schemas
- [silver/README.md](../silver/README.md) - bronze → silver promotion
- [tests/ingest/README.md](../../../tests/ingest/README.md) - unit tests for this module
- [scripts/README.md](../../../scripts/README.md) - `ingest_ieee_transactions.py` CLI
