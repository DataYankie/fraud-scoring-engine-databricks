# delta

**Purpose:** Delta Lake medallion table schemas and create-if-missing helpers for Unity Catalog.

## Contents

- `schema.py` - table DDLs plus `ensure_bronze_tables`, `ensure_silver_tables`,
  `ensure_gold_tables`, and `ensure_medallion_tables`
- `__init__.py` - re-exports the ensure helpers

## Layer map

| Layer | Tables |
|-------|--------|
| bronze | `transactions`, `transaction_identities`, `train_features` |
| silver | `transactions`, `transaction_identities` |
| gold | `behavioral_features`, `fraud_alerts` |

## Related

- [fraud_scoring_engine/README.md](../README.md)
- [ingest/README.md](../ingest/README.md) - writes bronze tables
- [silver/README.md](../silver/README.md) - promotes bronze → silver
- [scripts/README.md](../../../scripts/README.md) - pipeline commands
