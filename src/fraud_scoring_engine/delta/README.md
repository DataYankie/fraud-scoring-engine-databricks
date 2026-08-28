# delta

**Purpose:** Delta Lake bronze table schemas and bootstrap helpers for Unity Catalog tables.

## Contents

- `schema.py` - table definitions and `ensure_bronze_tables` to create bronze tables if missing
- `__init__.py` - re-exports `ensure_bronze_tables`

## Related

- [fraud_scoring_engine/README.md](../README.md)
- [ingest/README.md](../ingest/README.md) - writes data into these tables
- [scripts/README.md](../../../scripts/README.md) - pipeline commands that call `ensure_bronze_tables`
