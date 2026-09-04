# silver

**Purpose:** Promote bronze Delta tables into cleaned silver entities used by
feature computation and training joins.

## Contents

- `promote.py` - blank-string cleanup, key filters, and `promote_bronze_to_silver`
- `__init__.py` - re-exports promote helpers

## Related

- [delta/README.md](../delta/README.md) - silver table DDL
- [ingest/README.md](../ingest/README.md) - bronze ingest that feeds silver
- [scripts/README.md](../../../scripts/README.md) - `promote_silver.py` CLI
