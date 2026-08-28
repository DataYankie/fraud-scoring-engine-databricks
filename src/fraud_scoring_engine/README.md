# fraud_scoring_engine

**Purpose:** Core library for the fraud-scoring pipeline - Unity Catalog config, Spark session helpers, shared feature definitions, and subpackages for ingest, Delta schema, preprocessing, and training.

## Contents

- `config.py` - Unity Catalog, Volume, and MLflow path configuration
- `features.py` - Spark/Delta queries for per-transaction fraud scoring features
- `spark_session.py` - SparkSession helpers for Databricks and local development
- `delta/` - Delta Lake bronze table schemas (see [delta/README.md](delta/README.md))
- `ingest/` - IEEE-CIS CSV ingestion into Delta (see [ingest/README.md](ingest/README.md))
- `preprocessing/` - sklearn-style feature preprocessing (see [preprocessing/README.md](preprocessing/README.md))
- `training/` - training dataset assembly and MLflow setup (see [training/README.md](training/README.md))

## Related

- [src/README.md](../README.md)
- [tests/README.md](../../tests/README.md)
