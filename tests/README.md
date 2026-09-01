# tests

**Purpose:** pytest suite for the `fraud_scoring_engine` library - config, features, preprocessing, training, and MLflow helpers.

## Contents

- `conftest.py` - shared pytest fixtures
- `test_config.py` - Unity Catalog and path configuration
- `test_features.py` - fraud scoring feature queries
- `test_preprocessing.py` - `FraudFeaturePreprocessor`
- `test_split.py` - time-based dataset splits
- `test_dataset.py` - training frame assembly
- `test_mlflow_utils.py` - MLflow setup helpers
- `test_paths.py` - Volume and CSV path helpers
- `ingest/` - ingest module tests (see [ingest/README.md](ingest/README.md))

## Related

- [fraud_scoring_engine/README.md](../src/fraud_scoring_engine/README.md)
- [.github/workflows/README.md](../.github/workflows/README.md) - CI runs these tests on PRs
