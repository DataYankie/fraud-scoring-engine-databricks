# preprocessing

**Purpose:** sklearn-style feature preprocessing for fraud model training and scoring.

## Contents

- `preprocessor.py` - `FraudFeaturePreprocessor` (column selection, encoding, scaling)
- `columns.py` - feature column lists, drop columns, and missing-value constants

## Related

- [fraud_scoring_engine/README.md](../README.md)
- [training/README.md](../training/README.md) - assembles raw frames before preprocessing
- [notebooks/README.md](../../../notebooks/README.md) - XGBoost notebook uses the preprocessor
