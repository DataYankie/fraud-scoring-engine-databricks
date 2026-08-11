"""IEEE-CIS data ingestion into Unity Catalog Delta tables."""

__all__ = ["IngestResult", "ingest_train_transactions"]


def __getattr__(name: str):
    if name in __all__:
        from fraud_scoring_engine.ingest.loader import IngestResult, ingest_train_transactions

        return {
            "IngestResult": IngestResult,
            "ingest_train_transactions": ingest_train_transactions,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
