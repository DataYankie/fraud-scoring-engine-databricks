"""SparkSession helpers for Databricks and local development."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(*, app_name: str = "fraud-scoring-engine") -> SparkSession:
    """Return an active SparkSession, creating one if needed.

    On Databricks Runtime, ``getOrCreate`` reuses the cluster session.
    Locally, callers should configure Delta extensions before the first
    ``getOrCreate`` (see test fixtures).
    """
    return SparkSession.builder.appName(app_name).getOrCreate()
