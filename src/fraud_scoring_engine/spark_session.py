"""SparkSession helpers for Databricks and local development."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(*, app_name: str = "fraud-scoring-engine") -> SparkSession:
    """Return an active SparkSession, creating one if needed.

    On Databricks Runtime:
    - Tries to reuse the existing active session first (e.g., from notebook context)
    - Serverless/Standard (Spark Connect): Uses DatabricksSession
    - Classic clusters: Uses SparkSession.builder
    Locally, callers should configure Delta extensions before the first
    ``getOrCreate`` (see test fixtures).
    """
    # First, try to get an existing active session (works in notebook context)
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    
    # If no active session, try to create one
    try:
        # On Serverless/Standard compute (Spark Connect), use DatabricksSession
        from databricks.connect import DatabricksSession
        # Use serverless by default when no cluster context is available
        return DatabricksSession.builder.serverless(True).getOrCreate()
    except (ImportError, Exception):
        # On classic clusters or local dev, use standard SparkSession
        return SparkSession.builder.appName(app_name).getOrCreate()
