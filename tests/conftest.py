import pytest
import sys
import builtins
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# Create a robust mock for dbutils secrets management
mock_dbutils = MagicMock()
mock_dbutils.secrets.get.return_value = "mocked-secret-value"

# Inject dbutils into the global system modules namespace BEFORE any src code imports it
sys.modules['dbutils'] = mock_dbutils

# Handle notebooks/scripts that check for a global 'dbutils' variable
builtins.dbutils = mock_dbutils

# Build a comprehensive builder adapter that handles all traditional PySpark syntax safely
class SafeSparkBuilder:
    def __init__(self):
        self._app_name = "HealthPlatformTests"
    
    def master(self, name):
        # Safely capture and ignore the local master parameter for Databricks Connect
        return self
        
    def appName(self, name):
        self._app_name = name
        return self
        
    def config(self, key=None, value=None, conf=None):
        return self
        
    def getOrCreate(self):
        try:
            from databricks.connect import DatabricksSession
            return DatabricksSession.builder.getOrCreate()
        except Exception:
            mock_session = MagicMock()
            mock_session.readStream = MagicMock()
            mock_session.read = MagicMock()
            return mock_session

class SafeSparkSessionWrapper:
    builder = SafeSparkBuilder()
    
    @staticmethod
    def getActiveSession():
        try:
            from databricks.connect import DatabricksSession
            return DatabricksSession.builder.getOrCreate()
        except Exception:
            mock_session = MagicMock()
            mock_session.readStream = MagicMock()
            mock_session.read = MagicMock()
            return mock_session

# Inject our adapter straight into the active Python system module tree before test files compile


@pytest.fixture(scope="session")
def spark():
    """Session-scoped Spark fixture for ALL tests.

    Priority order:
    1. DatabricksSession — used when DATABRICKS_HOST + TOKEN are configured
       (e.g. local dev machine with databricks-connect or Databricks Connect)
    2. SparkSession.builder.getOrCreate() — used when SPARK_REMOTE is already
       set in the process environment (pytest launched as a subprocess from a
       Databricks Serverless file/notebook; the unix socket is active).
       MUST NOT call .master() here — Spark Connect and a local master are
       mutually exclusive and raise CANNOT_CONFIGURE_SPARK_CONNECT_MASTER.
    3. local[*] SparkSession — pure CI (Azure DevOps, GitHub Actions) where
       there is no Databricks runtime in the environment.
    """
    import os

    # ── Path 1: In-process run (pytest.main() from notebook/file context) ─────
    # When pytest is invoked via pytest.main() rather than `python -m pytest`,
    # the calling process already has a live SparkSession.  Reuse it.
    try:
        active = SparkSession.getActiveSession()
        if active is not None:
            yield active
            return
    except Exception:
        pass

    # ── Path 2: DatabricksSession (remote sc:// URL configured) ────────────
    # Used on a developer laptop with databricks-connect configured.
    try:
        from databricks.connect import DatabricksSession
        session = DatabricksSession.builder.getOrCreate()
        yield session
        return
    except Exception:
        pass

    # ── Path 3: Fully local (CI — Azure DevOps, GitHub Actions) ────────────
    # No Databricks runtime: SPARK_CONNECT_MODE_ENABLED is not set, so
    # local[*] SparkSession creation works normally.
    try:
        session = (
            SparkSession.builder
            .master("local[*]")
            .appName("HealthPlatformTests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        yield session
        session.stop()
        return
    except Exception:
        pass

    # ── Path 4: No Spark available ──────────────────────────────────────
    # Databricks Serverless subprocess: SPARK_CONNECT_MODE_ENABLED=1 prevents
    # local[*] and the unix socket is process-local (not usable in subprocesses).
    # Mark as SKIPPED so CI reports are clean; tests run fully in Azure DevOps
    # (no Databricks runtime) and in notebooks via pytest.main().
    pytest.skip(
        "No SparkSession available: run tests via pytest.main() inside a notebook "
        "or in Azure DevOps CI where local[*] Spark is available."
    )


@pytest.fixture(scope="session")
def spark_session(spark):
    """Alias kept for backward compatibility with tests that use spark_session."""
    return spark
