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
    """Shared session-scoped Spark fixture for ALL tests.
    Uses local[*] in CI (no Databricks cluster); falls back to DatabricksSession when available.
    """
    try:
        from databricks.connect import DatabricksSession
        session = DatabricksSession.builder.getOrCreate()
    except Exception:
        session = SparkSession.builder \
            .master("local[*]") \
            .appName("HealthPlatformTests") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def spark_session(spark):
    """Alias kept for backward compatibility with tests that use spark_session."""
    return spark
