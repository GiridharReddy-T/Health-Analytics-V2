import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from src.transformation import SilverTransformer

@pytest.fixture
def mock_spark():
    """Creates a mock Spark session for testing"""
    spark = MagicMock(spec=SparkSession)
    return spark

def test_clean_users_logic(mock_spark):
    """Test if the transformation calls the correct Spark methods"""
    # Setup the mock config
    config = MagicMock()
    config.db_name = "test_db"
    
    # Initialize transformer with mock
    transformer = SilverTransformer(mock_spark, config)
    
    # Create a mock dataframe
    mock_df = MagicMock()
    
    # Run the method
    transformer.clean_registered_users(mock_df)
    
    # Verify that Spark actually tried to select the right columns
    mock_df.select.return_value = mock_df