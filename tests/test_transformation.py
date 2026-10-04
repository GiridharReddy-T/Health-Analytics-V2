import pytest
from unittest.mock import MagicMock, patch
from src.transformation import SilverTransformer

@patch("src.transformation.F")
def test_clean_users_logic(mock_f):
    """Test if the transformation handles data casting rules correctly by mocking PySpark functions."""
    # 1. Mock the column function behavior to bypass JVM checks completely
    mock_column = MagicMock()
    mock_f.col.return_value = mock_column
    
    # Crucial fix: ensure .cast() returns a mock object so the chain can continue
    mock_column.cast.return_value = mock_column 

    # 2. Setup clean mock wrappers for configuration and spark sessions
    mock_spark = MagicMock()
    config = MagicMock()
    config.db_name = "test_db"

    # 3. Initialize your production transformer module using the mocks
    transformer = SilverTransformer(mock_spark, config)

    # 4. Create a mock dataframe that tracks chained withColumn calls
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df

    # 5. Trigger the transformation logic sequence
    transformer.clean_registered_users(mock_df)

    # 6. Verify that withColumn was called to cast the columns properly
    assert mock_df.withColumn.called, "Expected withColumn to be invoked on the DataFrame"
    
    # Check that F.col was used targeting the appropriate telemetry keys
    mock_f.col.assert_any_call("user_id")
    # This will now pass as long as your src/transformation.py applies F.col("device_id")
    mock_f.col.assert_any_call("device_id") 
