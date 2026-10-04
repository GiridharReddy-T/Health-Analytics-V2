import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from src.transformation import SilverTransformer

def test_clean_users_logic():
    """Test if the transformation handles data casting rules correctly using a real isolated local Spark session."""
    # 1. Spin up a real, lightweight local Spark session to satisfy F.col internal checks
    local_spark = SparkSession.builder \
        .master("local[*]") \
        .appName("TransformationLocalTest") \
        .getOrCreate()

    try:
        # 2. Setup a clean mock configuration wrapper
        config = MagicMock()
        config.db_name = "test_db"
    
        # 3. Initialize your production transformer module with the real local session
        transformer = SilverTransformer(local_spark, config)
    
        # 4. Create a real, lightweight schema-aligned test input dataframe
        input_data = [("12345", "9999", "AA:BB:CC:DD:EE:FF", 1696417200.0)]
        schema = ["user_id", "device_id", "mac_address", "registration_timestamp"]
        
        local_df = local_spark.createDataFrame(input_data, schema)
    
        # 5. Trigger the transformation logic sequence
        result_df = transformer.clean_registered_users(local_df)
    
        # 6. Core functional assertions to ensure columns were casted successfully
        schema_fields = {field.name: field.dataType.simpleString() for field in result_df.schema}
        
        assert "user_id" in schema_fields
        assert schema_fields["user_id"] == "long"
        assert schema_fields["device_id"] == "long"
        
    finally:
        # Always terminate the local thread session cleanly to free machine resources
        local_spark.stop()
