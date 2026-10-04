import pytest
import sys
from unittest.mock import MagicMock
from src.transformation import SilverTransformer

def test_clean_users_logic():
    """Test if the transformation handles data casting rules correctly using an unpatched local Spark session."""
    # 1. Force reload the core pyspark builder module to bypass the conftest monkeypatch hooks
    import pyspark.sql.session
    from pyspark.sql.session import SparkSession
    
    # 2. Spin up a true, operational local JVM Spark cluster instance
    local_spark = SparkSession.builder \
        .master("local") \
        .appName("TransformationIsolatedJVMTest") \
        .getOrCreate()

    try:
        # 3. Setup a clean mock configuration wrapper
        config = MagicMock()
        config.db_name = "test_db"
    
        # 4. Initialize your production transformer module using the verified JVM session context
        transformer = SilverTransformer(local_spark, config)
    
        # 5. Create a real, lightweight schema-aligned test input dataframe
        input_data = [("12345", "9999", "AA:BB:CC:DD:EE:FF", 1696417200.0)]
        schema = ["user_id", "device_id", "mac_address", "registration_timestamp"]
        
        local_df = local_spark.createDataFrame(input_data, schema)
    
        # 6. Trigger the transformation logic sequence
        result_df = transformer.clean_registered_users(local_df)
    
        # 7. Core functional assertions to ensure columns were casted successfully
        schema_fields = {field.name: field.dataType.simpleString() for field in result_df.schema}
        
        assert "user_id" in schema_fields
        assert schema_fields["user_id"] == "long"
        assert schema_fields["device_id"] == "long"
        
    finally:
        # 8. Cleanly stop the JVM engine to free system runner memory blocks
        local_spark.stop()
