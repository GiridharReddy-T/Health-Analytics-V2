import os

repo_base = "/Workspace/Users/chintuchinu1687@://gmail.com"
target_test_file = os.path.join(repo_base, "tests/test_transformation.py")

with open(target_test_file, 'w') as f:
    f.write("""import pytest
from unittest.mock import MagicMock
from src.transformation import SilverTransformer

def test_clean_users_logic(spark):
    \"\"\"Test if the transformation handles data casting rules correctly using a local Spark session.\"\"\"
    # 1. Setup a clean mock configuration wrapper
    config = MagicMock()
    config.db_name = "test_db"

    # 2. Initialize your production transformer module using the real local spark fixture context
    transformer = SilverTransformer(spark, config)

    # 3. Create a real, lightweight schema-aligned test input dataframe instead of a MagicMock
    input_data = [("12345", "9999", "AA:BB:CC:DD:EE:FF", 1696417200.0)]
    schema = ["user_id", "device_id", "mac_address", "registration_timestamp"]
    
    local_df = spark.createDataFrame(input_data, schema)

    # 4. Trigger the transformation logic sequence
    result_df = transformer.clean_registered_users(local_df)

    # 5. Core functional assertions to ensure columns were casted successfully
    # Collects the transformed data schema attributes fields safely
    schema_fields = {field.name: field.dataType.simpleString() for field in result_df.schema}
    
    assert "user_id" in schema_fields
    assert schema_fields["user_id"] == "long"
    assert schema_fields["device_id"] == "long"
""")

print("SUCCESS: Overwritten transformation tests to use real local Spark DataFrames!")
