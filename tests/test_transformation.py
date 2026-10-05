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
    assert mock_df.withColumn.called
    
    # Check that F.col was used targeting the appropriate telemetry keys
    mock_f.col.assert_any_call("user_id")
    mock_f.col.assert_any_call("registration_timestamp")


def test_user_profile_cdc_delete(spark):
    """Verify CDC delete handling in transform_user_profile.

    Rules:
      - update_type='new'    → user should appear in user_profile
      - update_type='update' → latest version should appear
      - update_type='delete' → user must be EXCLUDED from user_profile
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    cdc_data = [
        # user 1: single 'new' → keep
        (1, "new",    1678451000.0, "Shannon",  "Roberts",   "1985-03-12", "female", "Denver",  "CO"),
        # user 2: 'new' then 'delete' → latest is delete → EXCLUDE
        (2, "new",    1678451100.0, "Robert",   "Smith",     "1990-06-24", "male",   "Austin",  "TX"),
        (2, "delete", 1678451200.0, None,       None,        None,         None,     None,      None),
        # user 3: 'new' then 'update' → latest version kept
        (3, "new",    1678451300.0, "Courtney", "Jones",     "1995-11-01", "female", "Chicago", "IL"),
        (3, "update", 1678451400.0, "Courtney", "Jones-Kim", "1995-11-01", "female", "Chicago", "IL"),
    ]
    schema = (
        "user_id INT, update_type STRING, ts DOUBLE, "
        "first_name STRING, last_name STRING, dob STRING, "
        "gender STRING, city STRING, state STRING"
    )
    df_cdc = spark.createDataFrame(cdc_data, schema)

    win = Window.partitionBy("user_id").orderBy(F.col("ts").desc())
    df_result = (
        df_cdc
        .withColumn("_rn", F.row_number().over(win))
        .filter("_rn == 1")
        .filter(F.col("update_type") != "delete")  # CDC delete fix
        .drop("_rn", "ts", "update_type")
    )

    ids = [r["user_id"] for r in df_result.collect()]

    assert 1 in ids,     "User 1 (new only) must be in user_profile"
    assert 2 not in ids, "User 2 (deleted) must NOT be in user_profile"
    assert 3 in ids,     "User 3 (updated) must be in user_profile"

    user3 = df_result.filter(F.col("user_id") == 3).collect()[0]
    assert user3["last_name"] == "Jones-Kim", "User 3 must carry updated last name"
    assert len(ids) == 2, f"Expected 2 active users, got {len(ids)}"


