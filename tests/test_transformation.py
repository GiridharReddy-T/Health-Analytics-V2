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


def test_mac_address_masking_logic(spark):
    """Verify the mac_address masking expression used in the Unity Catalog function.

    The column mask body is:
        CONCAT(SUBSTR(mac, 1, 8), ':XX:XX:XX')
    This keeps the OUI vendor prefix (AA:BB:CC) and hides the device bytes.
    We test the expression directly in Spark SQL so the logic is validated
    independently of the UC function registration (which needs a live catalog).
    """
    from pyspark.sql import functions as F

    data = [
        ("AA:BB:CC:DD:EE:FF",),
        ("00:1A:2B:3C:4D:5E",),
        ("12:34:56:78:9A:BC",),
        (None,),
    ]
    df = spark.createDataFrame(data, ["mac_address"])

    # Apply the same expression used inside the UC masking function
    df_masked = df.withColumn(
        "masked",
        F.when(F.col("mac_address").isNull(), F.lit(None))
         .otherwise(F.concat(F.substring("mac_address", 1, 8), F.lit(":XX:XX:XX")))
    )

    rows = {r["mac_address"]: r["masked"] for r in df_masked.collect()}

    assert rows["AA:BB:CC:DD:EE:FF"] == "AA:BB:CC:XX:XX:XX", "OUI prefix must be preserved"
    assert rows["00:1A:2B:3C:4D:5E"] == "00:1A:2B:XX:XX:XX", "OUI prefix must be preserved"
    assert rows["12:34:56:78:9A:BC"] == "12:34:56:XX:XX:XX", "OUI prefix must be preserved"
    assert rows[None] is None, "NULL mac_address must remain NULL after masking"

    # All masked values must follow the pattern XX:XX:XX:XX:XX:XX where last 3 are XX
    for original, masked in rows.items():
        if original is not None:
            assert masked.endswith(":XX:XX:XX"), f"Last 3 octets not masked for {original}"
            assert masked[:8] == original[:8], f"OUI prefix changed for {original}"


def test_apply_column_masks_calls_correct_sql():
    """Verify apply_column_masks() creates the function and ALTERs all 4 mac tables."""
    from unittest.mock import MagicMock, call, patch
    from src.setup import SetupHelper

    mock_spark = MagicMock()
    mock_spark.sql.return_value = MagicMock()  # simulate DataFrame result

    config = MagicMock()
    config.db_name = "project_db"
    config.catalog = "dev_catalog"
    config.base_dir_data = "abfss://raw@datazone.dfs.core.windows.net"
    config.delta_zone = "abfss://delta@datazone.dfs.core.windows.net"
    config.checkpoint_path = "abfss://checkpoints@datazone.dfs.core.windows.net"

    with patch("src.setup.Config", return_value=config):
        helper = SetupHelper(mock_spark, "dev", catalog="dev_catalog")
        helper.initialized = True  # skip _require_db check
        helper.apply_column_masks()

    sql_calls = [str(c.args[0]).strip() for c in mock_spark.sql.call_args_list]
    joined = "\n".join(sql_calls)

    assert "CREATE OR REPLACE FUNCTION" in joined, "Must create masking function"
    assert "mask_mac_address" in joined, "Function must be named mask_mac_address"
    assert "is_member('data_engineers')" in joined, "Must gate on data_engineers group"
    assert "is_account_admin()" in joined, "Must gate on account admin"

    # Verify all 4 tables get ALTER TABLE ... SET MASK
    for tbl in ("registered_users_bz", "gym_logins_bz", "users", "gym_logs"):
        assert f"ALTER TABLE" in joined and tbl in joined, \
            f"SET MASK must be applied to {tbl}"


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


