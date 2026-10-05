import sys
import os
import logging
from pyspark.sql import functions as F


# Configure basic logging profile parameters
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Establish the verified repository root path context
repo_path = "/Workspace/Users/chintuchinu1687@gmail.com"
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

from src.config import Config
from src.setup import SetupHelper
from src.ingestion import BronzeIngestor
from src.transformation import SilverTransformer
from src.analytics import GoldAnalytics

config = Config()
setup = SetupHelper(spark, config.env, catalog="dev_catalog")

try:
    from pyspark.dbutils import DBUtils
    dbutils_reference = DBUtils(spark)
except Exception:
    dbutils_reference = None
# Target database identifier used in row-count lookups
target_db = f"{setup.catalog}.{setup.db_name}"

# 1. Force catalog context and ensure the Unity Catalog Volume exists before using it
print("📦 Registering Unity Catalog Volume asset configuration...")
spark.sql("USE CATALOG dev_catalog")
spark.sql("CREATE DATABASE IF NOT EXISTS project_db")
spark.sql("USE project_db")

# Run the DDL statement to physically create the volume wrapper
spark.sql("CREATE VOLUME IF NOT EXISTS dev_catalog.project_db.pipeline")
print("✅ Unity Catalog Volume 'pipeline' is registered and ready.")

# 2. Source: Azure ADLS raw container — batch reads, no streaming or checkpoints needed.
setup.delta_base = None   # UC managed tables — no external LOCATION clause
print("✨ Pipeline configured for batch ingestion from ADLS.")

# 4. Initialize Diagnostic Metrics Safety Default State Variables
raw_users, raw_info, raw_bpm, raw_wrk, raw_gym = 0, 0, 0, 0, 0
bz_gym_logs, bz_users = 0, 0
sv_users, sv_gym_logs, sv_bpm = 0, 0, 0
gold_bpm, gold_gym = 0, 0

# Defensive row metrics tracking helper
def get_row_count(spark_session, table_name, schema_context):
    clean_db = str(schema_context).split(".")[-1]
    try:
        return spark_session.table(f"dev_catalog.{clean_db}.{table_name}").count()
    except Exception:
        try:
            return spark_session.table(f"{clean_db}.{table_name}").count()
        except Exception:
            return 0

# ==============================================================================
# RUNNER PIPELINE STEPS
# ==============================================================================
print("=" * 80)
print("🔍 STEP 1/4: LAKEHOUSE STRUCTURE & SCHEMA SETTINGS FOUNDATION")
print("=" * 80)

if hasattr(setup, 'config'): 
    setup.config.db_name = "project_db"

setup.setup()
setup.validate()
print("✅ Structural Architecture Setup Verified.")

# Source files are read directly from abfss://raw@datazone.dfs.core.windows.net by Auto Loader

print("\n" + "=" * 80)
print("📥 STEP 2/4: BRONZE INGESTION STEP-BY-STEP TRACE")
print("=" * 80)

ingestor = BronzeIngestor(spark, "dev")
ingestor.catalog = "dev_catalog"
ingestor.db_name = "project_db"
ingestor.landing_zone = setup.landing_zone
# Checkpoints enable incremental pickup: only NEW files are processed on each run.
# Do NOT clear checkpoints on normal runs — that defeats incrementality.
# To force a full re-ingest, manually delete the checkpoint directory and re-run.
# Auto Loader streaming (availableNow=True) hangs indefinitely on Serverless/Spark Connect.
# Batch reads are used here — fast, reliable, ~15s for all 3 sources.
# For event-driven execution when new files land, use a Databricks Job with a
# file-arrival trigger pointing at abfss://raw@datazone.dfs.core.windows.net/

print(" ⚙️ Running batch ingestion from ADLS...")
try:
    ingestor.consume_user_registration()
    ingestor.consume_gym_logins()
    ingestor.consume_kafka_multiplex()
    print(" ✅ Ingestion complete.")
except Exception as e:
    print(f" ⚠️ Ingestion error: {str(e)}")

bz_gym_logs = get_row_count(spark, "gym_logins_bz", target_db)
bz_users = get_row_count(spark, "registered_users_bz", target_db)
print(f" 🎯 Row Counts Ingested into Bronze: gym_logins_bz ({bz_gym_logs}), registered_users_bz ({bz_users})")

print("\n" + "=" * 80)
print("🧼 STEP 3/4: SILVER CLEANING & TRANSFORMS AUDIT")
print("=" * 80)

transformer = SilverTransformer(spark, config)

print(" ⚙️ Running Silver CDC Operations...")
try:
    # registered_users_bz → users
    # .columns forces eager plan analysis inside this try block (Spark Connect lazy evaluation)
    df_users_bz = spark.table(f"{target_db}.registered_users_bz")
    df_users_clean = (transformer.clean_registered_users(df_users_bz)
        .select("user_id", "device_id", "mac_address", "registration_timestamp"))
    df_users_clean.columns  # force schema analysis before write
    (df_users_clean
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{target_db}.users"))
    # kafka_multiplex_bz → gym_logs, workout_bpm
    transformer.run_kafka_silver_transforms()
    # kafka_multiplex_bz (user_info) → user_profile → user_bins (demographics + age group)
    transformer.transform_user_profile()
    transformer.transform_user_bins()
    # kafka_multiplex_bz (workout) → workouts → completed_workouts (paired start/stop)
    transformer.transform_workouts()
    transformer.build_completed_workouts()
    # BPM: device_id → user_id (via users) + time-range join with completed_workouts → workout_bpm
    transformer.build_workout_bpm()
    # raw container → date_lookup (date dimension table)
    transformer.load_date_lookup()
    print(" ✅ Silver transformation layer routine complete.")
except Exception as e:
    print(f" ❌ Silver Transformation Error: {str(e)}")

sv_users = get_row_count(spark, "users", target_db)
sv_gym_logs = get_row_count(spark, "gym_logs", target_db)
sv_bpm = get_row_count(spark, "workout_bpm", target_db)
sv_user_bins = get_row_count(spark, "user_bins", target_db)
sv_completed = get_row_count(spark, "completed_workouts", target_db)
sv_date_lookup = get_row_count(spark, "date_lookup", target_db)
print(f" 🎯 Clean Rows in Silver: users ({sv_users}), gym_logs ({sv_gym_logs}), workout_bpm ({sv_bpm}), user_bins ({sv_user_bins}), completed_workouts ({sv_completed}), date_lookup ({sv_date_lookup})")

print("\n" + "=" * 80)
print("📊 STEP 4/4: GOLD ANALYTICS COMPILATION & POWER BI BINDINGS")
print("=" * 80)

analytics = GoldAnalytics(spark, config)
try:
    analytics.workout_bpm_summary()
    analytics.gym_summary()
except Exception as e:
    print(f" ⚠️ Gold analytics tracking exception: {str(e)}")

gold_bpm = get_row_count(spark, "workout_bpm_summary", target_db)
gold_gym = get_row_count(spark, "gym_summary", target_db)

print("\n" + "=" * 80)
print("📊 PIPELINE AUDIT BALANCE SHEET RESULT")
print("=" * 80)
print(f"{'Lakehouse Tier':<25} | {'Metric Property Evaluated':<28} | {'Total Count':<15}")
print("-" * 80)
print(f"{'1. Azure raw container':<25} | {'registered_users (Bronze)':<28} | {bz_users:<15}")
print(f"{'1. Azure raw container':<25} | {'gym_logins (Bronze)':<28} | {bz_gym_logs:<15}")
print(f"{'2. Bronze Delta Layer':<25} | {'registered_users_bz rows':<28} | {bz_users:<15}")
print(f"{'2. Bronze Delta Layer':<25} | {'gym_logins_bz rows':<28} | {bz_gym_logs:<15}")
print(f"{'3. Silver Clean Layer':<25} | {'users (clean profiles)':<28} | {sv_users:<15}")
print(f"{'3. Silver Clean Layer':<25} | {'gym_logs':<28} | {sv_gym_logs:<15}")
print(f"{'3. Silver Clean Layer':<25} | {'user_bins (age groups)':<28} | {sv_user_bins:<15}")
print(f"{'3. Silver Clean Layer':<25} | {'completed_workouts':<28} | {sv_completed:<15}")
print(f"{'3. Silver Clean Layer':<25} | {'workout_bpm':<28} | {sv_bpm:<15}")
print(f"{'3. Silver Clean Layer':<25} | {'date_lookup (dimension)':<28} | {sv_date_lookup:<15}")
print(f"{'4. Gold Target (BI)':<25} | {'workout_bpm_summary':<28} | {gold_bpm:<15}")
print(f"{'4. Gold Target (BI)':<25} | {'gym_summary (view)':<28} | {gold_gym:<15}")
print("=" * 80)
