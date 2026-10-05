# =============================================================================
# 02_Silver_Gold_Pipeline.py
# Responsibility : Bronze Delta tables → Silver transforms → Gold analytics
# Trigger        : Databricks multi-task Job — runs after 01_Run_Pipeline.py succeeds
# =============================================================================
import sys
import os
import argparse
import logging
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── DABs variable injection ─────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="dev_catalog", help="UC catalog (dev/test/prod)")
parser.add_argument("--env",     default="dev",         help="Environment name")
args, _ = parser.parse_known_args()

os.environ["CATALOG_NAME"] = args.catalog
os.environ["APP_ENV"]      = args.env

repo_path = "/Workspace/Users/chintuchinu1687@gmail.com"
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

from src.config import Config
from src.setup import SetupHelper
from src.transformation import SilverTransformer
from src.analytics import GoldAnalytics

config    = Config()   # reads CATALOG_NAME + APP_ENV from env
catalog   = config.catalog
setup     = SetupHelper(spark, config.env, catalog=catalog)
target_db = f"{catalog}.{config.db_name}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE {config.db_name}")

def get_row_count(tbl):
    try:
        return spark.table(f"{target_db}.{tbl}").count()
    except Exception:
        return 0

# ==============================================================================
# STEP 1/2 — SILVER TRANSFORMS
# ==============================================================================
print("=" * 70)
print("🧼 STEP 1/2: SILVER CLEANING & TRANSFORMS")
print("=" * 70)

transformer = SilverTransformer(spark, config)

try:
    # registered_users_bz → users (clean timestamps, dedup)
    df_users_bz    = spark.table(f"{target_db}.registered_users_bz")
    df_users_clean = (transformer.clean_registered_users(df_users_bz)
        .select("user_id", "device_id", "mac_address", "registration_timestamp"))
    df_users_clean.columns  # force eager schema analysis (Spark Connect)
    (df_users_clean
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{target_db}.users"))

    # gym_logins_bz → gym_logs (unix epoch → timestamp, duration_minutes)
    transformer.run_kafka_silver_transforms()

    # user_info CDC → user_profile (latest per user, deletes excluded)
    transformer.transform_user_profile()
    # user_profile → user_bins (age_group + demographics)
    transformer.transform_user_bins()

    # workout topic → workouts (raw start/stop events)
    transformer.transform_workouts()
    # workouts → completed_workouts (matched start/stop pairs)
    transformer.build_completed_workouts()

    # bpm topic → workout_bpm (device_id→user_id + time-range join)
    transformer.build_workout_bpm()

    # raw container → date_lookup (date dimension)
    transformer.load_date_lookup()

    print(" ✅ Silver transforms complete.")
except Exception as e:
    raise RuntimeError(f"🛑 Silver transformation failed: {e}") from e

# ==============================================================================
# STEP 2/2 — GOLD ANALYTICS
# ==============================================================================
print("\n" + "=" * 70)
print("📊 STEP 2/2: GOLD ANALYTICS")
print("=" * 70)

analytics = GoldAnalytics(spark, config)
try:
    analytics.workout_bpm_summary()  # min/avg/max BPM per workout session per user
    analytics.gym_summary()          # time in gym + time exercising per session
    print(" ✅ Gold analytics complete.")
except Exception as e:
    raise RuntimeError(f"🛑 Gold analytics failed: {e}") from e

# ==============================================================================
# AUDIT
# ==============================================================================
sv_users    = get_row_count("users")
sv_gym_logs = get_row_count("gym_logs")
sv_bins     = get_row_count("user_bins")
sv_cw       = get_row_count("completed_workouts")
sv_bpm      = get_row_count("workout_bpm")
sv_dl       = get_row_count("date_lookup")
gold_bpm    = get_row_count("workout_bpm_summary")
gold_gym    = get_row_count("gym_summary")

print("\n" + "=" * 70)
print("📊 SILVER + GOLD AUDIT")
print("=" * 70)
print(f"  Silver  users               : {sv_users:>8} rows")
print(f"  Silver  gym_logs            : {sv_gym_logs:>8} rows")
print(f"  Silver  user_bins           : {sv_bins:>8} rows")
print(f"  Silver  completed_workouts  : {sv_cw:>8} rows")
print(f"  Silver  workout_bpm         : {sv_bpm:>8} rows")
print(f"  Silver  date_lookup         : {sv_dl:>8} rows")
print(f"  Gold    workout_bpm_summary : {gold_bpm:>8} rows")
print(f"  Gold    gym_summary (view)  : {gold_gym:>8} rows")
print("=" * 70)
print("✅ Silver+Gold pipeline complete. Power BI Gold tables are refreshed.")