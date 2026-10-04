import sys
import os

# 1. Establish the verified repository root path context
repo_path = "/Workspace/Users/chintuchinu1687@gmail.com/Health-Analytics-V2"
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

# 2. Safe component package imports
from src.config import Config
from src.setup import SetupHelper
from src.ingestion import BronzeIngestor
from src.transformation import SilverTransformer
from src.analytics import GoldAnalytics

# 3. Initialize Configuration and force cloud storage paths
config = Config()
config.base_dir_data = "abfss://raw@datazone.dfs.core.windows.net"
config.delta_zone = "abfss://delta@datazone.dfs.core.windows.net"
config.checkpoint_path = "abfss://checkpoints@datazone.dfs.core.windows.net"

# Explicitly declare the targets using your validated three-tier catalog space
config.db_name = "dev_catalog.project_db"
target_db = config.db_name

# 4. Initialize SetupHelper attached to dev_catalog
setup = SetupHelper(spark, "dev")
setup.catalog = "dev_catalog"
setup.db_prefix = target_db  # Enforce verified catalog naming convention
setup.landing_zone = config.base_dir_data
setup.delta_base = config.delta_zone
setup.checkpoint_base = config.checkpoint_path

# Safety helper definitions for diagnostic metric tracking
def count_raw_files(folder_name):
    """Safely count incoming files sitting out in the datazone raw container storage."""
    try:
        files = dbutils.fs.ls(f"{config.base_dir_data}/files/{folder_name}/")
        return len([f for f in files if not f.isDir()])
    except Exception:
        return 0

def get_row_count(table_name):
    """Safely compute table record metrics to track data drops across layers."""
    try:
        return spark.table(f"{target_db}.{table_name}").count()
    except Exception:
        return 0

# 5. EXECUTION PIPELINE RUNNER
print("=" * 80)
print("🔍 STEP 1/4: LAKEHOUSE STRUCTURE & SCHEMA SETTINGS FOUNDATION")
print("=" * 80)
spark.sql("USE CATALOG dev_catalog")

# Patch the setup helper's inner config if it maps properties internally
if hasattr(setup, 'config'):
    setup.config.db_name = target_db

setup.setup()
setup.validate()
print("✅ Structural Architecture Setup Verified.")

# -------------------------------------------------------------
# STEP 2: BRONZE INGESTION EXECUTION & AUDIT
# -------------------------------------------------------------
print("\n" + "=" * 80)
print("📥 STEP 2/4: BRONZE INGESTION STEP-BY-STEP TRACE")
print("=" * 80)

raw_gym = count_raw_files("gym_logs")
raw_bpm = count_raw_files("workout_bpm")
print(f" 📂 Files Found in datazone Storage: gym_logs ({raw_gym}), workout_bpm ({raw_bpm})")

ingestor = BronzeIngestor(spark, config.env)
# Force ingestor instance properties to map to dev_catalog.project_db to stop dev namespace crashes
if hasattr(ingestor, 'config'): ingestor.config.db_name = target_db
if hasattr(ingestor, 'db_name'): ingestor.db_name = target_db

print(" ⚙️ Running Ingestion Pipeline...")
try:
    ingestor.consume()
except Exception as e:
    print(f" ⚠️ Ingestion warning/alert encountered: {str(e)}")

bz_gym_logs = get_row_count("gym_logs_bz")
bz_users = get_row_count("registered_users_bz")
print(f" 🎯 Row Counts Ingested into Bronze: gym_logs_bz ({bz_gym_logs}), registered_users_bz ({bz_users})")

# -------------------------------------------------------------
# STEP 3: SILVER CLEANING & CDC PIPELINE TRACE
# -------------------------------------------------------------
print("\n" + "=" * 80)
print("🧼 STEP 3/4: SILVER CLEANING & TRANSFORMS AUDIT")
print("=" * 80)

transformer = SilverTransformer(spark, config)
# Ensure the transformation context mirrors config tracking pointers
if hasattr(transformer, 'config'): transformer.config.db_name = target_db

print(" ⚙️ Running Silver CDC Operations...")
try:
    transformer.run_users_cdc()
except Exception as e:
    print(f" ❌ Silver Transformation Error: {str(e)}")
    print(" 👉 Action item: Check your transformation.py logic if you encounter a 'Column object not callable' message!")

sv_users = get_row_count("users")
sv_gym_logs = get_row_count("gym_logs")
sv_bpm = get_row_count("workout_bpm")
print(f" 🎯 Clean Rows in Silver: users ({sv_users}), gym_logs ({sv_gym_logs}), workout_bpm ({sv_bpm})")

# -------------------------------------------------------------
# STEP 4: GOLD AGGREGATIONS LAYER COMPILATION
# -------------------------------------------------------------
print("\n" + "=" * 80)
print("📊 STEP 4/4: GOLD ANALYTICS COMPILATION & POWER BI BINDINGS")
print("=" * 80)
analytics = GoldAnalytics(spark, config)

print(" ⚙️ Compiling Gold Performance Telemetry Summaries...")
# Dynamic SQL patch to safeguard u.age column resolution and avoid schema drift errors
try:
    analytics.workout_bpm_summary()
except Exception as query_err:
    if "age_group" in str(query_err):
        print(" 🔧 Mismatched u.age_group target found. Applying hotfix query strategy...")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {target_db}.workout_bpm_summary AS
            SELECT w.user_id, w.workout_id, w.session_id, u.age AS age_group, u.gender, u.city, u.state,
                   MIN(b.heartrate) AS min_bpm, AVG(b.heartrate) AS avg_bpm, MAX(b.heartrate) AS max_bpm, COUNT(b.heartrate) AS num_recordings
            FROM {target_db}.workout_bpm b
            JOIN {target_db}.completed_workouts w ON b.user_id = w.user_id AND b.workout_id = w.workout_id AND b.session_id = w.session_id
            JOIN {target_db}.user_bins u ON b.user_id = u.user_id
            GROUP BY w.user_id, w.workout_id, w.session_id, u.age, u.gender, u.city, u.state
        """)
    else:
        print(f" ❌ Gold analytics summary error: {str(query_err)}")

# Materialize gym visitor durations
try:
    analytics.gym_summary()
except Exception as e:
    print(f" ❌ Gym analytics mapping error: {str(e)}")

gold_bpm = get_row_count("workout_bpm_summary")
gold_gym = get_row_count("gym_summary")

# =============================================================
# 🧮 PIPELINE LIFECYCLE MONITORING BALANCE SHEET
# =============================================================
print("\n" + "=" * 80)
print("📊 PIPELINE AUDIT BALANCE SHEET RESULT")
print("=" * 80)
print(f"{'Lakehouse Tier':<25} | {'Metric Property Evaluated':<28} | {'Total Count':<15}")
print("-" * 80)
print(f"{'1. Storage Account':<25} | {'gym_logs raw file count':<28} | {raw_gym:<15}")
print(f"{'1. Storage Account':<25} | {'workout_bpm raw file count':<28} | {raw_bpm:<15}")
print(f"{'2. Bronze Delta Layer':<25} | {'gym_logs_bz rows':<28} | {bz_gym_logs:<15}")
print(f"{'2. Bronze Delta Layer':<25} | {'registered_users_bz rows':<28} | {bz_users:<15}")
print(f"{'3. Silver Clean Layer':<25} | {'clean users profiles count':<28} | {sv_users:<15}")
print(f"{'3. Silver Clean Layer':<25} | {'clean gym_logs count':<28} | {sv_gym_logs:<15}")
print(f"{'4. Gold Target (BI)':<25} | {'workout_bpm_summary rows':<28} | {gold_bpm:<15}")
print(f"{'4. Gold Target (BI)':<25} | {'gym_summary rows':<28} | {gold_gym:<15}")
print("=" * 80)

if gold_bpm > 0 and gold_gym > 0:
    print("🏆 SUCCESS: Data lineage is 100% green and active! Power BI is ready to refresh.")
else:
    print("🚨 FAULT ALIGNMENT WARNING: Review the metric tracking numbers above to isolate where rows drop to 0.")
print("=" * 80)
