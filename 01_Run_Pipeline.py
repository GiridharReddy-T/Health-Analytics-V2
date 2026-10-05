# =============================================================================
# 01_Bronze_Pipeline.py
# Responsibility : Ingest raw files from ADLS → Bronze Delta tables
# Trigger        : Databricks Job — file arrival on abfss://raw@datazone.dfs.core.windows.net/
# Downstream     : 02_Silver_Gold_Pipeline.py runs after this job succeeds
# =============================================================================
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

repo_path = "/Workspace/Users/chintuchinu1687@gmail.com"
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

from src.config import Config
from src.setup import SetupHelper
from src.ingestion import BronzeIngestor

config = Config()
setup  = SetupHelper(spark, config.env, catalog="dev_catalog")
target_db = f"{setup.catalog}.{setup.db_name}"

# ── Catalog + Volume bootstrap ────────────────────────────────────────────────
spark.sql("USE CATALOG dev_catalog")
spark.sql("CREATE DATABASE IF NOT EXISTS project_db")
spark.sql("USE project_db")
spark.sql("CREATE VOLUME IF NOT EXISTS dev_catalog.project_db.pipeline")

def get_row_count(tbl):
    try:
        return spark.table(f"{target_db}.{tbl}").count()
    except Exception:
        return 0

# ==============================================================================
# STEP 1/2 — LAKEHOUSE SCHEMA SETUP
# ==============================================================================
print("=" * 70)
print("📦 STEP 1/2: SCHEMA SETUP")
print("=" * 70)

if hasattr(setup, "config"):
    setup.config.db_name = "project_db"
setup.delta_base = None   # UC managed tables
setup.setup()
setup.validate()
print("✅ Schema setup verified.")

# ==============================================================================
# STEP 2/2 — BRONZE INGESTION: ADLS → DELTA
# ==============================================================================
print("\n" + "=" * 70)
print("📥 STEP 2/2: BRONZE INGESTION — ADLS → Delta tables")
print("=" * 70)

ingestor = BronzeIngestor(spark, "dev")
ingestor.catalog      = "dev_catalog"
ingestor.db_name      = "project_db"
ingestor.landing_zone = setup.landing_zone

print(" ⚙️  Ingesting from abfss://raw@datazone.dfs.core.windows.net/ ...")
try:
    ingestor.consume_user_registration()
    ingestor.consume_gym_logins()
    ingestor.consume_kafka_multiplex()
except Exception as e:
    # Raise so the Databricks Job marks this run FAILED — the Silver+Gold
    # task has 'depends_on: SUCCEEDED' so it will be skipped automatically.
    raise RuntimeError(f"🛑 Bronze ingestion failed — Silver+Gold will not run: {e}") from e

bz_users    = get_row_count("registered_users_bz")
bz_gym      = get_row_count("gym_logins_bz")
bz_kafka    = get_row_count("kafka_multiplex_bz")

print("\n" + "=" * 70)
print("📊 BRONZE AUDIT")
print("=" * 70)
print(f"  registered_users_bz : {bz_users:>8} rows")
print(f"  gym_logins_bz       : {bz_gym:>8} rows")
print(f"  kafka_multiplex_bz  : {bz_kafka:>8} rows")
print("=" * 70)
print("✅ Bronze pipeline complete. Silver+Gold job will now run.")
