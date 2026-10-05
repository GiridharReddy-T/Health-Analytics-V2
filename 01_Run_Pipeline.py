# =============================================================================
# 01_Bronze_Pipeline.py
# Responsibility : Ingest raw files from ADLS → Bronze Delta tables
# Trigger        : Databricks Job — file arrival on abfss://raw@datazone.dfs.core.windows.net/
# Downstream     : 02_Silver_Gold_Pipeline.py runs after this job succeeds
# =============================================================================
import sys
import os
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── DABs variable injection ─────────────────────────────────────────────────────
# DABs passes --catalog and --env via spark_python_task.parameters.
# parse_known_args() safely ignores any extra Spark submit args.
parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="dev_catalog", help="UC catalog (dev/test/prod)")
parser.add_argument("--env",     default="dev",         help="Environment name")
args, _ = parser.parse_known_args()

# Inject into env vars BEFORE Config() reads them
os.environ["CATALOG_NAME"] = args.catalog
os.environ["APP_ENV"]      = args.env

repo_path = "/Workspace/Users/chintuchinu1687@gmail.com"
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

from src.config import Config
from src.setup import SetupHelper
from src.ingestion import BronzeIngestor

config    = Config()   # now reads CATALOG_NAME + APP_ENV from env
catalog   = config.catalog
setup     = SetupHelper(spark, config.env, catalog=catalog)
target_db = f"{catalog}.{config.db_name}"

# ── Catalog + Volume bootstrap ────────────────────────────────────────────────
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.db_name}")
spark.sql(f"USE {config.db_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{config.db_name}.pipeline")

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
    setup.config.db_name = config.db_name
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

ingestor = BronzeIngestor(spark, config.env)
ingestor.catalog      = catalog
ingestor.db_name      = config.db_name
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
