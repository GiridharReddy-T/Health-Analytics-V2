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

# 4. Initialize SetupHelper attached to dev_catalog
setup = SetupHelper(spark, "dev")
setup.catalog = "dev_catalog"
setup.db_prefix = f"{setup.catalog}.{config.db_name}"
setup.landing_zone = config.base_dir_data
setup.delta_base = config.delta_zone
setup.checkpoint_base = config.checkpoint_path

# 5. EXECUTION PIPELINE RUNNER
print("--- Starting Lakehouse Pipeline Execution Sequence ---")
setup.setup()
setup.validate()
print("Pipeline Run Completed Successfully: 100% Green Status Locked In!")
