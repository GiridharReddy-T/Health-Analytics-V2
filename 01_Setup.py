# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
import sys
import os

# 1. Add the root of your repo to the Python path so we can import 'src'
# Safe, cross-compatible workspace lookup
try:
    # Captures the clean workspace directory path inside Databricks Repos
    repo_path = dbutils.notebook.getContext().notebookPath().get()
    # Step up out of the file name context to the workspace parent folder
    repo_path = os.path.dirname(repo_path)
except Exception:
    # Robust fallback for local terminals and pytest runner pipelines
    repo_path = os.getcwd()

# Dynamically normalize pathing references to cross OS environments
repo_path = os.path.abspath(repo_path)

if repo_path not in sys.path:
    sys.path.append(repo_path)
    print(f"Added to path: {repo_path}")

# 2. Import your custom classes
from src.config import Config
from src.setup import SetupHelper
from src.ingestion import BronzeIngestor
from src.transformation import SilverTransformer
from src.analytics import GoldAnalytics

# 3. Initialize Configuration (This loads your secrets and paths)
config = Config()

# 4. Initialize the Setup Helper
setup = SetupHelper(spark, config.env)

# 5. EXECUTION PIPELINE
print("--- Starting Lakehouse Setup ---")
setup.setup() # This creates your Bronze, Silver, Gold tables

print("--- Starting Data Ingestion (Bronze) ---")
ingestor = BronzeIngestor(spark, config.env)
ingestor.consume(once=True) # Runs the stream once for testing

print("--- Starting Transformation (Silver) ---")
transformer = SilverTransformer(spark, config)
# Here you would call your specific transformation methods

print("--- Starting Analytics (Gold) ---")
analytics = GoldAnalytics(spark, config)
# Here you would call your aggregation methods 

# 6. Final Validation
print("--- Validating Pipeline Integrity ---")
setup.validate()
print("Pipeline Execution Successful!")
