# Databricks notebook source
import sys
import os

# 1. Add the root of your repo to the Python path so we can import 'src'
# This allows Databricks to find your modular .py files
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

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