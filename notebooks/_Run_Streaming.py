# Databricks notebook source
import sys
sys.path.append('/path/to/src')
from src.ingestion import BronzeIngestor
from src.transformation import SilverTransformer
from src.analytics import GoldAnalytics

# Initialize ingestor, transformer, and analytics
ingestor = BronzeIngestor(spark, config.env)
transformer = SilverTransformer(spark, config)
analytics = GoldAnalytics(spark, config)

# Run the ingestion
ingestor.consume()

# Run the transformation
transformer.run_users_cdc()

# Run the analytics
analytics.workout_bpm_summary()
analytics.gym_summary()