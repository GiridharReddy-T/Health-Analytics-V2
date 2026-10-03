# Databricks notebook source
# Import your modules
import sys
sys.path.append('/path/to/src')
from src.config import Config
from src.setup import SetupHelper

# Initialize configuration and setup helper
config = Config()
setup = SetupHelper(spark, config.env)

# Run the setup
setup.setup()
setup.validate()