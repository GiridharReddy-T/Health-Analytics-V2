import pytest
from src.ingestion import BronzeIngestor

def test_bronze_ingestor(spark):
    ingestor = BronzeIngestor(spark, "dev")
    # Add your test cases here
    assert True