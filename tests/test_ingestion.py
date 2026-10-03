import pytest
from pyspark.sql import SparkSession
from src.ingestion import BronzeIngestor

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[]").getOrCreate()

def test_bronze_ingestor(spark):
    ingestor = BronzeIngestor(spark, "dev")
    # Add your test cases here
    assert True