import pytest
from pyspark.sql import SparkSession
from src.analytics import GoldAnalytics
from src.config import Config

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[]").getOrCreate()

def test_gold_analytics(spark):
    analytics = GoldAnalytics(spark, Config())
    # Add your test cases here
    assert True