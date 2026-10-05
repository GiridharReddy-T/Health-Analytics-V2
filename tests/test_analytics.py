import pytest
from src.analytics import GoldAnalytics
from src.config import Config

def test_gold_analytics(spark):
    analytics = GoldAnalytics(spark, Config())
    # Add your test cases here
    assert True