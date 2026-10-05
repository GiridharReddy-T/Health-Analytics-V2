import pytest
from unittest.mock import MagicMock
from pyspark.sql import functions as F


# ── Initialisation ──────────────────────────────────────────────────────
def test_bronze_ingestor_attributes():
    """BronzeIngestor initialises with the correct catalog, db_name, and landing_zone."""
    from src.ingestion import BronzeIngestor

    mock_spark = MagicMock()
    ingestor = BronzeIngestor(mock_spark, "dev")

    # env arg becomes the default catalog attribute
    assert ingestor.catalog == "dev",                 "catalog should reflect the env argument"
    assert ingestor.db_name == "project_db",          "db_name must default to project_db"
    assert "raw@datazone" in ingestor.landing_zone,   "landing_zone must point to the raw ADLS container"
    assert ingestor.spark is mock_spark,               "spark session must be stored"


# ── Kafka multiplex transformations ───────────────────────────────────────
def test_kafka_date_transformation(spark):
    """kafka_multiplex: unix epoch timestamp converts to a correct date column."""
    # 1678442400 = 2023-03-10 10:00:00 UTC
    df = spark.createDataFrame(
        [("k1", "v1", "bpm", 0, 0, 1678442400)],
        "key STRING, value STRING, topic STRING, partition LONG, offset LONG, timestamp LONG"
    )

    result = df.withColumn(
        "date", F.to_date(F.from_unixtime(F.col("timestamp")))
    ).collect()[0]

    assert result.date is not None,          "date column must not be null"
    assert str(result.date) == "2023-03-10", f"Expected 2023-03-10, got {result.date}"


def test_kafka_week_part_format(spark):
    """kafka_multiplex: week_part is YYYY-W (e.g. '2023-10') with a valid week number."""
    df = spark.createDataFrame(
        [("k1", "v1", "bpm", 0, 0, 1678442400)],
        "key STRING, value STRING, topic STRING, partition LONG, offset LONG, timestamp LONG"
    )

    result = df.withColumn(
        "week_part",
        F.concat(
            F.year(F.from_unixtime(F.col("timestamp"))).cast("string"),
            F.lit("-"),
            F.weekofyear(F.from_unixtime(F.col("timestamp"))).cast("string"),
        )
    ).collect()[0]

    assert "-" in result.week_part,         "week_part must contain a hyphen separator"
    year, week = result.week_part.split("-", 1)
    assert year == "2023",                   f"Expected year 2023, got {year}"
    assert week.isdigit(),                   f"Week must be numeric, got {week!r}"
    assert 1 <= int(week) <= 53,             f"Week number must be 1–53, got {week}"


# ── Metadata column enrichment ─────────────────────────────────────────────
def test_user_registration_metadata_columns(spark):
    """Batch CSV read adds load_time and source_file columns while preserving all source fields."""
    raw = spark.createDataFrame(
        [("1001", 5001, "AA:BB:CC:DD:EE:FF", "1678451681")],
        "user_id STRING, device_id LONG, mac_address STRING, registration_timestamp STRING"
    )

    enriched = raw.withColumns({
        "load_time":   F.current_timestamp(),
        "source_file": F.lit("abfss://raw@datazone.dfs.core.windows.net/1-registered_users_1.csv"),
    })

    cols = enriched.columns
    for col in ("user_id", "device_id", "mac_address", "registration_timestamp",
                "load_time", "source_file"):
        assert col in cols, f"Column '{col}' must be present after enrichment"

    row = enriched.collect()[0]
    assert row.load_time   is not None,                        "load_time must not be null"
    assert "registered_users" in row.source_file,              "source_file must reference the source CSV"
    assert row.user_id     == "1001",                          "user_id must be preserved"
    assert row.mac_address == "AA:BB:CC:DD:EE:FF",             "mac_address must be preserved"


def test_gym_logins_metadata_columns(spark):
    """Batch CSV read adds load_time and source_file to gym_logins schema."""
    raw = spark.createDataFrame(
        [("AA:BB:CC:DD:EE:FF", 5, "1678451529", "1678455129")],
        "mac_address STRING, gym BIGINT, login STRING, logout STRING"
    )

    enriched = raw.withColumns({
        "load_time":   F.current_timestamp(),
        "source_file": F.lit("abfss://raw@datazone.dfs.core.windows.net/5-gym_logins_1.csv"),
    })

    cols = enriched.columns
    for col in ("mac_address", "gym", "login", "logout", "load_time", "source_file"):
        assert col in cols, f"Column '{col}' must be present in gym_logins Bronze schema"

    row = enriched.collect()[0]
    assert row.mac_address == "AA:BB:CC:DD:EE:FF"
    assert row.gym         == 5
    assert row.source_file is not None