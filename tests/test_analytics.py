import pytest
from datetime import datetime
from unittest.mock import MagicMock
from pyspark.sql import functions as F


# ── Initialisation ──────────────────────────────────────────────────────
def test_gold_analytics_init():
    """GoldAnalytics stores the provided spark session and config."""
    from src.analytics import GoldAnalytics

    mock_spark  = MagicMock()
    mock_config = MagicMock()
    mock_config.db_name = "project_db"

    analytics = GoldAnalytics(mock_spark, mock_config)

    assert analytics.spark  is mock_spark
    assert analytics.config is mock_config


# ── workout_bpm_summary logic ───────────────────────────────────────────
def test_workout_bpm_aggregation_logic(spark):
    """workout_bpm_summary: min/avg/max/count BPM are correct per (user, workout, session)."""
    bpm_data = [
        (1, 101, 1, 120.0),
        (1, 101, 1, 145.0),
        (1, 101, 1, 155.0),   # user 1 — 3 readings
        (2, 202, 2, 100.0),
        (2, 202, 2, 110.0),   # user 2 — 2 readings
    ]
    df = spark.createDataFrame(
        bpm_data, "user_id INT, workout_id INT, session_id INT, heartrate DOUBLE"
    )

    rows = (
        df
        .groupBy("user_id", "workout_id", "session_id")
        .agg(
            F.min("heartrate").alias("min_bpm"),
            F.avg("heartrate").alias("avg_bpm"),
            F.max("heartrate").alias("max_bpm"),
            F.count("heartrate").alias("num_recordings"),
        )
        .orderBy("user_id")
        .collect()
    )

    assert len(rows) == 2, "Expected one summary row per unique workout session"

    u1 = rows[0]
    assert u1.user_id        == 1
    assert u1.min_bpm        == 120.0
    assert u1.max_bpm        == 155.0
    assert abs(u1.avg_bpm - (120.0 + 145.0 + 155.0) / 3) < 0.01
    assert u1.num_recordings == 3

    u2 = rows[1]
    assert u2.user_id        == 2
    assert u2.min_bpm        == 100.0
    assert u2.max_bpm        == 110.0
    assert u2.num_recordings == 2


def test_workout_bpm_session_isolation(spark):
    """workout_bpm_summary: different (user, session) pairs are never merged together."""
    # Two users share workout_id=101 but have different session_ids — must stay separate.
    bpm_data = [
        (1, 101, 1, 130.0),
        (1, 101, 1, 150.0),
        (2, 101, 2, 160.0),
    ]
    df = spark.createDataFrame(
        bpm_data, "user_id INT, workout_id INT, session_id INT, heartrate DOUBLE"
    )

    rows = (
        df
        .groupBy("user_id", "workout_id", "session_id")
        .agg(F.count("heartrate").alias("num_recordings"))
        .orderBy("user_id")
        .collect()
    )

    assert len(rows) == 2,                        "Each (user, session) must produce a separate row"
    assert rows[0].user_id == 1 and rows[0].num_recordings == 2
    assert rows[1].user_id == 2 and rows[1].num_recordings == 1


# ── gym_summary logic ────────────────────────────────────────────────
def test_gym_summary_workout_in_window(spark):
    """gym_summary: workout whose start_time falls WITHIN login/logout is included."""
    gym = spark.createDataFrame(
        [("AA:BB:CC:11:22:33", 1, datetime(2023, 3, 10, 9, 0, 0), datetime(2023, 3, 10, 11, 0, 0))],
        "mac_address STRING, gym INT, login TIMESTAMP, logout TIMESTAMP"
    )
    workouts = spark.createDataFrame(
        [
            ("AA:BB:CC:11:22:33", 101, 1, datetime(2023, 3, 10, 10, 0, 0), datetime(2023, 3, 10, 10, 30, 0)),  # IN window
            ("AA:BB:CC:11:22:33", 202, 2, datetime(2023, 3, 10, 12, 0, 0), datetime(2023, 3, 10, 12, 30, 0)),  # OUTSIDE
        ],
        "mac_address STRING, workout_id INT, session_id INT, start_time TIMESTAMP, end_time TIMESTAMP"
    )

    # gym_summary JOIN condition: workout.start_time BETWEEN login AND logout
    result = (
        gym.alias("l")
        .join(
            workouts.alias("w"),
            (F.col("l.mac_address") == F.col("w.mac_address")) &
            F.col("w.start_time").between(F.col("l.login"), F.col("l.logout")),
            "inner"
        )
        .select("w.workout_id", "w.session_id")
        .collect()
    )

    assert len(result) == 1,          f"Only 1 workout should be in the window, got {len(result)}"
    assert result[0].workout_id == 101, "The in-window workout (101) must be returned"


def test_gym_summary_workout_outside_window_excluded(spark):
    """gym_summary: workout whose start_time is OUTSIDE login/logout is excluded."""
    gym = spark.createDataFrame(
        [("AA:BB:CC:11:22:33", 1, datetime(2023, 3, 10, 9, 0, 0), datetime(2023, 3, 10, 10, 0, 0))],
        "mac_address STRING, gym INT, login TIMESTAMP, logout TIMESTAMP"
    )
    workouts = spark.createDataFrame(
        [("AA:BB:CC:11:22:33", 202, 2, datetime(2023, 3, 10, 11, 0, 0), datetime(2023, 3, 10, 11, 30, 0))],
        "mac_address STRING, workout_id INT, session_id INT, start_time TIMESTAMP, end_time TIMESTAMP"
    )

    result = (
        gym.alias("l")
        .join(
            workouts.alias("w"),
            (F.col("l.mac_address") == F.col("w.mac_address")) &
            F.col("w.start_time").between(F.col("l.login"), F.col("l.logout")),
            "inner"
        )
        .collect()
    )

    assert len(result) == 0, "No workout should match — start_time is after logout"


def test_gym_summary_minutes_calculation(spark):
    """gym_summary: minutes_in_gym and minutes_exercising are calculated correctly.

    Scenario: user checked in 09:00, checked out 10:30 (90 min in gym).
    Workout ran 09:30 – 10:00 (30 min exercising).
    """
    data = spark.createDataFrame(
        [("AA:BB:CC:11:22:33", 1,
          datetime(2023, 3, 10, 9, 0, 0), datetime(2023, 3, 10, 10, 30, 0),
          datetime(2023, 3, 10, 9, 30, 0), datetime(2023, 3, 10, 10, 0, 0))],
        "mac_address STRING, gym INT, "
        "login TIMESTAMP, logout TIMESTAMP, "
        "start_time TIMESTAMP, end_time TIMESTAMP"
    )

    row = data.withColumns({
        "minutes_in_gym": F.round(
            (F.col("logout").cast("long") - F.col("login").cast("long")) / 60
        ),
        "minutes_exercising": F.round(
            (F.col("end_time").cast("long") - F.col("start_time").cast("long")) / 60
        ),
    }).collect()[0]

    assert row.minutes_in_gym     == 90.0, f"Expected 90 min in gym, got {row.minutes_in_gym}"
    assert row.minutes_exercising == 30.0, f"Expected 30 min exercising, got {row.minutes_exercising}"