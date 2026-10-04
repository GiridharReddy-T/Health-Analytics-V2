from pyspark.sql import functions as F
from src.utils import get_logger

logger = get_logger(__name__)

class GoldAnalytics:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def workout_bpm_summary(self):
        logger.info("Creating workout BPM summary...")
        self.spark.sql(f"""
            CREATE OR REPLACE TABLE {self.config.db_name}.workout_bpm_summary AS
            SELECT
                w.user_id,
                w.workout_id,
                w.session_id,
                u.age_group,
                u.gender,
                u.city,
                u.state,
                MIN(b.heartrate) AS min_bpm,
                AVG(b.heartrate) AS avg_bpm,
                MAX(b.heartrate) AS max_bpm,
                COUNT(b.heartrate) AS num_recordings
            FROM {self.config.db_name}.workout_bpm b
            JOIN {self.config.db_name}.completed_workouts w ON b.user_id = w.user_id AND b.workout_id = w.workout_id AND b.session_id = w.session_id
            JOIN {self.config.db_name}.user_bins u ON b.user_id = u.user_id
            GROUP BY w.user_id, w.workout_id, w.session_id, u.age_group, u.gender, u.city, u.state
        """)
        logger.info("Workout BPM summary created")

    def gym_summary(self):
        logger.info("Creating gym summary...")
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self.config.db_name}.gym_summary AS
            SELECT
                to_date(l.login::timestamp) AS date,
                l.gym,
                l.mac_address,
                w.workout_id,
                w.session_id,
                -- FIX: Convert second differences to minutes by dividing by 60
                ROUND((l.logout::long - l.login::long) / 60) AS minutes_in_gym,
                ROUND((w.end_time::long - w.start_time::long) / 60) AS minutes_exercising
            FROM {self.config.db_name}.gym_logs l
            JOIN (
                SELECT
                    u.user_id,
                    u.mac_address,
                    w.workout_id,
                    w.session_id,
                    w.start_time,
                    w.end_time
                FROM {self.config.db_name}.completed_workouts w
                INNER JOIN {self.config.db_name}.users u ON w.user_id = u.user_id
            ) w ON l.mac_address = w.mac_address
            AND w.start_time BETWEEN l.login AND l.logout
            ORDER BY date, gym, l.mac_address, session_id
        """)
        logger.info("Gym summary created")
