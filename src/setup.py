# Databricks notebook source
import logging
from typing import Optional
from pyspark.sql import SparkSession

from src.config import Config

logger = logging.getLogger(__name__)


# COMMAND ----------

class SetupHelper:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        Conf = Config()
        self.landing_zone = Conf.base_dir_data + "/raw"
        self.checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = Conf.db_name
        self.initialized = False

    def create_db(self):
        self.spark.catalog.clearCache()
        logger.info(f"Creating the database {self.catalog}.{self.db_name}...")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
        self.spark.sql(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True
        logger.info("Done")

    # --- BRONZE LAYER ---
    def create_registered_users_bz(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating registered_users_bz table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.registered_users_bz(
                user_id long, 
                device_id long, 
                mac_address string,
                registration_timestamp double, 
                load_time timestamp, 
                source_file string)""")
        logger.info("Done")

    def create_gym_logins_bz(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating gym_logins_bz table...")
        self.spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logins_bz(
                mac_address string, 
                gym bigint, 
                login double, 
                logout double,
                load_time timestamp, 
                source_file string)""")
        logger.info("Done")

    def create_kafka_multiplex_bz(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating kafka_multiplex_bz table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_multiplex_bz(
                key string, 
                value string, 
                topic string, 
                partition bigint, 
                offset bigint,
                timestamp bigint, 
                date date, 
                week_part string, 
                load_time timestamp, 
                source_file string)
                PARTITIONED BY (topic, week_part)""")
        logger.info("Done")

    # --- SILVER LAYER ---
    def create_users(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating users table...")
        self.spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                user_id bigint, 
                device_id bigint, 
                mac_address string, 
                registration_timestamp timestamp)""")
        logger.info("Done")

    def create_gym_logs(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating gym_logs table...")
        self.spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                mac_address string, 
                gym bigint, 
                login timestamp, 
                logout timestamp)""")
        logger.info("Done")

    def create_user_profile(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating user_profile table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                user_id bigint, 
                dob DATE, 
                sex STRING, 
                gender STRING,
                first_name STRING, 
                last_name STRING, 
                street_address STRING,
                city STRING, 
                state STRING, 
                zip INT, 
                updated TIMESTAMP)""")
        logger.info("Done")

    def create_heart_rate(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating heart_rate table...")
        self.spark.sql("""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                device_id LONG,
                time TIMESTAMP,
                heartrate DOUBLE,
                valid BOOLEAN)""")  
        logger.info("Done")

    def create_user_bins(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating user_bins table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                user_id BIGINT, 
                age STRING, 
                gender STRING, 
                city STRING, 
                state STRING)""")
        logger.info("Done")

    def create_workouts(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating workouts table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
                user_id INT, 
                workout_id INT, 
                time TIMESTAMP, 
                action STRING, 
                session_id INT)""")
        logger.info("Done")

    def create_completed_workouts(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating completed_workouts table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
                user_id INT, 
                workout_id INT, 
                session_id INT, 
                start_time TIMESTAMP, 
                end_time TIMESTAMP)""")
        logger.info("Done")

    def create_workout_bpm(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating workout_bpm table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
                user_id INT, 
                workout_id INT, 
                session_id INT, 
                start_time TIMESTAMP,
                end_time TIMESTAMP, 
                time TIMESTAMP, 
                heartrate DOUBLE)""")
        logger.info("Done")

    def create_date_lookup(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating date_lookup table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.date_lookup(
                date date, 
                week int, 
                year int, 
                month int, 
                dayofweek int,
                dayofmonth int, 
                dayofyear int, 
                week_part string)""")
        logger.info("Done")

    # =====================================================================
    # GOLD LAYER TABLES AND VIEWS (business-level aggregations)
    # These are consumption-ready tables/views for analytics and dashboards
    # =====================================================================
    
    # Creates the workout_bpm_summary table (Gold layer)
    # Aggregated BPM statistics per workout session, enriched with user demographics
    # Used for health analytics dashboards and reports

    def create_workout_bpm_summary(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating workout_bpm_summary table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
                workout_id INT, 
                session_id INT, 
                user_id BIGINT, 
                age STRING, 
                gender STRING,
                city STRING, 
                state STRING, 
                min_bpm DOUBLE, 
                avg_bpm DOUBLE,
                max_bpm DOUBLE, 
                num_recordings BIGINT)""")
        logger.info("Done")

    def create_gym_summary(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating gym_summary view...")
        self.spark.sql(f"""CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
                SELECT to_date(login::timestamp) date, gym, l.mac_address, workout_id, session_id,
                round((logout::long - login::long)/60,2) minutes_in_gym,
                round((end_time::long - start_time::long)/60,2) minutes_exercising
                FROM gym_logs l
                JOIN (SELECT mac_address, workout_id, session_id, start_time, end_time
                      FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                ON l.mac_address = w.mac_address
                AND w.start_time BETWEEN l.login AND l.logout
                ORDER BY date, gym, l.mac_address, session_id""")
        logger.info("Done")

    # =====================================================================
    # LIFECYCLE METHODS (setup, validate, cleanup)
    # =====================================================================
    
    # Runs the complete setup: creates database and all tables in order
    # Bronze tables first, then silver, then gold
    # Prints total elapsed time upon completion
    
    def setup(self):
        import time
        start = int(time.time())
        logger.info("Starting setup ...")
        self.create_db()
        self.create_registered_users_bz()
        self.create_gym_logins_bz()
        self.create_kafka_multiplex_bz()
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()
        self.create_workout_bpm_summary()
        self.create_gym_summary()
        logger.info(f"Setup completed in {int(time.time()) - start} seconds")

    def assert_table(self, table_name: str):
        count = self.spark.sql(
            f"SHOW TABLES IN {self.catalog}.{self.db_name}"
        ).filter(f"isTemporary == false and tableName == '{table_name}'").count()
        assert count == 1, f"The table {table_name} is missing"
        logger.info(f"Found {table_name} table: Success")

    def validate(self):
        import time
        start = int(time.time())
        logger.info("Starting setup validation ...")
        assert self.spark.sql(
            f"SHOW DATABASES IN {self.catalog}"
        ).filter(f"databaseName == '{self.db_name}'").count() == 1, f"DB missing"
        logger.info(f"Found database: Success")
        for t in ["registered_users_bz", "gym_logins_bz", "kafka_multiplex_bz",
                  "users", "gym_logs", "user_profile", "heart_rate", "workouts",
                  "completed_workouts", "workout_bpm", "user_bins", "date_lookup",
                  "workout_bpm_summary", "gym_summary"]:
            self.assert_table(t)
        logger.info(f"Setup validation completed in {int(time.time()) - start} seconds")

    def cleanup(self):
        if self.spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(
            f"databaseName == '{self.db_name}'"
        ).count() == 1:
            logger.info(f"Dropping database {self.catalog}.{self.db_name}...")
            self.spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            logger.info("Done")
        # dbutils is Databricks-only; safe to keep but guard if running locally
        try:
            import dbutils
            logger.info(f"Deleting {self.landing_zone}...")
            dbutils.fs.rm(self.landing_zone, True)
            logger.info("Deleting checkpoints...")
            dbutils.fs.rm(self.checkpoint_base, True)
        except Exception:
            logger.warning("dbutils not available; skipping filesystem cleanup.")
