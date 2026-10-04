import os
import logging
from typing import Optional
from pyspark.sql import SparkSession

from src.config import Config

logger = logging.getLogger(__name__)

class SetupHelper:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        Conf = Config()
        
        # Cloud paths bound to your storage account container layers
        self.landing_zone = Conf.base_dir_data        # abfss://raw@datazone...
        self.delta_base = Conf.delta_zone             # abfss://delta@datazone...
        self.checkpoint_base = Conf.checkpoint_path   # abfss://checkpoints@datazone...
        
        # FIXED: Enforce your verified 'dev_catalog' three-level namespace scheme
        self.catalog = "dev_catalog"
        self.db_name = Conf.db_name
        self.db_prefix = f"{self.catalog}.{self.db_name}"
        self.initialized = False
        logger.info(f"Initialized SetupHelper using Unity Catalog route: {self.db_prefix}")

            

    def create_db(self):
        # # # self.spark.catalog.clearCache()
        logger.info(f"Creating database schema layer: {self.db_prefix}...")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_prefix}")
        self.spark.sql(f"USE {self.db_prefix}")
        self.initialized = True
        logger.info("Done")

    # --- BRONZE LAYER (EXTERNAL DELTA DATA BLOCKS) ---
    def create_registered_users_bz(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating registered_users_bz table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.registered_users_bz(
                user_id long, 
                device_id long, 
                mac_address string,
                registration_timestamp double, 
                load_time timestamp, 
                source_file string)
                USING delta LOCATION '{self.delta_base}/bronze/registered_users'""")
        logger.info("Done")

    def create_gym_logins_bz(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating gym_logins_bz table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.gym_logins_bz(
                mac_address string, 
                gym bigint, 
                login double, 
                logout double,
                load_time timestamp, 
                source_file string)
                USING delta LOCATION '{self.delta_base}/bronze/gym_logins'""")
        logger.info("Done")

    def create_kafka_multiplex_bz(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating kafka_multiplex_bz table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.kafka_multiplex_bz(
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
                USING delta PARTITIONED BY (topic, week_part) 
                LOCATION '{self.delta_base}/bronze/kafka_multiplex'""")
        logger.info("Done")

    # --- SILVER LAYER ---
    def create_users(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating users table...")
        self.spark.sql(f"""CREATE OR REPLACE TABLE {self.db_prefix}.users(
                user_id bigint, 
                device_id bigint, 
                mac_address string, 
                registration_timestamp timestamp)
                USING delta LOCATION '{self.delta_base}/silver/users'""")
        logger.info("Done")

    def create_gym_logs(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating gym_logs table...")
        self.spark.sql(f"""CREATE OR REPLACE TABLE {self.db_prefix}.gym_logs(
                mac_address string, 
                gym bigint, 
                login timestamp, 
                logout timestamp)
                USING delta LOCATION '{self.delta_base}/silver/gym_logs'""")
        logger.info("Done")

    def create_user_profile(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating user_profile table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.user_profile(
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
                updated TIMESTAMP)
                USING delta LOCATION '{self.delta_base}/silver/user_profiles'""")
        logger.info("Done")

    def create_heart_rate(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating heart_rate table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.heart_rate(
                device_id LONG,
                time TIMESTAMP,
                heartrate DOUBLE,
                valid BOOLEAN)
                USING delta LOCATION '{self.delta_base}/silver/heart_rate'""")  
        logger.info("Done")

    def create_user_bins(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating user_bins table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.user_bins(
                user_id BIGINT, 
                age STRING, 
                gender STRING, 
                city STRING, 
                state STRING)
                USING delta LOCATION '{self.delta_base}/silver/user_bins'""")
        logger.info("Done")

    def create_workouts(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating workouts table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.workouts(
                user_id INT, 
                workout_id INT, 
                time TIMESTAMP, 
                action STRING, 
                session_id INT)
                USING delta LOCATION '{self.delta_base}/silver/workouts'""")
        logger.info("Done")

    def create_completed_workouts(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating completed_workouts table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.completed_workouts(
                user_id INT, 
                workout_id INT, 
                session_id INT, 
                start_time TIMESTAMP, 
                end_time TIMESTAMP)
                USING delta LOCATION '{self.delta_base}/silver/completed_workouts'""")
        logger.info("Done")

    def create_workout_bpm(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating workout_bpm table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.workout_bpm(
                user_id INT, 
                workout_id INT, 
                session_id INT, 
                start_time TIMESTAMP,
                end_time TIMESTAMP, 
                time TIMESTAMP, 
                heartrate DOUBLE)
                USING delta LOCATION '{self.delta_base}/silver/workout_bpm'""")
        logger.info("Done")

    def create_date_lookup(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating date_lookup table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.date_lookup(
                date date, 
                week int, 
                year int, 
                month int, 
                dayofweek int,
                dayofmonth int, 
                dayofyear int, 
                week_part string)
                USING delta LOCATION '{self.delta_base}/silver/date_lookup'""")
        logger.info("Done")

    # --- GOLD LAYER TABLES AND VIEWS ---
    def create_workout_bpm_summary(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined.")
        logger.info("Creating workout_bpm_summary table...")
        # FIXED: Restored self.db_prefix string variable mapping continuity
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.workout_bpm_summary(
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
                num_recordings BIGINT)               
                USING delta LOCATION '{self.delta_base}/gold/workout_bpm_summary'""")
        logger.info("Done")

    def create_gym_summary(self):
        if self.initialized:
            print("Creating gym_summary gold view...", end='')
            # Unified under self.db_prefix and added dynamic catalog qualifiers to all inner tables
        self.spark.sql(f"""CREATE OR REPLACE VIEW {self.db_prefix}.gym_summary AS
                SELECT to_date(login::timestamp) date,       -- Extract date from login timestamp
                gym, l.mac_address, workout_id, session_id, 
                -- Calculate total minutes in the gym (logout - login in minutes)
                round((logout::long - login::long)/60,2) minutes_in_gym,
                -- Calculate total minutes exercising (end_time - start_time in minutes)
                round((end_time::long - start_time::long)/60,2) minutes_exercising
                FROM {self.db_prefix}.gym_logs l 
                JOIN (
                -- Subquery: Join completed_workouts with users using explicit schema qualifiers
                SELECT mac_address, workout_id, session_id, start_time, end_time
                FROM {self.db_prefix}.completed_workouts w 
                INNER JOIN {self.db_prefix}.users u ON w.user_id = u.user_id) w
                ON l.mac_address = w.mac_address 
                -- Match workouts that started during a gym session
                AND w.start_time BETWEEN l.login AND l.logout
                order by date, gym, l.mac_address, session_id""")
        logger.info("Done")      
        

    # =====================================================================
    # LIFECYCLE METHODS (setup, validate, cleanup)
    # =====================================================================
    
    # Runs the complete setup: creates database and all tables in order
    # Bronze tables first, then silver, then gold
    # Prints total elapsed time upon completion
    def setup(self):
        import time
        start = int(time.time())  # Record start time for duration tracking
        print(f"\nStarting setup ...")
        self.create_db()                    # Step 1: Create the database
        
        # Bronze layer tables
        self.create_registered_users_bz()   # Step 2: Raw user registrations
        self.create_gym_logins_bz()         # Step 3: Raw gym login/logout
        self.create_kafka_multiplex_bz()    # Step 4: Raw Kafka messages
        
        # Silver layer tables
        self.create_users()                 # Step 5: Cleansed users
        self.create_gym_logs()              # Step 6: Cleansed gym logs
        self.create_user_profile()          # Step 7: User demographics
        self.create_heart_rate()            # Step 8: Validated heart rate
        self.create_workouts()              # Step 9: Workout events
        self.create_completed_workouts()    # Step 10: Paired workout sessions
        self.create_workout_bpm()           # Step 11: Heart rate during workouts
        self.create_user_bins()             # Step 12: User demographic bins
        self.create_date_lookup()           # Step 13: Date dimension table
        
        # Gold layer tables/views
        self.create_workout_bpm_summary()   # Step 14: BPM summary aggregations
        self.create_gym_summary()           # Step 15: Gym usage summary view
        logger.info(f"Setup completed in {int(time.time()) - start} seconds")
    
    # Helper method to assert a single table exists in the database
    # Queries SHOW TABLES and filters for the expected table name
    # Raises AssertionError if the table is missing
    def assert_table(self, table_name):
        assert self.spark.sql(f"SHOW TABLES IN {self.db_prefix}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        logger.info(f"Found {table_name} table in {self.db_prefix}: Success")
    
    # Validates the entire setup by checking:
    #   1. The database exists in the catalog
    #   2. All 14 tables/views exist in the database
    # Prints success/failure for each check and total elapsed time
    def validate(self):
        import time
        start = int(time.time())
        logger.info(f"\nStarting setup validation ...")
        
        # First, verify the database itself exists inside the specific catalog target
        assert self.spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.db_prefix}' is missing"
        logger.info(f"Found database {self.db_prefix}: Success")
        
        # Then verify each table/view exists cleanly
        self.assert_table("registered_users_bz")    # Bronze: user registrations
        self.assert_table("gym_logins_bz")           # Bronze: gym logins
        self.assert_table("kafka_multiplex_bz")      # Bronze: Kafka messages
        self.assert_table("users")                   # Silver: cleansed users
        self.assert_table("gym_logs")                # Silver: cleansed gym logs
        self.assert_table("user_profile")            # Silver: user demographics
        self.assert_table("heart_rate")              # Silver: heart rate readings
        self.assert_table("workouts")                # Silver: workout events
        self.assert_table("completed_workouts")      # Silver: completed sessions
        self.assert_table("workout_bpm")             # Silver: workout heart rate
        self.assert_table("user_bins")               # Silver: user bins
        self.assert_table("date_lookup")             # Silver: date dimension
        self.assert_table("workout_bpm_summary")     # Gold: BPM summary
        self.assert_table("gym_summary")             # Gold: gym usage view
        logger.info(f"Setup validation completed in {int(time.time()) - start} seconds")
    
    # Cleans up all resources created by setup:
    #   1. Drops the entire database with CASCADE (removes all tables/views)
    #   2. Deletes the raw data landing zone directory
    #   3. Deletes the streaming checkpoint directory
    # Safe to call even if some resources don't exist
    def cleanup(self): 
        # Only drop database if it exists (avoids errors on re-runs)
        try:
            if self.spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
                logger.info(f"Dropping the database {self.db_prefix}...", end='')
                self.spark.sql(f"DROP DATABASE {self.db_prefix} CASCADE")  # CASCADE drops all objects inside
                logger.info("Done")
        except Exception as db_err:
            logger.warning(f"Database teardown skipped or database not present: {db_err}")

        # Inject safe global dbutils context binding to prevent terminal unit test failures
        import builtins
        dbutils_ctx = getattr(builtins, "dbutils", None)
        if dbutils_ctx is not None:
            try:
                # Remove the raw data landing zone (recursive delete)
                logger.info(f"Deleting raw landing zone storage path: {self.landing_zone}...", end='')
                dbutils_ctx.fs.rm(self.landing_zone, True)  # True = recursive delete
                logger.info("Done")
                
                # Remove streaming checkpoint files (recursive delete)
                logger.info(f"Deleting streaming checkpoint storage path: {self.checkpoint_base}...", end='')
                dbutils_ctx.fs.rm(self.checkpoint_base, True)  # True = recursive delete
                logger.info("Done")
                logger.info("Workspace environment teardown lifecycle complete.")
            except Exception as fs_err:
                logger.error(f"Cloud container path storage cleanup skipped: {fs_err}")
