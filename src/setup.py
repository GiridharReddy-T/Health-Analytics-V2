import builtins
import logging
import time
from typing import Optional

from pyspark.sql import SparkSession

from src.config import Config

logger = logging.getLogger(__name__)


class SetupHelper:
    """Creates, validates and tears down the Bronze / Silver / Gold objects."""

    def __init__(
        self,
        spark: SparkSession,
        env: str,
        dbutils=None,
        catalog: Optional[str] = None,
    ):
        self.spark = spark
        self.env = env
        self.dbutils = dbutils if dbutils is not None else getattr(builtins, "dbutils", None)

        conf = Config()
        self.landing_zone = conf.base_dir_data       # abfss://raw@...
        self.delta_base = conf.delta_zone            # abfss://delta@...
        self.checkpoint_base = conf.checkpoint_path  # abfss://checkpoints@...

        # Unity Catalog three-level namespace: <catalog>.<schema>.<table>
        # "dev" -> "dev_catalog", which is the value that used to be hardcoded.
        self.catalog = catalog or f"{env}_catalog"
        self.db_name = conf.db_name
        self.db_prefix = f"{self.catalog}.{self.db_name}"
        self.initialized = False
        logger.info(f"Initialized SetupHelper using Unity Catalog route: {self.db_prefix}")

    # ------------------------------------------------------------------ helpers
    def _require_db(self):
        if not self.initialized:
            raise ReferenceError("Application database not defined. Run create_db() first.")

    def _location(self, sub_path: str) -> str:
        """LOCATION clause for an external table, or '' when no storage root is set (local tests)."""
        return f"LOCATION '{self.delta_base}/{sub_path}'" if self.delta_base else ""

    @staticmethod
    def _check(condition: bool, message: str):
        # Explicit raise instead of `assert`, which is stripped when Python runs with -O.
        if not condition:
            raise AssertionError(message)

    # ------------------------------------------------------------------ database
    def create_db(self):
        logger.info(f"Creating database schema layer: {self.db_prefix}...")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_prefix}")
        self.spark.sql(f"USE {self.db_prefix}")
        self.initialized = True
        logger.info("Done")

    # ------------------------------------------------------------------ BRONZE
    def create_registered_users_bz(self):
        self._require_db()
        logger.info("Creating registered_users_bz table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.registered_users_bz(
                user_id long,
                device_id long,
                mac_address string,
                registration_timestamp double,
                load_time timestamp,
                source_file string)
                USING delta {self._location('bronze/registered_users')}""")
        logger.info("Done")

    def create_gym_logins_bz(self):
        self._require_db()
        logger.info("Creating gym_logins_bz table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.gym_logins_bz(
                mac_address string,
                gym bigint,
                login double,
                logout double,
                load_time timestamp,
                source_file string)
                USING delta {self._location('bronze/gym_logins')}""")
        logger.info("Done")

    def create_kafka_multiplex_bz(self):
        self._require_db()
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
                {self._location('bronze/kafka_multiplex')}""")
        logger.info("Done")

    # ------------------------------------------------------------------ SILVER
    def create_users(self):
        self._require_db()
        logger.info("Creating users table...")
        # IF NOT EXISTS (was CREATE OR REPLACE): re-running setup() must not wipe existing data.
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.users(
                user_id bigint,
                device_id bigint,
                mac_address string,
                registration_timestamp timestamp)
                USING delta {self._location('silver/users')}""")
        logger.info("Done")

    def create_gym_logs(self):
        self._require_db()
        logger.info("Creating gym_logs table...")
        # IF NOT EXISTS (was CREATE OR REPLACE): re-running setup() must not wipe existing data.
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.gym_logs(
                mac_address string,
                gym bigint,
                login timestamp,
                logout timestamp)
                USING delta {self._location('silver/gym_logs')}""")
        logger.info("Done")

    def create_user_profile(self):
        self._require_db()
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
                USING delta {self._location('silver/user_profiles')}""")
        logger.info("Done")

    def create_heart_rate(self):
        self._require_db()
        logger.info("Creating heart_rate table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.heart_rate(
                device_id LONG,
                time TIMESTAMP,
                heartrate DOUBLE,
                valid BOOLEAN)
                USING delta {self._location('silver/heart_rate')}""")
        logger.info("Done")

    def create_user_bins(self):
        self._require_db()
        logger.info("Creating user_bins table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.user_bins(
                user_id BIGINT,
                age_group STRING,
                gender STRING,
                city STRING,
                state STRING)
                USING delta {self._location('silver/user_bins')}""")
        logger.info("Done")

    def create_workouts(self):
        self._require_db()
        logger.info("Creating workouts table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.workouts(
                user_id INT,
                workout_id INT,
                time TIMESTAMP,
                action STRING,
                session_id INT)
                USING delta {self._location('silver/workouts')}""")
        logger.info("Done")

    def create_completed_workouts(self):
        self._require_db()
        logger.info("Creating completed_workouts table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.completed_workouts(
                user_id INT,
                workout_id INT,
                session_id INT,
                start_time TIMESTAMP,
                end_time TIMESTAMP)
                USING delta {self._location('silver/completed_workouts')}""")
        logger.info("Done")

    def create_workout_bpm(self):
        self._require_db()
        logger.info("Creating workout_bpm table...")
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.db_prefix}.workout_bpm(
                user_id INT,
                workout_id INT,
                session_id INT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                time TIMESTAMP,
                heartrate DOUBLE)
                USING delta {self._location('silver/workout_bpm')}""")
        logger.info("Done")

    def create_date_lookup(self):
        self._require_db()
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
                USING delta {self._location('silver/date_lookup')}""")
        logger.info("Done")

    # ------------------------------------------------------------------ GOLD
    def create_workout_bpm_summary(self):
        self._require_db()
        logger.info("Creating workout_bpm_summary table...")
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
                USING delta {self._location('gold/workout_bpm_summary')}""")
        logger.info("Done")

    def create_gym_summary(self):
        self._require_db()
        logger.info("Creating gym_summary gold view...")
        self.spark.sql(f"""CREATE OR REPLACE VIEW {self.db_prefix}.gym_summary AS
                SELECT to_date(CAST(login AS TIMESTAMP)) AS date,
                       gym, l.mac_address, workout_id, session_id,
                       round((CAST(logout AS LONG) - CAST(login AS LONG)) / 60, 2) AS minutes_in_gym,
                       round((CAST(end_time AS LONG) - CAST(start_time AS LONG)) / 60, 2) AS minutes_exercising
                FROM {self.db_prefix}.gym_logs l
                JOIN (
                    SELECT mac_address, workout_id, session_id, start_time, end_time
                    FROM {self.db_prefix}.completed_workouts w
                    INNER JOIN {self.db_prefix}.users u ON w.user_id = u.user_id
                ) w
                ON l.mac_address = w.mac_address
                AND w.start_time BETWEEN l.login AND l.logout
                ORDER BY date, gym, l.mac_address, session_id""")
        logger.info("Done")

    # ------------------------------------------------------------------ GOVERNANCE
    def apply_column_masks(self):
        """Create a column-masking function and apply it to every mac_address column.

        Masking policy
        --------------
        - Members of the 'data_engineers' Databricks group, or account admins,
          see the original value (needed for debugging and device lookups).
        - All other users (analysts, BI tools, downstream consumers) see:
              AA:BB:CC:XX:XX:XX
          The OUI vendor prefix (first 3 octets) is preserved so device-type
          analytics still work; the device-specific bytes are hidden.

        Tables masked
        -------------
        - registered_users_bz  (Bronze)
        - gym_logins_bz        (Bronze)
        - users                (Silver)
        - gym_logs             (Silver)
        Note: gym_summary is a view — the mask is inherited automatically from
        the underlying gym_logs table.

        This method is idempotent — safe to call on every pipeline run.
        """
        self._require_db()
        func_fqn = f"{self.db_prefix}.mask_mac_address"

        logger.info(f"Creating PII masking function {func_fqn} ...")
        # Unity Catalog masking functions support is_member() and current_user().
        # is_account_admin() is NOT a valid built-in SQL function in UC.
        # To grant full access to additional groups, add further is_member() checks.
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {func_fqn}(mac STRING)
            RETURNS STRING
            RETURN CASE
                WHEN is_member('data_engineers') OR is_member('admins') THEN mac
                ELSE CONCAT(SUBSTR(mac, 1, 8), ':XX:XX:XX')
            END
        """)

        mac_tables = [
            "registered_users_bz",  # Bronze — raw device registrations
            "gym_logins_bz",         # Bronze — raw gym check-ins
            "users",                  # Silver — clean user profiles
            "gym_logs",               # Silver — login/logout sessions
        ]
        for tbl in mac_tables:
            logger.info(f"Applying mac_address mask to {self.db_prefix}.{tbl} ...")
            self.spark.sql(f"""
                ALTER TABLE {self.db_prefix}.{tbl}
                ALTER COLUMN mac_address
                SET MASK {func_fqn}
            """)

        logger.info("mac_address column masks applied to all PII tables.")

    # =====================================================================
    # LIFECYCLE METHODS (setup, validate, cleanup)
    # =====================================================================
    def setup(self):
        """Create the database and every Bronze -> Silver -> Gold object. Safe to re-run."""
        start = int(time.time())
        logger.info("Starting setup ...")
        self.create_db()

        # Bronze
        self.create_registered_users_bz()
        self.create_gym_logins_bz()
        self.create_kafka_multiplex_bz()

        # Silver
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()

        # Gold
        self.create_workout_bpm_summary()
        self.create_gym_summary()

        # Governance — PII column masks (idempotent: re-applying on each run is safe)
        self.apply_column_masks()
        logger.info(f"Setup completed in {int(time.time()) - start} seconds")

    def assert_table(self, table_name: str):
        """Raise AssertionError unless exactly one table/view with this name exists."""
        found = (
            self.spark.sql(f"SHOW TABLES IN {self.db_prefix}")
            .filter(f"isTemporary == false and tableName == '{table_name}'")
            .count()
        )
        self._check(found == 1, f"The table {table_name} is missing")
        logger.info(f"Found {table_name} table in {self.db_prefix}: Success")

    def validate(self):
        """Check that the database, all 14 tables/views, and the PII masking function exist."""
        start = int(time.time())
        logger.info("Starting setup validation ...")

        found = (
            self.spark.sql(f"SHOW DATABASES IN {self.catalog}")
            .filter(f"databaseName == '{self.db_name}'")
            .count()
        )
        self._check(found == 1, f"The database '{self.db_prefix}' is missing")
        logger.info(f"Found database {self.db_prefix}: Success")

        for table in (
            "registered_users_bz", "gym_logins_bz", "kafka_multiplex_bz",                 # Bronze
            "users", "gym_logs", "user_profile", "heart_rate", "workouts",                # Silver
            "completed_workouts", "workout_bpm", "user_bins", "date_lookup",
            "workout_bpm_summary", "gym_summary",                                          # Gold
        ):
            self.assert_table(table)

        # Confirm PII masking function is registered.
        # SHOW FUNCTIONS may return the short name ('mask_mac_address') or the fully
        # qualified name ('dev_catalog.project_db.mask_mac_address') depending on the
        # UC runtime; LIKE '%mask_mac_address' matches both safely.
        mask_count = (
            self.spark.sql(f"SHOW FUNCTIONS IN {self.db_prefix}")
            .filter("function LIKE '%mask_mac_address'")
            .count()
        )
        self._check(mask_count == 1,
                    f"PII masking function {self.db_prefix}.mask_mac_address is missing")
        logger.info(f"Found masking function {self.db_prefix}.mask_mac_address: Success")
        logger.info(f"Setup validation completed in {int(time.time()) - start} seconds")

    def cleanup(self):
        """DESTRUCTIVE: drop the database (CASCADE) and delete the landing-zone and checkpoint paths."""
        if self.env == "prod":
            raise RuntimeError("Refusing to run cleanup() against the prod environment.")

        try:
            exists = (
                self.spark.sql(f"SHOW DATABASES IN {self.catalog}")
                .filter(f"databaseName == '{self.db_name}'")
                .count()
                == 1
            )
            if exists:
                logger.info(f"Dropping the database {self.db_prefix}...")
                self.spark.sql(f"DROP DATABASE {self.db_prefix} CASCADE")
                logger.info("Database drop completed.")
        except Exception as db_err:
            logger.warning(f"Database teardown skipped or database not present: {db_err}")

        if self.dbutils is None:
            logger.info("Storage cleanup skipped: no dbutils context (expected in local unit tests).")
            return

        try:
            logger.info(f"Deleting raw landing zone path: {self.landing_zone}...")
            self.dbutils.fs.rm(self.landing_zone, True)  # True = recursive
            logger.info(f"Deleting streaming checkpoint path: {self.checkpoint_base}...")
            self.dbutils.fs.rm(self.checkpoint_base, True)
            logger.info("Environment teardown complete.")
        except Exception as fs_err:
            logger.error(f"Storage path cleanup failed: {fs_err}")