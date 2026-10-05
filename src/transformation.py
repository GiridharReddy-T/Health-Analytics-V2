from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

# FIX: Corrected from logging.get_logger to logging.getLogger
logger = logging.getLogger(__name__)

class SilverTransformer:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.catalog = "dev_catalog"
        self.db_name = "project_db"
        self.db_prefix = f"{self.catalog}.{self.db_name}"

    def clean_registered_users(self, df):
        logger.info("Transforming registered users...")
        return df.withColumn("user_id", F.col("user_id").cast("long")) \
                 .withColumn("device_id", F.col("device_id").cast("long")) \
                 .withColumn("mac_address", F.lower(F.col("mac_address"))) \
                 .withColumn("registration_timestamp",
                         F.from_unixtime(F.col("registration_timestamp").cast("long")).cast("timestamp")) \
                 .withColumn("processed_at", F.current_timestamp()) \
                 .dropDuplicates(["user_id"])

    def clean_heart_rate(self, df):
        logger.info("Transforming heart rate data...")
        return df.filter(F.col("heartrate") > 0) \
                 .withColumn("valid", F.when((F.col("heartrate") < 40) | (F.col("heartrate") > 220), False).otherwise(True)) \
                 .withColumn("time", F.col("time").cast("timestamp"))

    def aggregate_gym_sessions(self, df):
        logger.info("Aggregating gym sessions...")
        return df.withColumn("login_ts", F.from_unixtime(F.col("login").cast("long")).cast("timestamp")) \
                 .withColumn("logout_ts", F.from_unixtime(F.col("logout").cast("long")).cast("timestamp")) \
                 .withColumn("duration_minutes", (F.col("logout_ts").cast("long") - F.col("login_ts").cast("long")) / 60) \
                 .drop("login", "logout")

    def process_kafka_multiplex(self, df):
        logger.info("Processing Kafka multiplexed stream...")
        return df.withColumn("data", F.from_json(F.col("value"), "topic STRING, value STRING, timestamp BIGINT")) \
                 .select(".*", "data.*") \
                 .withColumn("event_time", (F.col("timestamp") / 1000).cast("timestamp")) \
                 .filter(F.col("topic").isNotNull())

    def apply_user_bins(self, df):
        logger.info("Applying age binning and calculating generation brackets...")
        return df.withColumn("age", F.floor(F.datediff(F.current_date(), F.col("dob")) / 365.25)) \
                 .withColumn("age_group", 
                     F.when(F.col("age") < 18, "under 18")
                      .when(F.col("age") < 35, "18-34")
                      .when(F.col("age") < 50, "35-49")
                      .when(F.col("age") < 65, "50-64")
                      .otherwise("65+"))

    def merge_workouts(self, df):
        logger.info("Merging workout sessions...")
        window = Window.partitionBy("user_id", "workout_id").orderBy(F.col("time").desc())
        return df.withColumn("rank", F.rank().over(window)) \
                 .filter("rank == 1") \
                 .drop("rank")

    def upsert_users(self, df_micro_batch, batch_id):
        # FIX: Force catalog routing context right inside the micro-batch thread
        self.spark.sql(f"USE CATALOG {self.catalog}")
        self.spark.sql(f"USE {self.db_name}")
        
        df_micro_batch.createOrReplaceTempView("users_delta")
        
        # Pass a safe, single-part table reference here
        self.spark.sql("""
            MERGE INTO users a
            USING users_delta b
            ON a.user_id = b.user_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"Batch {batch_id}: MERGE into users complete")

    def run_users_cdc(self, once=True, processing_time="10 seconds"):
        # Force catalog routing before reading streaming targets
        self.spark.sql(f"USE CATALOG {self.catalog}")
        self.spark.sql(f"USE {self.db_name}")

        # FIX: Restructured the layout to separate casting conversions from direct schema selection parameters
        df_stream = (self.spark.readStream 
            .table("registered_users_bz") 
            .option("startingVersion", "0") 
            .option("ignoreDeletes", True)
            .select("user_id", "device_id", "mac_address", "registration_timestamp")
            .withColumn("registration_timestamp", F.col("registration_timestamp").cast("timestamp"))
        )

        writer = (df_stream.writeStream
                  .foreachBatch(self.upsert_users)
                  .outputMode("update")
                  .option("checkpointLocation", f"{self.config.checkpoint_path}/users")
                  .queryName("users_cdc_stream"))

        if once:
            return writer.trigger(availableNow=True).start()
        return writer.trigger(processingTime=processing_time).start()

    def run_kafka_silver_transforms(self):
        import time
        start = int(time.time())
        logger.info("Running Kafka silver transforms...")

        # gym_logins_bz -> gym_logs
        df_gym_bz = self.spark.table(f"{self.db_prefix}.gym_logins_bz")
        (self.aggregate_gym_sessions(df_gym_bz)
            .withColumnRenamed("login_ts", "login")
            .withColumnRenamed("logout_ts", "logout")
            .select("mac_address", "gym", "login", "logout", "duration_minutes")
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.db_prefix}.gym_logs"))
        logger.info("gym_logs silver table populated.")

        logger.info(f"Kafka silver transforms completed in {int(time.time()) - start} seconds")

    # ── BPM data → workout_bpm (requires users + completed_workouts) ─────────────
    def build_workout_bpm(self):
        """Parse BPM readings, resolve device_id → user_id via users table,
        then match each reading to a workout session via time-range join on
        completed_workouts. Must be called AFTER build_completed_workouts().

        BPM value schema: {device_id: BIGINT, time: BIGINT (unix seconds), heartrate: DOUBLE}
        """
        logger.info("Building workout_bpm (device_id → user_id, time-range join with completed_workouts)...")
        bpm_schema = "device_id BIGINT, time BIGINT, heartrate DOUBLE"
        df_kafka = self.spark.table(f"{self.db_prefix}.kafka_multiplex_bz")
        df_users = self.spark.table(f"{self.db_prefix}.users")
        df_cw = self.spark.table(f"{self.db_prefix}.completed_workouts")

        # 1. Parse raw BPM readings (device_id, unix-second timestamp, heartrate)
        df_bpm = (df_kafka
            .filter(F.col("topic") == "bpm")
            .withColumn("b", F.from_json(F.col("value"), bpm_schema))
            .select(
                F.col("b.device_id").alias("device_id"),
                F.from_unixtime(F.col("b.time")).cast("timestamp").alias("time"),
                F.col("b.heartrate").alias("heartrate")
            )
            .filter(F.col("heartrate") > 0)          # quality gate: drop zero/null BPM
        )

        # 2. device_id → user_id via the users Silver table
        df_bpm_user = df_bpm.join(
            df_users.select(F.col("user_id").alias("user_id"),
                            F.col("device_id").alias("u_device_id")),
            df_bpm.device_id == F.col("u_device_id"),
            "inner"
        ).drop("u_device_id")

        # 3. Time-range join: match BPM reading to the workout session active at that moment
        df_cw_a = df_cw.select(
            F.col("user_id").alias("cw_user_id"),
            "workout_id", "session_id", "start_time", "end_time"
        )
        (df_bpm_user
            .join(df_cw_a,
                  (F.col("user_id") == F.col("cw_user_id")) &
                  (F.col("time") >= F.col("start_time")) &
                  (F.col("time") <= F.col("end_time")),
                  "inner")
            .select("user_id", "workout_id", "session_id",
                    "start_time", "end_time", "time", "heartrate")
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.db_prefix}.workout_bpm"))
        logger.info("workout_bpm silver table populated.")

    # ── user_info topic → user_profile ──────────────────────────────────────
    def transform_user_profile(self):
        """Parse latest user_info record per user → user_profile silver table."""
        import time as _time
        logger.info("Transforming user_profile from kafka_multiplex_bz (user_info topic)...")
        user_schema = (
            "user_id INT, update_type STRING, timestamp DOUBLE, dob STRING, "
            "sex STRING, gender STRING, first_name STRING, last_name STRING, "
            "address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>"
        )
        df_kafka = self.spark.table(f"{self.db_prefix}.kafka_multiplex_bz")
        win = Window.partitionBy("user_id").orderBy(F.col("ts").desc())
        (df_kafka
            .filter(F.col("topic") == "user_info")
            .withColumn("d", F.from_json(F.col("value"), user_schema))
            .select(
                F.col("d.user_id").alias("user_id"),
                F.col("d.dob").alias("dob"),
                F.col("d.sex").alias("sex"),
                F.col("d.gender").alias("gender"),
                F.col("d.first_name").alias("first_name"),
                F.col("d.last_name").alias("last_name"),
                F.col("d.address.street_address").alias("street_address"),
                F.col("d.address.city").alias("city"),
                F.col("d.address.state").alias("state"),
                F.col("d.address.zip").alias("zip"),
                F.col("d.timestamp").cast("timestamp").alias("updated"),
                F.col("d.timestamp").alias("ts"),
            )
            .withColumn("_rn", F.row_number().over(win))
            .filter("_rn == 1")
            .drop("_rn", "ts")
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.db_prefix}.user_profile"))
        logger.info("user_profile silver table populated.")

    # ── user_profile → user_bins (age group + demographics) ─────────────────
    def transform_user_bins(self):
        """Age-bin user_profile → user_bins silver table."""
        logger.info("Building user_bins from user_profile...")
        df = self.spark.table(f"{self.db_prefix}.user_profile")
        (df
            .withColumn("dob_parsed", F.to_date(F.col("dob"), "MM/dd/yyyy"))
            .withColumn("age", F.floor(F.datediff(F.current_date(), F.col("dob_parsed")) / 365.25))
            .withColumn("age_group",
                F.when(F.col("age") < 18, "under 18")
                 .when(F.col("age") < 35, "18-34")
                 .when(F.col("age") < 50, "35-49")
                 .when(F.col("age") < 65, "50-64")
                 .otherwise("65+"))
            .select("user_id", "age_group", "gender", "city", "state")
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.db_prefix}.user_bins"))
        logger.info("user_bins silver table populated.")

    # ── workout topic → workouts (raw events) ────────────────────────────────
    def transform_workouts(self):
        """Parse workout start/stop events from kafka_multiplex_bz → workouts table."""
        logger.info("Transforming workout events from kafka_multiplex_bz...")
        workout_schema = "user_id INT, workout_id INT, timestamp DOUBLE, action STRING, session_id INT"
        df_kafka = self.spark.table(f"{self.db_prefix}.kafka_multiplex_bz")
        (df_kafka
            .filter(F.col("topic") == "workout")
            .withColumn("d", F.from_json(F.col("value"), workout_schema))
            .select(
                F.col("d.user_id").alias("user_id"),
                F.col("d.workout_id").alias("workout_id"),
                F.col("d.timestamp").cast("timestamp").alias("time"),
                F.col("d.action").alias("action"),
                F.col("d.session_id").alias("session_id"),
            )
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.db_prefix}.workouts"))
        logger.info("workouts silver table populated.")

    # ── workouts (start+stop pairs) → completed_workouts ────────────────────
    def build_completed_workouts(self):
        """Pair workout start/stop events → completed_workouts silver table."""
        logger.info("Building completed_workouts from workouts table...")
        df = self.spark.table(f"{self.db_prefix}.workouts")
        starts = df.filter(F.col("action") == "start").select(
            "user_id", "workout_id", "session_id", F.col("time").alias("start_time"))
        stops = df.filter(F.col("action") == "stop").select(
            "user_id", "workout_id", "session_id", F.col("time").alias("end_time"))
        (starts.join(stops, ["user_id", "workout_id", "session_id"], "inner")
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.db_prefix}.completed_workouts"))
        logger.info("completed_workouts silver table populated.")

    # ── date dimension (raw container) → date_lookup ────────────────────────
    def load_date_lookup(self):
        """Read date dimension JSON from the raw container → date_lookup silver table.

        All numeric fields arrive as strings in the JSON; they are cast to INT here.
        A glob pattern (*date-lookup*.json) picks up any batch-numbered variant of
        the file (e.g. 6-date-lookup.json, 7-date-lookup.json, ...).
        """
        logger.info("Loading date_lookup from raw container...")
        source_path = f"{self.config.base_dir_data}/*date-lookup*.json"
        (self.spark.read
            .format("json")
            .load(source_path)
            .select(
                F.col("date").cast("date").alias("date"),
                F.col("week").cast("int").alias("week"),
                F.col("year").cast("int").alias("year"),
                F.col("month").cast("int").alias("month"),
                F.col("dayofweek").cast("int").alias("dayofweek"),
                F.col("dayofmonth").cast("int").alias("dayofmonth"),
                F.col("dayofyear").cast("int").alias("dayofyear"),
                F.col("week_part"),
            )
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.db_prefix}.date_lookup"))
        logger.info("date_lookup silver table populated.")
