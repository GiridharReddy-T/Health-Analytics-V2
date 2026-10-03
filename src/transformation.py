from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils import get_logger

logger = get_logger(__name__)

class SilverTransformer:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def clean_registered_users(self, df):
        logger.info("Transforming registered users...")
        return df.withColumn("user_id", F.col("user_id").cast("long")) \
                 .withColumn("device_id", F.col("device_id").cast("long")) \
                 .withColumn("mac_address", F.lower(F.col("mac_address"))) \
                 .withColumn("registration_timestamp", F.col("registration_timestamp").cast("timestamp")) \
                 .withColumn("processed_at", F.current_timestamp()) \
                 .dropDuplicates(["user_id"])

    def clean_heart_rate(self, df):
        logger.info("Transforming heart rate data...")
        return df.filter(F.col("heartrate") > 0) \
                 .withColumn("valid", F.when((F.col("heartrate") < 40) | (F.col("heartrate") > 220), False).otherwise(True)) \
                 .withColumn("time", F.col("time").cast("timestamp"))

    def aggregate_gym_sessions(self, df):
        logger.info("Aggregating gym sessions...")
        return df.withColumn("login_ts", F.col("login").cast("timestamp")) \
                 .withColumn("logout_ts", F.col("logout").cast("timestamp")) \
                 .withColumn("duration_minutes", (F.col("logout_ts").cast("long") - F.col("login_ts").cast("long")) / 60) \
                 .drop("login", "logout")

    def process_kafka_multiplex(self, df):
        logger.info("Processing Kafka multiplexed stream...")
        return df.withColumn("data", F.from_json(F.col("value"), "topic STRING, value STRING, timestamp BIGINT")) \
                 .select(".*", "data.*") \
                 .withColumn("event_time", (F.col("timestamp") / 1000).cast("timestamp")) \
                 .filter(F.col("topic").isNotNull())

    def apply_user_bins(self, df):
        logger.info("Applying age binning...")
        return df.withColumn("age", F.floor(F.datediff(F.current_date(), F.col("dob")) / 365.25)) \
                 .withColumn("age_group", F.when(F.col("age") < 18, "under 18")
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
        df_micro_batch.createOrReplaceTempView("users_delta")
        self.spark.sql(f"""
            MERGE INTO {self.config.db_name}.users a
            USING users_delta b
            ON a.user_id = b.user_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"Batch {batch_id}: MERGE into users complete")

    def run_users_cdc(self, once=True, processing_time="10 seconds"):
        df_stream = self.spark.readStream \
            .table(f"{self.config.db_name}.registered_users_bz") \
            .option("startingVersion", "0") \
            .option("ignoreDeletes", True) \
            .select("user_id", "device_id", "mac_address", F.col("registration_timestamp").cast("timestamp"))

        writer = (df_stream.writeStream
                  .foreachBatch(self.upsert_users)
                  .outputMode("update")
                  .option("checkpointLocation", f"{self.config.checkpoint_path}/users")
                  .queryName("users_cdc_stream"))

        if once:
            return writer.trigger(availableNow=True).start()
        return writer.trigger(processingTime=processing_time).start()
