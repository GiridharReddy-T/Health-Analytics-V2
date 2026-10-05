import logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from src.config import Config

logger = logging.getLogger(__name__)

class BronzeIngestor:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        Conf = Config()
        self.landing_zone = Conf.base_dtr_data  # abfss://raw@datazone.dfs.core.windows.net
        self.checkpoint_base = Conf.base_dir_checkpount + "/checkpoint"
        self.catalog = env
        self.db_name = Conf.db_name
        
    # --- REGISTERED USERS (CSV batch) ---
    def consume_user_registration(self):
        """Batch-read registered_users CSV files from ADLS raw container → Bronze Delta table.
        NOTE: Auto Loader streaming (availableNow=True) hangs indefinitely on Serverless/Spark
        Connect. Use this reliable batch approach + a Databricks Job file-arrival trigger for
        event-driven execution when new files land in ADLS.
        """
        schema = "user_id string, device_id long, mac_address string, registration_timestamp string"
        self.spark.sql(f"USE CATALOG {self.catalog}")
        self.spark.sql(f"USE {self.db_name}")
        (self.spark.read
            .format("csv")
            .schema(schema)
            .option("header", "true")
            .option("pathGlobFilter", "*registered_users*.csv")
            .load(self.landing_zone)
            .withColumns({"load_time": F.current_timestamp(), "source_file": F.col("_metadata.file_path")})
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.catalog}.{self.db_name}.registered_users_bz"))
        logger.info("registered_users_bz Bronze table loaded from ADLS.")

    # --- GYM LOGINS (CSV batch) ---
    def consume_gym_logins(self):
        """Batch-read gym_logins CSV files from ADLS raw container → Bronze Delta table."""
        schema = "mac_address string, gym bigint, login string, logout string"
        self.spark.sql(f"USE CATALOG {self.catalog}")
        self.spark.sql(f"USE {self.db_name}")
        (self.spark.read
            .format("csv")
            .schema(schema)
            .option("header", "true")
            .option("pathGlobFilter", "*gym_logins*.csv")
            .load(self.landing_zone)
            .withColumns({"load_time": F.current_timestamp(), "source_file": F.col("_metadata.file_path")})
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.catalog}.{self.db_name}.gym_logins_bz"))
        logger.info("gym_logins_bz Bronze table loaded from ADLS.")

    # --- KAFKA MULTIPLEX (JSON batch) ---
    def consume_kafka_multiplex(self):
        """Batch-read Kafka JSON files from ADLS raw container → Bronze Delta table.
        Glob *_*.json matches user_info/bpm/workout; excludes date-lookup (no underscore).
        """
        schema = "key string, value string, topic string, partition long, offset long, timestamp long"
        self.spark.sql(f"USE CATALOG {self.catalog}")
        self.spark.sql(f"USE {self.db_name}")
        (self.spark.read
            .format("json")
            .schema(schema)
            .option("pathGlobFilter", "*_*.json")
            .load(self.landing_zone)
            .withColumns({
                "date": F.to_date(F.from_unixtime(F.col("timestamp"))),
                "week_part": F.concat(
                    F.year(F.from_unixtime(F.col("timestamp"))).cast("string"),
                    F.lit("-"),
                    F.weekofyear(F.from_unixtime(F.col("timestamp"))).cast("string")
                ),
                "load_time": F.current_timestamp(),
                "source_file": F.col("_metadata.file_path")
            })
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz"))
        logger.info("kafka_multiplex_bz Bronze table loaded from ADLS.")

    # --- MASTER ORCHESTRATOR ---
    def consume(self):
        """Run all three Bronze batch ingestions sequentially."""
        import time
        start = int(time.time())
        logger.info("Starting Bronze layer batch ingestion from ADLS raw container...")
        self.consume_user_registration()
        self.consume_gym_logins()
        self.consume_kafka_multiplex()
        logger.info(f"Bronze layer ingestion completed in {int(time.time()) - start} seconds")
        
# --- VALIDATION ---       
    def assert_count(self, table_name, expected_count, filter_expr="true"):
        logger.info(f"Validating record counts in {table_name}...")
        actual_count = self.spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
        logger.info(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success")        
        
    def validate(self, sets=1):
        import time
        start = int(time.time())
        logger.info(f"\nValidating bronz layer records...")
        self.assert_count("registered_users_bz", 5 if sets == 1 else 10)
        self.assert_count("gym_logins_bz", 8 if sets == 1 else 16)
        self.assert_count("kafka_multiplex_bz", 7 if sets == 1 else 13, "topic='user_info'")
        self.assert_count("kafka_multiplex_bz", 16 if sets == 1 else 32, "topic='workout'")
        self.assert_count("kafka_multiplex_bz", sets * 253801, "topic='bpm'")
        print(f"Bronze layer validation completed in {int(time.time()) - start} seconds")
     
    