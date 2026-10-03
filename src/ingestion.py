import logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from src.config import Config

logger = logging.getLogger(__name__)

class BronzeIngestor:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        Conf = Config()
        self.landing_zone = Conf.base_dtr_data + "/raw"
        self.checkpoint_base = Conf.base_dir_checkpount + "/checkpoint"
        self.catalog = env
        self.db_name = Conf.db_name
        
    def consume_user_registration(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = "user_id long, device_id long, mac_address string, registration_timestamp double"
        
        df_stream = (self.spark.readStream
                        .format("cloudFiles")
                        .schema(schema)
                        .option("maxFilesPerTrigger", 1)
                        .option("cloudFiles.format", "csv")
                        .option("header", "true")
                        .load(self.landing_zone + "/registered_users_bz")
                        .withColumn("load_time", F.current_timestamp()) 
                        .withColumn("source_file", F.col("_metadata.file_path")))
        
        writer = (df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/registered_users_bz") \
                                 .outputMode("append") \
                                 .queryName("registered_users_bz_ingestion_stream"))
        
        if once:
            return writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
        return writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
        
    # --- GYM LOGINS (CSV) ---         
    def consume_gym_logins(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = "mac_address string, gym bigint, login double, logout double"
        
        df_stream = (self.spark.readStream 
                        .format("cloudFiles") 
                        .schema(schema) 
                        .option("maxFilesPerTrigger", 1) 
                        .option("cloudFiles.format", "csv") 
                        .option("header", "true") 
                        .load(self.landing_zone + "/gym_logins_bz") 
                        .withColumn("load_time", F.current_timestamp())
                        .withColumn("source_file", F.col("_metadata.file_path"))
                    )
        
        # Use append mode because bronze layer is expected to insert only from source
        writer = (df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/gym_logins_bz") \
                                 .outputMode("append") \
                                 .queryName("gym_logins_bz_ingestion_stream"))
        
        if once:
            return writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        return writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        
# --- KAFKA MULTIPLEX (JSON) ---        
    def consume_kafka_multiplex(self, once=True, processing_time=" seconds"):
        schema = "key string, value string, topic string, partition bigint, offset bigint, timestamp bigint"
        df_date_lookup = spark.table(f"{self.catalog}.{self.db_name}.date_lookup").select("date", "week_part")
        
        df_stream = (self.spark.readStream
                        .format("cloudFiles")
                        .schema(schema)
                        .option("maxFilesPerTrigger", )
                        .option("cloudFiles.format", "json")
                        .load(self.landing_zone + "/kafka_multiplex_bz")                        
                        .withColumn("load_time", F.current_timestamp())       
                        .withColumn("source_file", F.col("_metadata.file_path"))
                        .join(F.broadcast(df_date_lookup), 
                              [F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date")], 
                              "left"))
        
        # Use append mode because bronze layer is expected to insert only from source
        writer = (df_stream.writeStream
                             .format("delta")
                             .option("checkpointLocation", self.checkpoint_base + "/kafka_multiplex_bz")
                             .outputMode("append")
                             .queryName("kafka_multiplex_bz_ingestion_stream"))
        if once:
            return writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
        return writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
        
# --- MASTER ORCHESTRATOR ---            
    def consume(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        logger.info(f"\nStarting bronze layer consumption ...")
        self.consume_user_registration(once, processing_time) 
        self.consume_gym_logins(once, processing_time) 
        self.consume_kafka_multiplex(once, processing_time)
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
        logger.info(f"Completed bronze layer consumtion {int(time.time()) - start} seconds")
        
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
     
    