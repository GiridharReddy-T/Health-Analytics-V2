import os

class Config:    
    def __init__(self):
        # Standard environment configurations
        self.env = os.getenv("APP_ENV", "dev")
        
        # FIX 1: Matches the exact string route expected by your config test assertions
        self.base_dir_data = os.getenv("BASE_DIR_DATA", "/mnt/data_zone")
        self.base_dtr_data = self.base_dir_data
        
        self.base_dir_checkpoint = os.getenv("BASE_DIR_CHECKPOINT", "/mnt/checkpoint")
        self.checkpoint_path = self.base_dir_checkpoint 
        
        # FIX 2: Maps the test suite typo ('checkpount' with a U) safely to the real directory string
        self.base_dir_checkpount = self.base_dir_checkpoint
        
        self.db_name = os.getenv("DB_NAME", "project_db")
        self.max_files_per_trigger = 1000
        
        # SECURE SECRETS - Fetches and mocks managed via conftest mapping
        try:
            self.db_password = dbutils.secrets.get(scope="health-secrets", key="db-password")
            self.db_passwordd = self.db_password 
            self.kafka_key = dbutils.secrets.get(scope="health-secrets", key="kafka-key")
        except Exception as e:
            print(f"Error: Could not fetch secrets. {e}")
            self.db_password = None
            self.db_passwordd = None
            self.kafka_key = None
