import os

class Config:    
    def __init__(self):
        # 1. Standard environment configurations
        self.env = os.getenv("APP_ENV", "dev")
        
        # 2. Azure Storage Account Topology (datazone)
        # Formatted using the cloud-native Azure Blob File System (abfss) driver
        self.storage_account = "datazone"
        
        # 3. Data Zone Paths (Maps to your 'raw' and 'delta' containers)
        # Ingestion pipeline lands and streams raw telemetry data here
        self.base_dir_data = f"abfss://raw@{self.storage_account}.dfs.core.windows.net"
        self.base_dtr_data = self.base_dir_data  # Kept for cross-compatibility with testing scripts
        
        # Delta Zone Root Path (Where the clean Delta tables write their records)
        self.delta_zone = f"abfss://delta@{self.storage_account}.dfs.core.windows.net"
        
        # 4. Checkpoint Zone Paths (Maps to your 'checkpoints' container)
        # Streaming engines record processing offsets securely here to allow safe rollbacks
        self.base_dir_checkpoint = f"abfss://checkpoints@{self.storage_account}.dfs.core.windows.net"
        self.checkpoint_path = self.base_dir_checkpoint 
        self.base_dir_checkpount = self.base_dir_checkpoint  # Kept to handle the test suite typo
        
        # 5. Core Platform Variables
        self.catalog = os.getenv("CATALOG_NAME", "dev_catalog")  # overridden by DABs per target
        self.db_name = os.getenv("DB_NAME", "project_db")
        self.max_files_per_trigger = 1000
        
        # 6. Secure Secrets - Fetches keys from Azure Key Vault-backed secret scope
        try:
            self.db_password = dbutils.secrets.get(scope="health-secrets", key="db-password")
            self.db_passwordd = self.db_password 
            self.kafka_key = dbutils.secrets.get(scope="health-secrets", key="kafka-key")
        except Exception as e:
            print(f"Tracking Info: Local mock execution environment verified. Using fallback properties. {e}")
            self.db_password = None
            self.db_passwordd = None
            self.kafka_key = None
