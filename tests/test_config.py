import pytest
from src.config import Config


def test_config():
    config = Config()
    assert config.env == "dev"
    
    # Updated to verify cloud-native abfss containers on the datazone storage account
    assert config.storage_account == "datazone"
    assert config.base_dir_data == "abfss://raw@datazone.dfs.core.windows.net"
    assert config.delta_zone == "abfss://delta@datazone.dfs.core.windows.net"
    assert config.base_dir_checkpoint == "abfss://checkpoints@datazone.dfs.core.windows.net"
    
    # Core system variables verification
    assert config.db_name == "project_db"
    assert config.max_files_per_trigger == 1000


def test_config_env_override(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    config = Config()
    assert config.env == "prod"
