import pytest
from src.config import Config


def test_config():
    config = Config()
    assert config.env == "dev"
    assert config.base_dir_data == "/mnt/data_zone"
    assert config.base_dir_checkpoint == "/mnt/checkpoint"
    assert config.db_name == "project_db"
    assert config.max_files_per_trigger == 1000


def test_config_env_override(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    config = Config()
    assert config.env == "prod"