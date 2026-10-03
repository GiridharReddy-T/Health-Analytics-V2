import logging
import sys
from datetime import datetime

def get_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def format_timestamp(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def validate_schema(df, expected_columns: list):
    actual_columns = df.columns
    for col in expected_columns:
        if col not in actual_columns:
            raise ValueError(f"Missing column: {col}")
    return True