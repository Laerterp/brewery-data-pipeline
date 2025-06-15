# src/bronze_to_silver.py
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class SilverTransformer:
    def __init__(self, silver_dir: str):
        self.silver_dir = silver_dir
        Path(self.silver_dir).mkdir(parents=True, exist_ok=True)
    
    def process_file(self, bronze_file_path: str) -> bool:
        try:
            logger.info(f"Processing {bronze_file_path}")
            df = pd.read_json(bronze_file_path)
            
            # Simples transformação (adicione suas regras)
            df["process_date"] = pd.to_datetime("today")
            
            # Salva em Parquet
            output_path = f"{self.silver_dir}/breweries.parquet"
            df.to_parquet(output_path)
            
            return True
        except Exception as e:
            logger.error(f"Failed to process {bronze_file_path}: {e}")
            return False