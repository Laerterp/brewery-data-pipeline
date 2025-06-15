import os
import pandas as pd
from glob import glob

# Caminho para a pasta Silver (onde estão os Parquets)
silver_path = r"C:\Users\55349\brewery-data-pipeline\data\silver"

# Listar todos os arquivos .parquet (incluindo subpastas se particionado)
parquet_files = glob(os.path.join(silver_path, "**/*.parquet"), recursive=True)

print(f"Arquivos Parquet encontrados: {parquet_files}")

# Listar e ler arquivos
parquet_files = glob(os.path.join(silver_path, "**/*.parquet"), recursive=True)
combined_df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

# Salvar consolidado
combined_df.to_parquet(os.path.join(silver_path, "breweries_consolidated.parquet"))