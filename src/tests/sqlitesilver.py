# silver_to_sqlite.py
import sqlite3
import pandas as pd
from glob import glob
from pathlib import Path

# Configurações
SILVER_PATH = r"C:\Users\55349\brewery-data-pipeline\data\silver"
DB_PATH = r"C:\Users\55349\brewery-data-pipeline\data\breweries.db"  # Arquivo SQLite de saída

def main():
    # 1. Encontrar todos os arquivos Parquet na Silver
    parquet_files = glob(f"{SILVER_PATH}/**/*.parquet", recursive=True)
    print(f" Encontrados {len(parquet_files)} arquivos Parquet")
    
    # 2. Criar conexão com o SQLite
    conn = sqlite3.connect(DB_PATH)
    
    # 3. Processar cada arquivo e criar tabelas no SQLite
    for file_path in parquet_files:
        # Extrair nome da tabela do caminho do arquivo
        table_name = Path(file_path).stem.lower()
        
        # Ler arquivo Parquet
        df = pd.read_parquet(file_path)
        
        # Salvar no SQLite (substitui se já existir)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f" Tabela '{table_name}' criada com {len(df)} registros")
    
    # 4. Fechar conexão e finalizar
    conn.close()
    print(f"\n Banco de dados criado em: {DB_PATH}")
    print("Você agora pode abrir este arquivo no SQLite para fazer suas consultas e criar a camada Gold.")

if __name__ == "__main__":
    main()
    