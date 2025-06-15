# silver_to_sqlite_specific.py
import sqlite3
import pandas as pd
from pathlib import Path

# Configurações
SPECIFIC_FILES = [
    r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_metadata\breweries_by_state_20250614_013844.parquet",
    r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_metadata\breweries_by_type_20250614_013844.parquet"
]
DB_PATH = r"C:\Users\55349\brewery-data-pipeline\data\breweries.db"  # Arquivo SQLite existente

def main():
    # 1. Verificar se os arquivos existem
    for file_path in SPECIFIC_FILES:
        if not Path(file_path).exists():
            print(f"❌ Arquivo não encontrado: {file_path}")
            return
    
    # 2. Conectar ao banco de dados SQLite existente
    conn = sqlite3.connect(DB_PATH)
    
    # 3. Processar cada arquivo específico
    for file_path in SPECIFIC_FILES:
        # Extrair nome da tabela do caminho do arquivo
        table_name = Path(file_path).stem.lower()
        
        # Ler arquivo Parquet
        try:
            df = pd.read_parquet(file_path)
            
            # Salvar no SQLite (substitui se já existir)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"✅ Tabela '{table_name}' criada/atualizada com {len(df)} registros")
        except Exception as e:
            print(f"❌ Erro ao processar {file_path}: {str(e)}")
    
    # 4. Fechar conexão e finalizar
    conn.close()
    print(f"\n🎉 Arquivos específicos adicionados ao banco de dados: {DB_PATH}")

if __name__ == "__main__":
    main()