import duckdb
import pandas as pd
import os  # Adicione esta linha para manipulação de diretórios

# Conectar ao DuckDB
con = duckdb.connect()

# Carregar os dados da Silver
con.execute("""
    CREATE TABLE silver_breweries AS
    SELECT * FROM 'C:/Users/55349/brewery-data-pipeline/data/silver/breweries_consolidated.parquet'
""")

# Converter para DataFrame
df = con.execute("SELECT * FROM silver_breweries").fetchdf()

# Definir caminho e criar diretório se não existir
excel_path = r"C:\Users\55349\brewery-data-pipeline\data\gold\cervejarias_consolidadas.xlsx"
os.makedirs(os.path.dirname(excel_path), exist_ok=True)  # Cria a pasta 'gold' se necessário

# Exportar para Excel
df.to_excel(excel_path, index=False, engine='openpyxl')

print(f"Tabela exportada com sucesso para: {excel_path}")

