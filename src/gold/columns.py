import duckdb

# Conectar ao DuckDB (cria um banco em memória)
con = duckdb.connect()

# 1. LER OS DADOS DA SILVER (arquivo consolidado) - usando raw string
con.execute(r"""
    CREATE TABLE silver_breweries AS
    SELECT * FROM 'C:\Users\55349\brewery-data-pipeline\data\silver\breweries_consolidated.parquet';
""")

# Verificar colunas disponíveis
print("Colunas disponíveis na silver_breweries:")
print(con.execute("DESCRIBE silver_breweries").fetchdf())