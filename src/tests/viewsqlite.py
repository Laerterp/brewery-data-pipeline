import duckdb
from pathlib import Path
import os
import sqlite3

# 1. Configurar caminhos de forma robusta
base_path = Path(r"C:\Users\55349\brewery-data-pipeline")
gold_path = base_path / "data" / "gold"

# Criar diretório se não existir (com verificação)
gold_path.mkdir(parents=True, exist_ok=True)

# Conectar ao DuckDB (cria um banco em memória)
con = duckdb.connect()

# 1. LER OS DADOS DA SILVER (arquivo consolidado) - usando raw string
con.execute(r"""
    CREATE TABLE silver_breweries AS
    SELECT * FROM 'C:\Users\55349\brewery-data-pipeline\data\silver\breweries_consolidated.parquet';
""")

# Dimensão Localização
con.execute("""
    CREATE TABLE dim_localizacao AS
    SELECT 
        row_number() OVER () AS localizacao_id,
        city AS cidade,
        estado_provincia AS estado,
        country AS pais,
        codigo_postal AS cep
    FROM silver_breweries
    GROUP BY city, estado_provincia, country, codigo_postal;
""")

# Dimensão Tipo de Cervejaria (CORRIGIDO - usando 'site' em vez de 'website_url')
con.execute("""
    CREATE TABLE dim_tipo_cervejaria AS
    SELECT 
        row_number() OVER () AS tipo_id,
        tipos_cervejaria AS tipo,
        site
    FROM silver_breweries
    WHERE tipos_cervejaria IS NOT NULL
    GROUP BY tipos_cervejaria, site;
""")

# 3. CRIAR A TABELA FATO (GOLD)
con.execute("""
    CREATE TABLE fact_cervejarias AS
    SELECT
        b.id,
        b.name AS nome,
        t.tipo_id,
        l.localizacao_id,
        b.longitude,
        b.latitude,
        b.phone AS telefone
    FROM silver_breweries b
    LEFT JOIN dim_localizacao l 
        ON b.city = l.cidade 
        AND b.estado_provincia = l.estado
    LEFT JOIN dim_tipo_cervejaria t 
        ON b.tipos_cervejaria = t.tipo;
""")

# 3. Exportar tabelas para Parquet (como você já tinha)
tables = ["dim_localizacao", "dim_tipo_cervejaria", "fact_cervejarias"]

for table in tables:
    output_file = gold_path / f"{table}.parquet"
    con.execute(f"""
        COPY {table} TO '{output_file.as_posix()}' (FORMAT PARQUET);
    """)
    print(f"Tabela {table} exportada para Parquet: {output_file}")

# 4. Criar arquivo SQLite para visualização
sqlite_path = gold_path / "brewery_dimensional_model.db"

# Conectar ao SQLite e criar as tabelas
with sqlite3.connect(sqlite_path) as sqlite_conn:
    # Criar cursor
    cursor = sqlite_conn.cursor()
    
    # Para cada tabela no DuckDB, exportar para SQLite
    for table in tables:
        # Obter os dados da tabela do DuckDB
        data = con.execute(f"SELECT * FROM {table}").fetchall()
        
        # Obter a estrutura da tabela (colunas)
        cols = con.execute(f"DESCRIBE {table}").fetchall()
        col_defs = [f"{col[0]} {col[1]}" for col in cols]
        
        # Criar a tabela no SQLite
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute(f"CREATE TABLE {table} ({', '.join(col_defs)})")
        
        # Inserir os dados
        col_names = [col[0] for col in cols]
        placeholders = ", ".join(["?" for _ in col_names])
        insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"
        
        cursor.executemany(insert_sql, data)
        print(f"Tabela {table} criada no SQLite com {len(data)} registros")
    
    # Adicionar também a tabela silver para referência
    silver_data = con.execute("SELECT * FROM silver_breweries").fetchall()
    silver_cols = con.execute("DESCRIBE silver_breweries").fetchall()
    silver_col_defs = [f"{col[0]} {col[1]}" for col in silver_cols]
    
    cursor.execute("DROP TABLE IF EXISTS silver_breweries")
    cursor.execute(f"CREATE TABLE silver_breweries ({', '.join(silver_col_defs)})")
    
    silver_placeholders = ", ".join(["?" for _ in silver_cols])
    cursor.executemany(f"INSERT INTO silver_breweries VALUES ({silver_placeholders})", silver_data)
    print(f"Tabela silver_breweries criada no SQLite com {len(silver_data)} registros")

print(f"\nBanco de dados SQLite criado em: {sqlite_path}")
print("Você pode abrir este arquivo com o SQLite Browser ou DB Browser for SQLite")