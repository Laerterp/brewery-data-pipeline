import duckdb
from pathlib import Path
import os

# Configurar caminhos
base_path = Path(r"C:\Users\55349\brewery-data-pipeline")
gold_path = base_path / "data" / "gold"
gold_path.mkdir(parents=True, exist_ok=True)

# Conectar ao DuckDB
con = duckdb.connect()

# 1. Carregar dados da Silver
con.execute(r"""
    CREATE TABLE silver_breweries AS
    SELECT * FROM 'C:\Users\55349\brewery-data-pipeline\data\silver\breweries_consolidated.parquet';
""")

# 2. Criar dimensões 
con.execute("""
    CREATE TABLE dim_localizacao AS
    SELECT 
        row_number() OVER () AS localizacao_id,
        city AS cidade,
        estado_provincia AS estado,
        country AS pais,
        codigo_postal AS cep,
        longitude,
        latitude,
        location
    FROM silver_breweries
    GROUP BY city, estado_provincia, country, codigo_postal, longitude, latitude, location;
""")

con.execute("""
    CREATE TABLE dim_tipo_cervejaria AS
    SELECT 
        row_number() OVER () AS tipo_id,
        tipos_cervejaria AS tipo,
        site
    FROM silver_breweries
    WHERE tipos_cervejaria IS NOT NULL
    GROUP BY tipos_cervejaria, site
""")

# Adicionando a nova dimensão de status
con.execute("""
    CREATE TABLE dim_status AS
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY CASE WHEN phone = 'N/A' THEN 'Inativo' ELSE 'Ativo' END) AS status_id,
        CASE 
            WHEN phone = 'N/A' THEN 'Inativo'
            ELSE 'Ativo'
        END AS status,
        CASE 
            WHEN phone = 'N/A' THEN 'Cervejaria não operacional'
            ELSE 'Cervejaria operacional'
        END AS status_description
    FROM silver_breweries;
""")

# 3. Criar tabela fato SEM JOINs (apenas dados brutos)
con.execute("""
    CREATE TABLE fato_cervejarias AS
    SELECT
        id,
        name AS nome,
        tipos_cervejaria,  -- Mantido como texto (depois podemos mapear para dim_tipo)
        address_1 AS endereco,
        address_2 AS endereco_2,
        phone AS telefone,
        site,
        street AS rua,
        city,              -- Dados brutos (depois podemos associar a dim_localizacao)
        estado_provincia,
        country,
        codigo_postal,
        longitude,
        latitude,
        location,
        CURRENT_TIMESTAMP AS data_carga,
        CASE WHEN phone = 'N/A' THEN 'Inativo' ELSE 'Ativo' END AS status  -- Adicionando status para possível join depois
    FROM silver_breweries;
""")

# 4. Exportar para Parquet (atualizado com a nova dimensão)
tables = ["dim_localizacao", "dim_tipo_cervejaria", "dim_status", "fato_cervejarias"]

for table in tables:
    output_file = gold_path / f"{table}.parquet"
    con.execute(f"""
        COPY {table} TO '{output_file.as_posix()}' (FORMAT PARQUET);
    """)
    print(f"Tabela {table} exportada para: {output_file}")

# Fechar conexão
con.close()