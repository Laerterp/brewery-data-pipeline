# 🍻 BEES Data Engineering - Breweries Case

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.5%2B-orange)](https://airflow.apache.org/)

Repositório do projeto de engenharia de dados para consumo e processamento de dados de cervejarias utilizando arquitetura medallion.

##  Objetivo

Implementar um pipeline de dados que:
1. Consome dados da [Open Brewery DB API](https://www.openbrewerydb.org/)
2. Processa seguindo a arquitetura medallion:
   - Bronze (dados brutos)
   - Silver (dados curados)
   - Gold (dados analíticos)
3. Particiona dados por localização geográfica


 # Projeto de Extração de Dados de Cervejarias

Pipeline para extração de dados de cervejarias próximas a determinadas coordenadas geográficas.

##  Visão Geral

Este projeto extrai dados básicos de cervejarias próximas a uma localização específica usando a [Open Brewery DB API](https://www.openbrewerydb.org/).

## Tecnologias

- **Linguagem**: Python 3.9+
- **Orquestração**: Apache Airflow 2.5+
- **Armazenamento**: SQLite (dev) / PostgreSQL (prod)
- **Processamento**: Pandas, PySpark
- **Infraestrutura**: Docker, Docker-compose
- **Monitoramento**: Prometheus + Grafana

## Configuração

```bash
# 1. Clonar repositório
git clone https://github.com/Laerterp/brewery-data-pipeline.git

# 2. Configurar ambiente
cd brewery-data-pipeline
cp .env.example .env

# 3. Iniciar containers
docker-compose up -d

bash
python extraction.py
Os dados serão salvos em data/bronze/breweries_raw/ com timestamp no nome do arquivo.

##Configurações Personalizáveis
Edite no código:

LATITUDE e LONGITUDE: Coordenadas geográficas de referência

PER_PAGE: Número de resultados por requisição (max 200 pela API)

OUTPUT_DIR: Diretório de saída dos dados brutos

##  Camada Bronze (Raw) 
**Formato**: JSON bruto (exatamente como recebido da API)  
**Contém**:
- `id`: Identificador único (string)
- `name`: Nome da cervejaria (string)
- `brewery_type`: Micro/brewpub/etc (string)
- `address_1/2/3`: Endereço completo (string|null)
- `latitude/longitude`: Coordenadas (float|null)
- `website_url`: URL (string|null)
- `updated_at`: Timestamp (string ISO 8601)

**Exemplo de arquivo**:
```json
{
  "id": "10-56-brewing-company-oxnard",
  "name": "10-56 Brewing Company",
  "brewery_type": "micro",
  "street": "302 North Ventura Road",
  "city": "Oxnard",
  "state": "California",
  "postal_code": "93030",
  "country": "United States",
  "longitude": "-119.2072836",
  "latitude": "34.2748005",
  "website_url": "http://www.1056brewingcompany.com"
}

Camada Silver (Processed) → Próxima etapa do pipeline
Diferenças-chave:

Estrutura normalizada (tabelas relacionais)

**Objetivo**: Dados limpos, normalizados e prontos para análise

### Transformações Realizadas

1. **Tratamento de Valores Nulos**:
   ```python
   df.fillna({
       'address_2': '',
       'address_3': '',
       'phone': 'N/A',
       'website_url': 'N/A',
       'longitude': 0.0,
       'latitude': 0.0
   }, inplace=True)

   2. Conversão de Tipos:

Endereços secundários para string

Telefones como texto (preservando formatos)

CEPs como string (mesmo contendo números)

## Estrutura Final (Parquet)
Coluna	Tipo	Descrição	Exemplo
id	string	ID único	"10-56-brewing-company"
name	string	Nome da cervejaria	"Stone Brewing"
tipos_cervejaria	string	Micro/brewpub/etc	"micro"
street	string	Endereço principal	"302 N Ventura Rd"
location	string	Localização completa	"Oxnard, CA, US"
processed_at	timestamp	Quando foi processado	"2024-06-15 14:30:00"

## Armazenamento
python
df.to_parquet(
    "data/silver/breweries_bycity.parquet",
    engine='pyarrow',
    compression='snappy'
)

## Particionamento na Camada Silver

**Objetivo**: Otimizar consultas por localização geográfica

### Estrutura de Particionamento
```bash
data/silver/
└── breweries_partitioned/
    ├── estado_provincia=California/
    │   ├── city=San Diego/
    │   │   └── part-00000.parquet
    │   └── city=Los Angeles/
    │       └── part-00001.parquet
    └── estado_provincia=New York/
        └── city=New York/
            └── part-00002.parquet

# Particionamento por estado e cidade
partition_cols = ['estado_provincia', 'city']
df.to_parquet(
    "data/silver/breweries_partitioned",
    partition_cols=partition_cols,
    engine='pyarrow',
    compression='snappy'
)

# Exemplo: Consulta eficiente para San Diego
pd.read_parquet(
    "data/silver/breweries_partitioned/estado_provincia=California/city=San Diego"
)
Custo-Eficiência: Ler apenas partições relevantes reduz I/O

Organização: Estrutura auto-documentada


## Camada Silver para SQLite (Persistência)

**Objetivo**: Criar um banco de dados relacional para análise ad-hoc e preparação da camada Gold

### Fluxo Completo
```mermaid
graph LR
    A[Bronze: JSON] --> B(Silver: Parquet)
    B --> C{SQLite}
    C --> D[Gold: Análises]

 Código de Carga
python
# silver_to_sqlite.py
import sqlite3
import pandas as pd
from glob import glob

# 1. Carrega todos os Parquets particionados
parquet_files = glob("data/silver/**/*.parquet", recursive=True)

# 2. Conecta ao SQLite
conn = sqlite3.connect("data/breweries.db")

# 3. Cria tabelas para cada partição
for file in parquet_files:
    df = pd.read_parquet(file)
    table_name = "breweries_" + file.split("/")[-2].replace("=", "_")  # Ex: breweries_estado_provincia_CA
    df.to_sql(table_name, conn, if_exists='replace', index=False)


Próximos Passos (Camada Gold)
**Objetivo**: Dados enriquecidos e agregados para análise business intelligence

Consolidação e Limpeza
1. Unificação de Fontes

CREATE TABLE gold.consolidated_breweries AS
SELECT * FROM breweries_bycity
UNION
SELECT * FROM breweries_bycountry
UNION
SELECT * FROM breweries_bydist;

 Tratamento de Duplicatas

-- Identificação
SELECT id, COUNT(*) as duplicates 
FROM consolidated_breweries
GROUP BY id
HAVING COUNT(*) > 1;

-- Correção (usando ROW_NUMBER para selecionar o registro mais completo)
CREATE TABLE gold.breweries_deduplicated AS
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY 
               CASE WHEN name IS NOT NULL THEN 0 ELSE 1 END,
               CASE WHEN address_1 IS NOT NULL THEN 0 ELSE 1 END) AS rn
    FROM consolidated_breweries
) WHERE rn = 1;

Consulta SQL para análise de cervejarias por estado e tipo
SELECT 
    b.state,
    b.brewery_count AS total_breweries,
    m.brewery_type,
    COUNT(m.brewery_type) AS type_count
FROM 
    breweries_by_state_20250614_194248 AS b
LEFT JOIN 
    micro_states_20250614_194849 AS m ON b.state = m.state
GROUP BY 
    b.state, b.brewery_count, m.brewery_type
ORDER BY 
    b.state, type_count DESC;

Comparativo Coreanas vs Globais

CREATE VIEW gold.korean_vs_global AS
SELECT 
    type,
    SUM(CASE WHEN source = 'Korean' THEN count ELSE 0 END) as korean,
    SUM(CASE WHEN source = 'Global' THEN count ELSE 0 END) as global,
    ROUND(SUM(CASE WHEN source = 'Korean' THEN count ELSE 0 END) * 100.0 / 
         SUM(count), 2) as korean_percentage
FROM (
    SELECT 'micro' as type, source, COUNT(*) as count FROM combined_view GROUP BY 1,2
    UNION ALL
    SELECT 'nan


Modelagem Dimensional (Camada Gold)

**Objetivo**: Estruturar dados para análise OLAP com dimensões hierárquicas e métricas de negócio

##  Esquema Estrela
```mermaid
erDiagram
    FACT_BREWERIES ||--o{ DIM_LOCATION : "localiza"
    FACT_BREWERIES ||--|{ DIM_BREWERY_TYPE : "classifica"
    FACT_BREWERIES ||--o{ DIM_STATUS : "possui"
    FACT_BREWERIES {
        string brewery_id PK
        int location_id FK
        int type_id FK
        int status_id FK
        date created_at
        date updated_at
    }

 Tabelas de Dimensão
1. DIM_LOCATION (Geografia)
sql
CREATE TABLE dim_location AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY city, estado_provincia, country) AS location_id,
    city,
    estado_provincia AS state,
    country,
    codigo_postal AS postal_code,
    longitude,
    latitude
FROM consolidated_breweries;
Hierarquia Natural:

text
Country → State → City → Postal Code
Atributos Chave:

Coordenadas geográficas (lat/long)

Descrição completa (full_location_description)

2. DIM_BREWERY_TYPE (Tipologia)
sql
CREATE TABLE dim_tipocervejarias AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY tipos_cervejaria) AS type_id,
    tipos_cervejaria AS brewery_type,
    CASE 
        WHEN tipos_cervejaria = 'micro' THEN 'Microcervejaria'
        WHEN tipos_cervejaria = 'brewpub' THEN 'Pub cervejeiro'
        -- ... outros mapeamentos
    END AS type_description
FROM breweries_consolidated;
Categorias:

Micro

Brewpub

Large

Contract

Planning

3. DIM_STATUS (Operacionalidade)
sql
CREATE TABLE dim_status AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY status) AS status_id,
    status,
    CASE 
        WHEN status = 'Inativo' THEN 'Cervejaria não operacional'
        ELSE 'Cervejaria operacional'
    END AS status_description
FROM (
    SELECT DISTINCT CASE WHEN phone = 'N/A' THEN 'Inativo' ELSE 'Ativo' END AS status
    FROM breweries_consolidated
);

CREATE TABLE fact_breweries AS
SELECT 
    c.id AS brewery_id,
    l.location_id,
    t.type_id,
    s.status_id,
    CURRENT_TIMESTAMP AS created_at, -- Valor padrão
    CURRENT_TIMESTAMP AS updated_at  -- Valor padrão
FROM consolidated_breweries c
JOIN dim_location l ON c.city = l.city AND c.estado_provincia = l.state
JOIN dim_tipocervejarias t ON c.tipos_cervejaria = t.brewery_type
JOIN dim_status s ON CASE WHEN c.phone = 'N/A' THEN 'Inativo' ELSE 'Ativo' END = s.status;


 Análises Habilitadas
1. Distribuição Geográfica por Tipo
SELECT 
    l.country,
    l.state,
    t.brewery_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY l.country, l.state), 1) AS percentage
FROM fact_breweries f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_tipocervejarias t ON f.type_id = t.type_id
GROUP BY 1, 2, 3;

2. Comparativo de tabela Coreano 

WITH korean_stats AS (
    SELECT 
        l.state,
        COUNT(DISTINCT CASE WHEN k.state IS NOT NULL THEN f.brewery_id END) AS korean_count
    FROM fact_breweries f
    JOIN dim_location l ON f.location_id = l.location_id
    LEFT JOIN korean_breweries_by_state k ON l.state = k.state
    GROUP BY 1
)
SELECT 
    l.state,
    l.country,
    COUNT(*) AS total,
    k.korean_count,
    ROUND(k.korean_count * 100.0 / COUNT(*), 1) AS korean_percentage
FROM fact_breweries f
JOIN dim_location l ON f.location_id = l.location_id
LEFT JOIN korean_stats k ON l.state = k.state
GROUP BY 1, 2, 4;


✍️ Autor

Laerte Rocha Neves
