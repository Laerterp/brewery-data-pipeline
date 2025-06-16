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


📊 ## Camada Silver para SQLite (Persistência)

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

Próximos passos --- 📊 Modelagem Dimensional (Camada Gold)
A modelagem dimensional foi implementada com base em um esquema estrela, otimizando a consulta analítica e facilitando agregações de dados por localização, tipo e status operacional das cervejarias.

Essa estrutura permite responder perguntas como:

Quantas cervejarias existem por estado e tipo?

Qual a proporção de microcervejarias em determinado país?

Como as cervejarias coreanas se comparam globalmente?

🛠️ Estrutura do Esquema Estrela
mermaid
Copiar
Editar
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
🗺️ Dimensão Geográfica – dim_location
Hierarquia natural: Country → State → City → Postal Code.

A tabela geográfica foi construída a partir dos dados únicos de cidade, estado e país, com identificação por location_id:

Campo	Descrição
city	Cidade da cervejaria
state	Estado/província
country	País
postal_code	Código postal (zipcode)
latitude/longitude	Coordenadas geográficas
full_location_description	Texto completo do local

Apenas registros com cidade e país foram considerados válidos.

🍺 Dimensão de Tipo – dim_tipocervejarias
Essa dimensão categoriza o tipo de cervejaria com uma descrição legível, útil para filtros e segmentações.

brewery_type (API)	Descrição legível
micro	Microcervejaria
brewpub	Pub cervejeiro
large	Grande cervejaria
planning	Planejamento
contract	Contrato
closed	Fechada
regional	Regional
outros	Outro tipo

✅ Dimensão de Status – dim_status
Criada com base na presença ou ausência de telefone. Ausência indica cervejaria inativa.

Status	Descrição Operacional
Ativo	Cervejaria operacional
Inativo	Cervejaria não operacional (sem telefone)

🧮 Tabela Fato – fact_breweries
A tabela fato conecta todas as dimensões e armazena os registros únicos de cada cervejaria, permitindo análises cruzadas.

Campo	Descrição
brewery_id	ID único da cervejaria
location_id	FK para localização geográfica
type_id	FK para tipo de cervejaria
status_id	FK para status operacional
created_at	Data de inserção
updated_at	Data da última atualização

🔍 Exemplos de Consultas Analíticas
1. Distribuição Geográfica por Tipo
sql
Copiar
Editar
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
2. Análise com Microcervejarias (Externa)
SELECT 
    COALESCE(l.state, k.state) AS state,
    COALESCE(l.country, 'South Korea') AS country,
    c.tipos_cervejaria,
    COUNT(DISTINCT c.id) AS brewery_count,
    k.brewery_count AS korean_brewery_count
FROM 
    consolidated_breweries c
LEFT JOIN 
    dim_location l ON c.state = l.state AND c.country = l.country
LEFT JOIN 
    korean_breweries_by_state_20250614_192933 k ON c.state = k.state
GROUP BY 
    COALESCE(l.state, k.state),
    COALESCE(l.country, 'South Korea'),
    c.tipos_cervejaria,
    k.brewery_count
ORDER BY 
    COALESCE(l.country, 'South Korea'), 
    COALESCE(l.state, k.state);

🍺 Quantidade de Cervejarias por Tipo e Localização
Estado/Província	País	Tipo	Quantidade
Bouche du Rhône	France	micro	1
Gangwondo	South Korea	brewpub	1
Jeollabukdo	South Korea	brewpub	1
Seoul	South Korea	brewpub	1
California	United States	brewpub	14
California	United States	closed	4
California	United States	contract	1
California	United States	large	5
California	United States	micro	24
California	United States	planning	3
California	United States	regional	2
Colorado	United States	brewpub	1
Oklahoma	United States	micro	1
Texas	United States	micro	1
Wisconsin	United States	micro	1

Essa view responde a umas das perguntas do case que é quantidade de cervejarias por tipo e localização.

🎯 Benefícios da Modelagem
Estrutura simplificada para análise com ferramentas como Power BI e Tableau.

Performance otimizada com uso de formato columnar (ex: Parquet na camada Silver).

Flexibilidade para integrar fontes externas (ex: Coreia, microcervejarias).

Facilidade na construção de dashboards e KPIs.




## Demonstração de Conhecimento em Orquestração de Dados com Apache Airflow

Documentação do Pipeline de Dados de Cervejaria
Ferramenta de Orquestração: Apache Airflow (Docker)
Repositório: brewery-data-pipeline

1. Estrutura Atual
1.1 Containers Docker
Serviços Principais:

airflow-apiserver: Interface web (porta 8080)

airflow-scheduler: Agendamento de DAGs

airflow-worker: Execução de tarefas

postgres: Banco de dados (porta 5432)

redis: Broker para Celery (porta 6379)

1.2 Diretórios
dags/: Armazena as DAGs (ex: brewery_pipeline.py)

logs/: Logs de execução

plugins/: Operadores customizados (se necessário)

1.3 Credenciais Padrão
Airflow UI: http://localhost:8080

Usuário: airflow

Senha: airflow

2. Monitoramento e Alertas
2.1 Objetivos
Detectar falhas no pipeline (erros em tarefas).

Identificar problemas de qualidade (dados inconsistentes, nulos, etc.).

Alertar em tempo real via Slack e e-mail.

2.2 Implementação
A. Monitoramento Básico
Ferramenta	Função
Airflow UI	Visualização manual do status das DAGs (Success, Failed, Running).
Prometheus	Coleta métricas (ex: tempo de execução, tarefas falhas).
Grafana	Dashboard com métricas em tempo real (ex: DAGs falhas nas últimas 24h).
B. Alertas Automatizados
Canal	Configuração
E-mail	Variáveis no docker-compose.yaml (SMTP) + email_on_failure=True nas DAGs.
Slack	Webhook + Callback nas DAGs (ex: on_failure_callback=slack_fail_alert).
Exemplo de Callback para Slack:

python
def slack_fail_alert(context):  
    message = f"""  
    :red_circle: Falha na DAG `{context['dag'].dag_id}`.  
    *Tarefa*: `{context['task'].task_id}`  
    *Erro*: `{context['exception']}`  
    *Log*: {context['task_instance'].log_url}  
    """  
    SlackWebhookOperator(  
        task_id='slack_alert',  
        slack_webhook_conn_id='slack_default',  
        message=message  
    ).execute(context)  
C. Validação de Dados
Checks na DAG:

python
def validate_data(**context):  
    data = context['ti'].xcom_pull(task_ids='extract_data')  
    if data.empty:  
        raise ValueError("Dados vazios!")  
    if data.duplicated().sum() > 0:  
        context['ti'].log.warning("Dados duplicados encontrados!")  
Ferramentas:

Great Expectations: Valida esquemas e estatísticas dos dados.

SQL Operators: Consultas para verificar consistência no PostgreSQL.

3. Tratamento de Falhas
Estratégia	Implementação
Retentativas	retries=3 e retry_delay=timedelta(minutes=5) nas DAGs.
Timeouts	execution_timeout=timedelta(minutes=30) para evitar tarefas travadas.
Dependências	Uso de ExternalTaskSensor para verificar pré-requisitos.
4. Próximos Passos
Adicionar testes automatizados para as DAGs (ex: com pytest).

Implementar dashboards no Grafana (ex: taxa de falhas, tempo médio de execução).

Alertas para qualidade de dados (ex: notificar se >5% dos registros forem nulos).

Como Executar
Iniciar containers:

bash
docker-compose -f docker-compose.yaml up -d  
Acessar o Airflow:
http://localhost:8080

Adicionar DAGs:
Salve arquivos .py em dags/.


✍️ Autor

Laerte Rocha Neves
