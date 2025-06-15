import pandas as pd
import os
from datetime import datetime

# Caminho do arquivo JSON (autocomplete)
file_path = r"C:\Users\55349\brewery-data-pipeline\data\bronze\autocomplete\autocomplete_san_diego_20250615_014527.json"

# Ler o JSON em um DataFrame
df = pd.read_json(file_path)

# Análise inicial dos dados
print(f"Total de registros: {len(df)}")
print("\nPrimeiras linhas:")
print(df.head())

print("\nTipos de dados:")
print(df.dtypes)

print("\nValores nulos por coluna:")
print(df.isnull().sum())

# Conversão de tipos de dados
df['address_2'] = df['address_2'].fillna('').astype(str)
df['address_3'] = df['address_3'].fillna('').astype(str)
df['phone'] = df['phone'].fillna('N/A').astype(str)
df['postal_code'] = df['postal_code'].astype(str)
df['website_url'] = df['website_url'].fillna('N/A').astype(str)

print("\nTipos de dados após conversão:")
print(df.dtypes)

# Preencher valores nulos de forma consistente
df.fillna({
    'address_2': '',
    'address_3': '',
    'phone': 'N/A',
    'website_url': 'N/A',
    'longitude': 0.0,
    'latitude': 0.0
}, inplace=True)

# Criar uma coluna de localização consolidada
df['location'] = df['city'] + ', ' + df['state_province'] + ', ' + df['country']

# Renomear colunas para snake_case
df.rename(columns={
    'brewery_type': 'tipos_cervejaria',
    'state_province': 'estado_provincia',
    'postal_code': 'codigo_postal',
    'website_url': 'site'
}, inplace=True)

# Adicionar metadados (timestamp de processamento)
df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Gerar nome do arquivo de saída com timestamp atual
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
output_filename = f"autocomplete_san_diego_{current_time}.parquet"
output_path = os.path.join(
    r"C:\Users\55349\brewery-data-pipeline\data\silver\autocomplete",
    output_filename
)

# Criar diretório se não existir
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Salvar em Parquet
df.to_parquet(
    output_path,
    engine='pyarrow',
    compression='snappy'
)

print(f"\nProcessamento concluído! Arquivo salvo em: {output_path}")