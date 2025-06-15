import pandas as pd
import os
from datetime import datetime

# Caminho do arquivo JSON de exemplo
file_path = r"C:\Users\55349\brewery-data-pipeline\data\bronze\breweries_raw\micro_breweries_20250611_232153.json"

# Ler o JSON em um DataFrame
df = pd.read_json(file_path)


print(f"Total de registros: {len(df)}")
print("\nPrimeiras linhas:")
print(df.head())

print("\nTipos de dados:")
print(df.dtypes)

print("\nValores nulos por coluna:")
print(df.isnull().sum())

# converter address_2 e address_3 para string
df['address_2'] = df['address_2'].fillna('').astype(str)  # Substitui NaN por string vazia
df['address_3'] = df['address_3'].fillna('').astype(str)

#Converter phone para String (e tratar números como texto)
df['phone'] = df['phone'].fillna('N/A').astype(str)  # Se null, vira "N/A"

#Garantir que postal_code seja String (caso contenha números)
df['postal_code'] = df['postal_code'].astype(str)  # CEPs podem ter hífens ou letras

#Verificar website_url (se houver URLs inválidas)
df['website_url'] = df['website_url'].fillna('N/A').astype(str)

print("\nTipos de dados:")
print(df.dtypes) #visualizando as transformações dos tipos de dados e se estão corretos

# Preencher valores nulos de forma consistente
df.fillna({
    'address_2': '',
    'address_3': '',
    'phone': 'N/A',
    'website_url': 'N/A',
    'longitude': 0.0,
    'latitude': 0.0
}, inplace=True)

# Criar uma coluna de localização consolidada (para particionamento futuro)
df['location'] = df['city'] + ', ' + df['state_province'] + ', ' + df['country']

# Renomear colunas para snake_case (opcional, mas recomendado)
df.rename(columns={
    'brewery_type': 'tipos_cervejaria',
    'state_province': 'estado_provincia',
    'postal_code': 'codigo_postal',
    'website_url': 'site'
}, inplace=True)


# 5. Adicionar metadados (timestamp de processamento)
df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Caminho de saída (Silver Layer)
output_path = r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_type.parquet"


# Salvar em Parquet (sem particionamento ainda, pois é um único arquivo)
df.to_parquet(
    output_path,
    engine='pyarrow',       # Mais eficiente que 'fastparquet'
    compression='snappy'     # Boa relação entre compressão e velocidade
)

print(f"Arquivo salvo em: {output_path}")



