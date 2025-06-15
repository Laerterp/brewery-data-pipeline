import pandas as pd
import os
from datetime import datetime

# Caminho do arquivo JSON (breweries_search)
file_path = r"C:\Users\55349\brewery-data-pipeline\data\bronze\breweries_raw\breweries_search_20250615_015132.json"

# Ler o JSON em um DataFrame
try:
    df = pd.read_json(file_path)
    print("Arquivo carregado com sucesso!")
except Exception as e:
    print(f"Erro ao carregar o arquivo: {e}")
    exit()

# Análise inicial dos dados
print(f"\nTotal de registros: {len(df)}")
print("\nPrimeiras linhas:")
print(df.head())

print("\nTipos de dados originais:")
print(df.dtypes)

# Tratamento de dados
# 1. Converter colunas de endereço para string
address_cols = ['address_1', 'address_2', 'address_3']
for col in address_cols:
    if col in df.columns:
        df[col] = df[col].fillna('').astype(str)
    else:
        print(f"Aviso: Coluna {col} não encontrada no DataFrame")

# 2. Tratar outras colunas importantes
if 'phone' in df.columns:
    df['phone'] = df['phone'].fillna('N/A').astype(str)
    
if 'postal_code' in df.columns:
    df['postal_code'] = df['postal_code'].astype(str)

if 'website_url' in df.columns:
    df['website_url'] = df['website_url'].fillna('N/A').astype(str)

# 3. Preencher valores nulos
fill_values = {
    'address_2': '',
    'address_3': '',
    'phone': 'N/A',
    'website_url': 'N/A',
    'longitude': 0.0,
    'latitude': 0.0
}

# Aplicar apenas para colunas existentes
fill_values = {k: v for k, v in fill_values.items() if k in df.columns}
df.fillna(fill_values, inplace=True)

# 4. Criar coluna de localização (se todas as colunas necessárias existirem)
location_cols = ['city', 'state_province', 'country']
if all(col in df.columns for col in location_cols):
    df['location'] = df['city'] + ', ' + df['state_province'] + ', ' + df['country']
else:
    print("Aviso: Não foi possível criar coluna 'location' - colunas necessárias não encontradas")

# 5. Renomear colunas (opcional)
rename_map = {
    'brewery_type': 'tipos_cervejaria',
    'state_province': 'estado_provincia',
    'postal_code': 'codigo_postal',
    'website_url': 'site'
}

# Aplicar apenas renomeações para colunas existentes
rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
df.rename(columns=rename_map, inplace=True)

# 6. Adicionar metadados
df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df['source_file'] = os.path.basename(file_path)

# Verificação final
print("\nTipos de dados após transformação:")
print(df.dtypes)

print("\nValores nulos por coluna:")
print(df.isnull().sum())

# Salvar o arquivo processado
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
output_filename = f"breweries_search_{current_time}.parquet"
output_path = os.path.join(
    r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_search",
    output_filename
)

# Criar diretório se não existir
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Salvar em Parquet
try:
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy'
    )
    print(f"\nProcessamento concluído com sucesso! Arquivo salvo em: {output_path}")
    print(f"Total de registros processados: {len(df)}")
except Exception as e:
    print(f"\nErro ao salvar o arquivo: {e}")
    