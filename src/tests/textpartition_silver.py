import pandas as pd

# Ler o arquivo consolidado da Silver
df = pd.read_parquet(r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_consolidated.parquet")

# 1. Criar coluna de localização (usando os nomes das colunas renomeadas)
df['localizacao'] = df['city'] + ', ' + df['estado_provincia']

# 2. Definir colunas de particionamento (usando os nomes renomeados)
partition_cols = ['estado_provincia', 'city']  # Ou adicione 'country' se relevante

# 3. Salvar particionado
output_path = r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_partitioned"
df.to_parquet(
    output_path,
    partition_cols=partition_cols,
    engine='pyarrow',
    compression='snappy'  # Opcional: compressão para economizar espaço
)

print(f"Dados particionados salvos em: {output_path}")


# Ler uma partição específica para teste
test_df = pd.read_parquet(
    output_path,
    filters=[('estado_provincia', '=', 'California'), ('city', '=', 'San Diego')]
)
print(test_df.head())


