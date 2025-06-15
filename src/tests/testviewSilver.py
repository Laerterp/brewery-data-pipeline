import pandas as pd

# Carregar o arquivo Parquet em um DataFrame
df = pd.read_parquet(r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_consolidated.parquet")

# Ver colunas
print("Colunas disponíveis:")
print(df.columns.tolist())

# Ver primeiras linhas
print("\nPrimeiras 5 linhas:")
print(df.head())

# Ver tipos de dados
print("\nTipos de dados:")
print(df.dtypes)

# Ver estatísticas descritivas (apenas para colunas numéricas)
print("\nEstatísticas descritivas:")
print(df.describe())