# Adicione esta função ao seu script
def export_tables_to_parquet(conn, output_path):
    # Lista de tabelas para exportar
    tables_to_export = ['dim_location', 'dim_tipocervejarias', 'dim_status', 'fact_breweries']
    
    for table in tables_to_export:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            df.to_parquet(f"{output_path}/{table}.parquet")
            print(f" Tabela '{table}' exportada como Parquet")
        except Exception as e:
            print(f" Erro ao exportar {table}: {str(e)}")

# Modifique a função main para incluir a exportação
def main():
    # ... (código existente)
    
    # 5. Exportar tabelas dimensionais e fato como Parquet
    export_tables_to_parquet(conn, SILVER_PATH)
    
    # ... (código existente)