import pandas as pd
from pathlib import Path
import os
import glob

def consolidate_brewery_data(input_paths, output_path):
    """
    Consolida arquivos de cervejarias com estrutura similar (estado + tipo)
    """
    try:
        # 1. Verificar arquivos de entrada
        existing_files = [f for f in input_paths if Path(f).exists()]
        if not existing_files:
            raise FileNotFoundError("Nenhum arquivo de entrada encontrado")
        
        print("🔍 Arquivos encontrados para análise:")
        for f in existing_files:
            print(f"- {f}")

        # 2. Carregar e padronizar os DataFrames
        dfs = []
        
        for file_path in existing_files:
            df = pd.read_parquet(file_path)
            
            # Padronizar nomes de colunas (case insensitive)
            df.columns = df.columns.str.lower()
            
            # Verificar se tem as colunas essenciais
            required_cols = {'state', 'brewery_count', 'brewery_type', 'processed_at'}
            if not required_cols.issubset(df.columns):
                print(f"\n⚠️ Arquivo ignorado: {Path(file_path).name}")
                print("Motivo: Faltam colunas essenciais")
                print("Colunas encontradas:", df.columns.tolist())
                continue
            
            print(f"\n✅ Arquivo compatível: {Path(file_path).name}")
            print("Colunas:", df.columns.tolist())
            
            # Selecionar apenas as colunas relevantes
            standardized_df = df[['state', 'brewery_count', 'brewery_type', 'processed_at']].copy()
            
            # Adicionar origem dos dados
            standardized_df['data_source'] = Path(file_path).stem
            dfs.append(standardized_df)

        # 3. Verificar se temos arquivos para consolidar
        if not dfs:
            raise ValueError("Nenhum arquivo com estrutura compatível foi encontrado")

        # 4. Consolidar dados
        consolidated_df = pd.concat(dfs, ignore_index=True)
        
        # Processamento adicional
        consolidated_df = consolidated_df.sort_values(['state', 'brewery_type'])
        consolidated_df['consolidated_at'] = pd.Timestamp.now()
        
        # 5. Salvar resultado
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        consolidated_df.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy'
        )
        
        print(f"\n🎉 Consolidação concluída! Arquivo salvo em: {output_path}")
        print("\nResumo dos dados consolidados:")
        
        # Estatísticas consolidadas
        print("\nTotal de registros:", len(consolidated_df))
        print("\nDistribuição por tipo:")
        print(consolidated_df['brewery_type'].value_counts())
        print("\nTop 10 estados:")
        print(consolidated_df.groupby('state')['brewery_count'].sum().sort_values(ascending=False).head(10))
        
        return output_path
    
    except Exception as e:
        print(f"\n❌ Erro na consolidação: {str(e)}")
        raise

def find_latest_file(pattern):
    """Encontra o arquivo mais recente que corresponde ao padrão"""
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)

if __name__ == "__main__":
    # Lista dos padrões de arquivos que queremos buscar
    file_patterns = [
        r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_metadata\breweries_combined_*.parquet",
        r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_metadata\korea\korean_breweries_combined_*.parquet",
        r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_metadata\breweries_metadata_combined_*.parquet"
    ]
    
    # Encontrar os arquivos mais recentes para cada padrão
    latest_files = []
    for pattern in file_patterns:
        latest = find_latest_file(pattern)
        if latest:
            latest_files.append(latest)
    
    if not latest_files:
        print("❌ Nenhum arquivo válido encontrado para consolidação")
        exit(1)
    
    print("\n📌 Arquivos selecionados para consolidação:")
    for f in latest_files:
        print(f"- {f}")
    
    # Arquivo de saída
    output_file = r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_global_consolidated.parquet"
    
    try:
        consolidate_brewery_data(latest_files, output_file)
    except Exception as e:
        print(f"Falha no processo: {str(e)}")
        exit(1)
        