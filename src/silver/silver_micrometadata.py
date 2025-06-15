import pandas as pd
import os
from datetime import datetime
import json
from pathlib import Path

def process_microbrewery_metadata(input_file, output_base_dir):
    """Processa o arquivo JSON de microcervejarias e salva em arquivos separados na pasta 'micro'"""
    try:
        # 1. Carregar o arquivo JSON especificado
        print(f"Processing microbrewery metadata file: {input_file}")
        with open(input_file, 'r') as f:
            metadata = json.load(f)
        
        # 2. Criar diretório 'micro' se não existir
        output_dir = Path(output_base_dir) / "micro"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 3. Processar dados por estado
        state_df = pd.DataFrame(list(metadata['by_state'].items()), 
                              columns=['state', 'brewery_count'])
        state_df['brewery_type'] = 'micro'  # Todos são micro
        state_df['processed_at'] = processing_time
        state_df['source_file'] = os.path.basename(input_file)
        
        print("\n=== State Data Summary ===")
        print(f"Total states: {len(state_df)}")
        print(f"Total microbreweries: {state_df['brewery_count'].sum()}")
        print("\nTop 5 states:")
        print(state_df.sort_values('brewery_count', ascending=False).head())
        
        # 4. Processar dados por tipo
        type_df = pd.DataFrame(list(metadata['by_type'].items()),
                             columns=['brewery_type', 'count'])
        type_df['processed_at'] = processing_time
        type_df['source_file'] = os.path.basename(input_file)
        
        print("\n=== Type Data Summary ===")
        print(f"Brewery type: {type_df.iloc[0]['brewery_type']}")
        print(f"Total count: {type_df.iloc[0]['count']}")
        
        # 5. Salvar arquivos na pasta 'micro'
        # Arquivo de estados
        state_output = output_dir / f"micro_states_{file_timestamp}.parquet"
        state_df.to_parquet(state_output, engine='pyarrow', compression='snappy')
        print(f"\nSaved state data to: {state_output}")
        
        # Arquivo de tipos
        type_output = output_dir / f"micro_types_{file_timestamp}.parquet"
        type_df.to_parquet(type_output, engine='pyarrow', compression='snappy')
        print(f"Saved type data to: {type_output}")
        
        return str(state_output), str(type_output)
    
    except Exception as e:
        print(f"\nError processing microbrewery metadata: {str(e)}")
        raise

if __name__ == "__main__":
    # Arquivo específico de microcervejarias
    input_file = r"C:\Users\55349\brewery-data-pipeline\data\bronze\breweries_metadata\microbreweries_metadata_20250614_012949.json"
    
    # Diretório de saída base (será criada a subpasta 'micro')
    output_base_dir = r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_metadata"
    
    try:
        state_file, type_file = process_microbrewery_metadata(input_file, output_base_dir)
        print("\nProcessing complete!")
        print(f"Microbrewery state data saved to: {state_file}")
        print(f"Microbrewery type data saved to: {type_file}")
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        exit(1)