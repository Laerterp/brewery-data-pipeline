import pandas as pd
import os
from datetime import datetime
import json
from pathlib import Path

def find_metadata_file(input_path):
    """Find metadata file, handling both directories and specific files"""
    path = Path(input_path)
    
    if path.is_file():
        return str(path)
    elif path.is_dir():
        metadata_files = list(path.glob('*south_korea*metadata*.json'))
        if not metadata_files:
            metadata_files = list(path.glob('*korea*metadata*.json'))
            if not metadata_files:
                raise FileNotFoundError(f"No Korean metadata files found in {input_path}")
        return str(max(metadata_files, key=os.path.getmtime))
    else:
        raise FileNotFoundError(f"Path not found: {input_path}")

def transform_korean_metadata(input_path, output_dir):
    """Transform Korean metadata into separate silver layer files for states and types"""
    try:
        # 1. Find the file
        input_file = find_metadata_file(input_path)
        print(f"Processing Korean metadata file: {input_file}")
        
        # 2. Load data
        with open(input_file, 'r') as f:
            metadata = json.load(f)
        
        processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        source_file = os.path.basename(input_file)
        total_breweries = metadata.get('total', None)
        
        # 3. Process state data if exists
        state_df = pd.DataFrame()
        if 'by_state' in metadata:
            state_df = pd.DataFrame(list(metadata['by_state'].items()), 
                                 columns=['state', 'brewery_count'])
            state_df['processed_at'] = processing_time
            state_df['source_file'] = source_file
            state_df['total_breweries'] = total_breweries
            
            print("\n=== State Data Summary ===")
            print(f"Total states: {len(state_df)}")
            print(f"Total breweries accounted: {state_df['brewery_count'].sum()}")
            print("\nTop 5 states:")
            print(state_df.sort_values('brewery_count', ascending=False).head())
        
        # 4. Process type data if exists
        type_df = pd.DataFrame()
        if 'by_type' in metadata:
            type_df = pd.DataFrame(list(metadata['by_type'].items()),
                                columns=['brewery_type', 'count'])
            type_df['processed_at'] = processing_time
            type_df['source_file'] = source_file
            type_df['total_breweries'] = total_breweries
            
            print("\n=== Type Data Summary ===")
            print(f"Total types: {len(type_df)}")
            print(f"Total breweries accounted: {type_df['count'].sum()}")
            print("\nType distribution:")
            print(type_df)
        
        # 5. Save to silver layer
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save state data if exists
        if not state_df.empty:
            state_output = Path(output_dir) / f"korean_breweries_by_state_{timestamp}.parquet"
            state_df.to_parquet(state_output, engine='pyarrow', compression='snappy')
            print(f"\nSaved state data to: {state_output}")
        
        # Save type data if exists
        if not type_df.empty:
            type_output = Path(output_dir) / f"korean_breweries_by_type_{timestamp}.parquet"
            type_df.to_parquet(type_output, engine='pyarrow', compression='snappy')
            print(f"Saved type data to: {type_output}")
        
        return (
            str(state_output) if not state_df.empty else None,
            str(type_output) if not type_df.empty else None
        )
        
    except Exception as e:
        print(f"\nError processing Korean metadata: {str(e)}")
        raise

if __name__ == "__main__":
    # Pode ser um diretório ou arquivo específico
    input_path = r"C:\Users\55349\brewery-data-pipeline\data\bronze\breweries_metadata"
    
    output_dir = r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_metadata\korea"
    
    try:
        state_file, type_file = transform_korean_metadata(input_path, output_dir)
        print("\nProcessing complete!")
        if state_file:
            print(f"State data saved to: {state_file}")
        if type_file:
            print(f"Type data saved to: {type_file}")
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        exit(1)