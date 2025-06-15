import pandas as pd
import os
from datetime import datetime
import json
from pathlib import Path

def find_latest_metadata_file(input_dir):
    """Find the most recent metadata JSON file in the directory"""
    metadata_files = list(Path(input_dir).glob('*metadata*.json'))
    if not metadata_files:
        raise FileNotFoundError(f"No metadata files found in {input_dir}")
    
    # Get the most recent file by modification time
    latest_file = max(metadata_files, key=os.path.getmtime)
    return str(latest_file)

def transform_metadata_to_silver(input_dir, output_dir):
    """Transform metadata JSON into separate silver layer parquet files for states and types"""
    
    try:
        # 1. Find the latest metadata file automatically
        input_path = find_latest_metadata_file(input_dir)
        print(f"Found metadata file: {input_path}")
        
        # 2. Load the metadata JSON
        with open(input_path, 'r') as f:
            metadata = json.load(f)
        
        processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        source_file = os.path.basename(input_path)
        
        # 3. Create State DataFrame
        if 'by_state' in metadata:
            state_df = pd.DataFrame(list(metadata['by_state'].items()), 
                                  columns=['state', 'brewery_count'])
            state_df['processed_at'] = processing_time
            state_df['source_file'] = source_file
            state_df['total_breweries'] = metadata.get('total', None)
            
            print("\n=== State Data Summary ===")
            print(f"Total states: {len(state_df)}")
            print(f"Total breweries accounted: {state_df['brewery_count'].sum()}")
            print("\nTop 5 states:")
            print(state_df.sort_values('brewery_count', ascending=False).head())
        
        # 4. Create Type DataFrame
        if 'by_type' in metadata:
            type_df = pd.DataFrame(list(metadata['by_type'].items()),
                                 columns=['brewery_type', 'count'])
            type_df['processed_at'] = processing_time
            type_df['source_file'] = source_file
            type_df['total_breweries'] = metadata.get('total', None)
            
            print("\n=== Type Data Summary ===")
            print(f"Total types: {len(type_df)}")
            print(f"Total breweries accounted: {type_df['count'].sum()}")
            print("\nType distribution:")
            print(type_df.sort_values('count', ascending=False))
        
        # 5. Save to silver layer
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save state data if exists
        if 'by_state' in metadata:
            state_output = Path(output_dir) / f"breweries_by_state_{timestamp}.parquet"
            state_df.to_parquet(state_output, engine='pyarrow', compression='snappy')
            print(f"\nSaved state data to: {state_output}")
        
        # Save type data if exists
        if 'by_type' in metadata:
            type_output = Path(output_dir) / f"breweries_by_type_{timestamp}.parquet"
            type_df.to_parquet(type_output, engine='pyarrow', compression='snappy')
            print(f"Saved type data to: {type_output}")
        
        return (
            str(state_output) if 'by_state' in metadata else None,
            str(type_output) if 'by_type' in metadata else None
        )
    
    except Exception as e:
        print(f"\nError processing metadata: {str(e)}")
        raise

# Example usage
if __name__ == "__main__":
    # Input directory containing metadata JSON files
    input_bronze_dir = r"C:\Users\55349\brewery-data-pipeline\data\bronze\breweries_metadata"
    
    # Output directory for silver layer
    output_silver_dir = r"C:\Users\55349\brewery-data-pipeline\data\silver\breweries_metadata"
    
    # Run transformation
    try:
        state_file, type_file = transform_metadata_to_silver(input_bronze_dir, output_silver_dir)
        print("\nProcessing complete!")
        if state_file:
            print(f"State data saved to: {state_file}")
        if type_file:
            print(f"Type data saved to: {type_file}")
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        exit(1)