import requests
import json
from datetime import datetime
from pathlib import Path

# Configuration
BASE_URL = "https://api.openbrewerydb.org/v1/breweries/meta"
PARAMS = {'by_type': 'micro'}  # Filter for microbreweries
OUTPUT_DIR = "data/bronze/breweries_metadata"

def fetch_microbreweries_metadata():
    """Fetch metadata about microbreweries from the API"""
    try:
        response = requests.get(BASE_URL, params=PARAMS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error accessing API: {e}")
        return None

def save_metadata(data):
    """Save metadata to JSON file"""
    # Create directory if it doesn't exist
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"microbreweries_metadata_{timestamp}.json"
    filepath = Path(OUTPUT_DIR) / filename
    
    # Save data
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Metadata saved to: {filepath}")
    return filepath

def run_extraction():
    """Run the complete extraction process"""
    print("Fetching microbreweries metadata from API...")
    
    # 1. Fetch data
    metadata = fetch_microbreweries_metadata()
    
    if not metadata:
        print("Failed to get metadata from API")
        return None
    
    # 2. Save metadata
    saved_path = save_metadata(metadata)
    
    print("Successfully extracted microbreweries metadata")
    print(f"Total microbreweries: {metadata.get('total', 'unknown')}")
    return saved_path

if __name__ == "__main__":
    run_extraction()