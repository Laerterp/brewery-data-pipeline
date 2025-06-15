import requests
import json
from datetime import datetime
from pathlib import Path

# Configuration
BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
PARAMS = {
    'by_state': 'california',
    'sort': 'type,name:asc',
    'per_page': 3
}
OUTPUT_DIR = "data/bronze/breweries_raw"

def fetch_breweries():
    """Fetch breweries from California sorted by type and name"""
    try:
        response = requests.get(BASE_URL, params=PARAMS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error accessing API: {e}")
        return None

def save_raw_data(data):
    """Save raw data to JSON file"""
    # Create directory if it doesn't exist
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"california_breweries_sorted_{timestamp}.json"
    filepath = Path(OUTPUT_DIR) / filename
    
    # Save data
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Data saved to: {filepath}")
    return filepath

def run_extraction():
    """Run the complete extraction process"""
    print("Fetching California breweries sorted by type and name...")
    
    # 1. Fetch data
    breweries_data = fetch_breweries()
    
    if not breweries_data:
        print("Failed to get data from API")
        return None
    
    # 2. Save raw data
    saved_path = save_raw_data(breweries_data)
    
    print(f"Successfully extracted {len(breweries_data)} California breweries")
    print(f"Sorting: by type, then name ascending")
    return saved_path

if __name__ == "__main__":
    run_extraction()