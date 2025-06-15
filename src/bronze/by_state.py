import requests
import json
from datetime import datetime
from pathlib import Path

# Configuration
BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
TARGET_STATE = "california"  # State name in lowercase
PER_PAGE = 3
OUTPUT_DIR = "data/bronze/breweries_raw"

def fetch_breweries():
    """Fetch breweries by state from the API"""
    params = {
        'by_state': TARGET_STATE,
        'per_page': PER_PAGE
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
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
    filename = f"breweries_in_{TARGET_STATE}_{timestamp}.json"
    filepath = Path(OUTPUT_DIR) / filename
    
    # Save data
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Data saved to: {filepath}")
    return filepath

def run_extraction():
    """Run the complete extraction process"""
    print(f"Fetching breweries in {TARGET_STATE.title()}...")
    
    # 1. Fetch data
    breweries_data = fetch_breweries()
    
    if not breweries_data:
        print("Failed to get data from API")
        return None
    
    # 2. Save raw data
    saved_path = save_raw_data(breweries_data)
    
    print(f"Successfully extracted {len(breweries_data)} breweries from {TARGET_STATE.title()}")
    return saved_path

if __name__ == "__main__":
    run_extraction()