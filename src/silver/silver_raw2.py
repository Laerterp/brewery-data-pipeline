import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import sys

# Configurações
INPUT_FILE = r"C:\Users\55349\brewery-data-pipeline\data\bronze\breweries_raw\breweries_california_20250615_015949.json"
OUTPUT_DIR = "data/silver/breweries_processed"

def load_breweries_data(file_path: str) -> Optional[List[Dict]]:
    """Carrega os dados do arquivo JSON"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao ler arquivo {file_path}: {e}")
        return None

def process_breweries_data(breweries: List[Dict]) -> List[Dict]:
    """Processa os dados das cervejarias"""
    processed = []
    
    for brewery in breweries:
        # Exemplo de transformações:
        processed_brewery = {
            'id': brewery.get('id'),
            'name': brewery.get('name', '').title(),
            'brewery_type': brewery.get('brewery_type', '').capitalize(),
            'city': brewery.get('city', '').title(),
            'state': brewery.get('state', '').upper(),
            'country': brewery.get('country', '').upper(),
            'coordinates': {
                'latitude': float(brewery.get('latitude', 0)),
                'longitude': float(brewery.get('longitude', 0))
            } if brewery.get('latitude') and brewery.get('longitude') else None,
            'website_url': brewery.get('website_url', ''),
            'updated_at': brewery.get('updated_at', '')
        }
        processed.append(processed_brewery)
    
    return processed

def save_processed_data(data: List[Dict], input_path: Path) -> Path:
    """Salva os dados processados em novo arquivo"""
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"processed_{input_path.stem}_{timestamp}.json"
    filepath = Path(OUTPUT_DIR) / filename
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Dados processados salvos em: {filepath}")
        return filepath
    except IOError as e:
        print(f"Erro ao salvar arquivo: {e}")
        raise

def run_processing_pipeline():
    """Executa todo o pipeline de processamento"""
    print("\n=== Iniciando processamento dos dados ===")
    
    # 1. Carregar dados brutos
    raw_data = load_breweries_data(INPUT_FILE)
    if not raw_data:
        print("Falha ao carregar dados brutos")
        return None
    
    print(f"Total de cervejarias carregadas: {len(raw_data)}")
    
    # 2. Processar dados
    processed_data = process_breweries_data(raw_data)
    
    # 3. Salvar dados processados
    input_path = Path(INPUT_FILE)
    try:
        saved_path = save_processed_data(processed_data, input_path)
    except Exception as e:
        print(f"Falha ao salvar dados processados: {e}")
        return None
    
    print("\n=== Processamento concluído com sucesso ===")
    return saved_path

if __name__ == "__main__":
    result = run_processing_pipeline()
    if not result:
        sys.exit(1)