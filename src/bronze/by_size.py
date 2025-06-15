import requests
import json
from datetime import datetime
from pathlib import Path

# Configurações básicas
SIZE = 3  # Número de cervejarias aleatórias para retornar
API_URL = f"https://api.openbrewerydb.org/v1/breweries/random?size={SIZE}"
OUTPUT_DIR = "data/bronze/breweries_raw"

def fetch_random_breweries():
    """Busca dados de cervejarias aleatórias da API"""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Verifica se houve erro
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a API: {e}")
        return None

def save_raw_data(data):
    """Salva os dados brutos em JSON"""
    # Cria o diretório se não existir
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Gera um nome de arquivo com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"breweries_random_{timestamp}.json"
    filepath = Path(OUTPUT_DIR) / filename
    
    # Salva os dados
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Dados salvos em: {filepath}")
    return filepath

def run_extraction():
    """Executa todo o processo de extração"""
    print(f"Iniciando extração de {SIZE} cervejarias aleatórias...")
    
    # 1. Busca os dados
    breweries_data = fetch_random_breweries()
    
    if not breweries_data:
        print("Falha ao obter dados da API")
        return None
    
    # 2. Salva os dados brutos
    saved_path = save_raw_data(breweries_data)
    
    print(f"Extraídas {len(breweries_data)} cervejarias aleatórias")
    print("Extração concluída com sucesso!")
    return saved_path

# Exemplo de uso:
if __name__ == "__main__":
    run_extraction()