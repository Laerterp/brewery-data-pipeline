import requests
import json
from datetime import datetime
from pathlib import Path

# Configurações básicas
QUERY = "san diego"  # Termo de busca (pode usar espaços)
PER_PAGE = 3        # Número de cervejarias por requisição
API_URL = f"https://api.openbrewerydb.org/v1/breweries/search?query={QUERY.replace(' ', '%20')}&per_page={PER_PAGE}"
OUTPUT_DIR = "data/bronze/breweries_raw"

def fetch_breweries():
    """Busca dados da API de search"""
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
    filename = f"breweries_search_{timestamp}.json"
    filepath = Path(OUTPUT_DIR) / filename
    
    # Salva os dados
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Dados salvos em: {filepath}")
    return filepath

def run_extraction():
    """Executa todo o processo de extração"""
    print(f"Iniciando busca por cervejarias com o termo: '{QUERY}'...")
    
    # 1. Busca os dados
    breweries_data = fetch_breweries()
    
    if not breweries_data:
        print("Falha ao obter dados da API")
        return None
    
    # 2. Salva os dados brutos
    saved_path = save_raw_data(breweries_data)
    
    print(f"Encontradas {len(breweries_data)} cervejarias")
    print("Extração concluída com sucesso!")
    return saved_path

# Exemplo de uso:
if __name__ == "__main__":
    run_extraction()