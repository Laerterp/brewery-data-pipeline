import requests
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

# Configurações
DEFAULT_SIZE = 3  # Número padrão de cervejarias aleatórias
MAX_RETRIES = 3   # Máximo de tentativas em caso de falha
OUTPUT_DIR = "data/bronze/breweries_raw"

def fetch_random_breweries(size: int = DEFAULT_SIZE) -> Optional[List[Dict]]:
    """
    Obtém cervejarias aleatórias da API
    
    Args:
        size: Quantidade de cervejarias a retornar (1-50)
    
    Returns:
        Lista de dicionários com dados das cervejarias ou None em caso de erro
    """
    url = f"https://api.openbrewerydb.org/v1/breweries/random?size={size}"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Verifica se a resposta está no formato esperado
            data = response.json()
            if not isinstance(data, list):
                print(f"Resposta inesperada da API: {data}")
                return None
                
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {attempt + 1} falhou: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                return None

def save_raw_data(data: List[Dict], prefix: str = "random") -> Path:
    """
    Salva os dados brutos em formato JSON
    
    Args:
        data: Dados a serem salvos
        prefix: Prefixo para o nome do arquivo
    
    Returns:
        Caminho completo do arquivo salvo
    """
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"breweries_{prefix}_{timestamp}.json"
    filepath = Path(OUTPUT_DIR) / filename
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Dados salvos em: {filepath}")
        return filepath
    except IOError as e:
        print(f"Erro ao salvar arquivo: {e}")
        raise

def run_extraction(size: int = DEFAULT_SIZE) -> Optional[Path]:
    """
    Executa o pipeline completo de extração
    
    Args:
        size: Quantidade de cervejarias a recuperar
    
    Returns:
        Caminho do arquivo salvo ou None em caso de erro
    """
    print(f"\n=== Iniciando extração de {size} cervejaria(s) aleatória(s) ===")
    
    # Validação do parâmetro size
    if not 1 <= size <= 50:
        print("Erro: size deve estar entre 1 e 50")
        return None
    
    # 1. Extração
    breweries = fetch_random_breweries(size)
    if not breweries:
        print("Falha ao obter dados da API após várias tentativas")
        return None
    
    # 2. Persistência
    try:
        saved_path = save_raw_data(breweries)
    except Exception as e:
        print(f"Falha ao salvar dados: {e}")
        return None
    
    print(f"=== Extração concluída ===")
    print(f"Total de cervejarias obtidas: {len(breweries)}")
    return saved_path

if __name__ == "__main__":
    # Exemplo de uso com tratamento de argumentos
    import sys
    
    try:
        size = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SIZE
    except ValueError:
        print("Uso: python script.py [size=1-50]")
        sys.exit(1)
    
    result = run_extraction(size)
    if not result:
        sys.exit(1)