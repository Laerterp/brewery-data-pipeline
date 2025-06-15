import requests
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import time

# Configurações
STATE = "california"
SORT = "type,name:asc"
PER_PAGE = 3
BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
OUTPUT_DIR = "data/bronze/breweries_raw"
MAX_RETRIES = 3
RETRY_DELAY = 2  # segundos

def build_api_url(page: int = 1) -> str:
    """Constrói a URL da API com os parâmetros especificados"""
    params = {
        "by_state": STATE,
        "sort": SORT,
        "per_page": PER_PAGE,
        "page": page
    }
    return f"{BASE_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

def fetch_breweries_page(page: int = 1) -> Optional[Tuple[List[Dict], bool]]:
    """
    Busca uma página de cervejarias da API
    
    Args:
        page: Número da página a ser buscada
    
    Returns:
        Tupla com (lista de cervejarias, tem_more) ou None em caso de erro
    """
    url = build_api_url(page)
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if not isinstance(data, list):
                print(f"Resposta inesperada da API: {data}")
                return None
                
            # Verifica se há mais páginas (heurística simples)
            has_more = len(data) == PER_PAGE
            
            return data, has_more
            
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {attempt + 1} falhou: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                return None

def save_raw_data(data: List[Dict], page: int = None) -> Path:
    """
    Salva os dados brutos em formato JSON
    
    Args:
        data: Dados a serem salvos
        page: Número da página (opcional)
    
    Returns:
        Caminho completo do arquivo salvo
    """
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    page_suffix = f"_p{page}" if page is not None else ""
    filename = f"breweries_{STATE}{page_suffix}_{timestamp}.json"
    filepath = Path(OUTPUT_DIR) / filename
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Dados salvos em: {filepath}")
        return filepath
    except IOError as e:
        print(f"Erro ao salvar arquivo: {e}")
        raise

def fetch_all_breweries(max_pages: int = None) -> List[Dict]:
    """
    Busca todas as cervejarias (com paginação)
    
    Args:
        max_pages: Número máximo de páginas a buscar (None para todas)
    
    Returns:
        Lista combinada de todas as cervejarias
    """
    all_breweries = []
    current_page = 1
    
    while True:
        print(f"Buscando página {current_page}...")
        result = fetch_breweries_page(current_page)
        
        if result is None:
            print(f"Falha ao buscar página {current_page}")
            break
            
        breweries, has_more = result
        all_breweries.extend(breweries)
        
        if not has_more or (max_pages and current_page >= max_pages):
            break
            
        current_page += 1
        time.sleep(1)  # Delay para evitar rate limiting
    
    return all_breweries

def run_extraction(pages: int = 1) -> Optional[List[Path]]:
    """
    Executa o pipeline completo de extração
    
    Args:
        pages: Número de páginas a extrair (None para todas)
    
    Returns:
        Lista de caminhos dos arquivos salvos ou None em caso de erro
    """
    print(f"\n=== Iniciando extração de cervejarias na {STATE.upper()} ===")
    print(f"Parâmetros: sort={SORT}, per_page={PER_PAGE}")
    
    saved_files = []
    try:
        breweries = fetch_all_breweries(pages)
        if not breweries:
            print("Nenhuma cervejaria encontrada")
            return None
        
        # Salva os dados combinados
        saved_path = save_raw_data(breweries)
        saved_files.append(saved_path)
        
        print(f"\n=== Extração concluída ===")
        print(f"Total de cervejarias obtidas: {len(breweries)}")
        return saved_files
        
    except Exception as e:
        print(f"Erro durante a extração: {e}")
        return None

if __name__ == "__main__":
    # Exemplo de uso com tratamento de argumentos
    import sys
    
    try:
        pages = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    except ValueError:
        print("Uso: python script.py [pages=1]")
        sys.exit(1)
    
    result = run_extraction(pages)
    if not result:
        sys.exit(1)