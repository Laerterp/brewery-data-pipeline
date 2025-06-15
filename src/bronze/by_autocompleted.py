# bronze/by_autocomplete.py
import requests
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

def fetch_autocomplete(query):
    """Busca cervejarias usando o endpoint de autocomplete"""
    url = f"https://api.openbrewerydb.org/v1/breweries/autocomplete?query={query}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verifica erros HTTP
        return response.json()
    except Exception as e:
        print(f"Erro ao buscar autocomplete: {str(e)}")
        return None

def save_autocomplete_data(query, data, output_dir):
    """Salva os dados de autocomplete"""
    try:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"autocomplete_{query}_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✅ Dados salvos em: {output_file}")
        return output_file
    except Exception as e:
        print(f"Erro ao salvar arquivo: {str(e)}")
        return None

def main():
    # Configurações
    query = "san diego"
    output_dir = r"C:\Users\55349\brewery-data-pipeline\data\bronze\autocomplete"
    
    # Buscar dados
    print(f"🔍 Buscando cervejarias para: '{query}'")
    data = fetch_autocomplete(query)
    
    if data:
        # Salvar dados brutos
        saved_file = save_autocomplete_data(query.replace(" ", "_"), data, output_dir)
        if saved_file:
            # Converter para DataFrame e salvar como Parquet (opcional)
            df = pd.DataFrame(data)
            parquet_file = saved_file.with_suffix('.parquet')
            df.to_parquet(parquet_file)
            print(f"✅ Versão Parquet salva em: {parquet_file}")

if __name__ == "__main__":
    main()