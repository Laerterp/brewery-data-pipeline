# Executando a extração
saved_file = run_extraction()

# Mostrando a estrutura e conteúdo do arquivo gerado
if saved_file:
    print("\n=== Estrutura do Arquivo Gerado ===")
    print(f"Nome do arquivo: {saved_file.name}")
    print(f"Caminho completo: {saved_file}")
    print(f"Tamanho: {saved_file.stat().st_size} bytes")
    
    # Lendo e mostrando o conteúdo do arquivo
    print("\n=== Conteúdo do Arquivo JSON ===")
    with open(saved_file, 'r') as f:
        data = json.load(f)
        
        # Mostrando a estrutura (chaves do primeiro item)
        if len(data) > 0:
            first_item = data[0]
            print("\nEstrutura dos dados (campos disponíveis):")
            for key in first_item.keys():
                print(f"- {key}")
            
            # Mostrando amostra dos dados
            print("\nAmostra dos dados (3 primeiros registros):")
            for i, brewery in enumerate(data[:3]):
                print(f"\nRegistro {i+1}:")
                for key, value in brewery.items():
                    print(f"  {key}: {value}")
        else:
            print("O arquivo não contém dados.")