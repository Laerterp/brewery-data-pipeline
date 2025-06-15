-- Consulta para consolidar dados de todas as tabelas de cervejarias
CREATE TABLE consolidated_breweries AS
SELECT * FROM breweries_bycity
UNION
SELECT * FROM breweries_bycountry
UNION
SELECT * FROM breweries_bydist
UNION
SELECT * FROM breweries_ids
UNION
SELECT * FROM breweries_list
UNION
SELECT * FROM breweries_postal
UNION
SELECT * FROM breweries_san_diego_20250611
UNION
SELECT * FROM breweries_state
UNION
SELECT * FROM breweries_type
UNION
SELECT * FROM autocomplete_san_diego_20250615_020731
UNION
SELECT * FROM breweries_search_20250615_020951;

-- Consulta para validar a junção de todas as tabelas de cervejarias

SELECT * FROM consolidated_breweries;

-- consulta para validar sobreposição de cervejarias, isso mostra uma discrepância dos dados
SELECT COUNT(*), COUNT(DISTINCT id) FROM consolidated_breweries;

-- como houve sobreposição e repetição de cervejaria, houve a tratativa para indentificar IDs problemáticos
-- Encontre os IDs que aparecem mais de uma vez
SELECT id, COUNT(*) as duplicate_count
FROM consolidated_breweries
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Portanto fomos corrigir a tabela consolidada para garantir que cada ID apareça apenas uma vez
-- Criação da tabela temporária
CREATE TABLE temp_breweries AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY 
               CASE WHEN name IS NOT NULL THEN 0 ELSE 1 END,
               CASE WHEN city IS NOT NULL THEN 0 ELSE 1 END,
               CASE WHEN country IS NOT NULL THEN 0 ELSE 1 END,
               CASE WHEN address_1 IS NOT NULL THEN 0 ELSE 1 END) AS rn
    FROM consolidated_breweries
) 
WHERE rn = 1;

-- Verificação dos resultados, sendo o resultado das mesmas iguais isso nos confirma a consistência dos dados
SELECT COUNT(*), COUNT(DISTINCT id) FROM temp_breweries;

-- Remova a versão antiga com duplicatas
DROP TABLE IF EXISTS consolidated_breweries;

-- Renome da tabela corrigida
ALTER TABLE temp_breweries RENAME TO consolidated_breweries;

-- Confirmação de que não há mais duplicatas
SELECT id, COUNT(*) 
FROM consolidated_breweries
GROUP BY id
HAVING COUNT(*) > 1;
-- retornou 0 linhas


-- Consulta SQL para análise de cervejarias por estado e tipo
SELECT 
    b.state,
    b.brewery_count AS total_breweries,
    m.brewery_type,
    COUNT(m.brewery_type) AS type_count
FROM 
    breweries_by_state_20250614_194248 AS b
LEFT JOIN 
    micro_states_20250614_194849 AS m ON b.state = m.state
GROUP BY 
    b.state, b.brewery_count, m.brewery_type
ORDER BY 
    b.state, type_count DESC;

    /*O que esta consulta faz:
Objetivo principal: Lista todos os estados com suas cervejarias, mostrando quantas são de cada tipo

Detalhes da operação:

Usa LEFT JOIN para garantir que todos os estados da tabela principal apareçam, mesmo sem cervejarias do tipo micro

O GROUP BY cria um registro para cada combinação estado/tipo

COUNT() calcula quantas cervejarias existem de cada tipo em cada estado*/

-- Consulta SQL para análise de cervejarias por estado e tipo (Análise de Cervejarias Coreanas)

SELECT 
    k.state,
    k.brewery_count AS total_korean_breweries,
    m.brewery_type,
    COUNT(m.brewery_type) AS microbrewery_count
FROM 
    korean_breweries_by_state_20250614_192933 AS k
LEFT JOIN 
    micro_states_20250614_194849 AS m ON k.state = m.state
WHERE 
    m.brewery_type = 'micro' OR m.brewery_type IS NULL
GROUP BY 
    k.state, k.brewery_count, m.brewery_type
ORDER BY 
    microbrewery_count DESC;

    -- Criação de uma view para visualizar ambas as tabelas
    CREATE VIEW combined_breweries_view AS
-- Consulta para cervejarias coreanas
SELECT 
    k.state,
    k.brewery_count AS total_breweries,
    m.brewery_type,
    COUNT(m.brewery_type) AS type_count,
    'Korean' AS source_dataset
FROM 
    korean_breweries_by_state_20250614_192933 AS k
LEFT JOIN 
    micro_states_20250614_194849 AS m ON k.state = m.state
WHERE 
    m.brewery_type = 'micro' OR m.brewery_type IS NULL
GROUP BY 
    k.state, k.brewery_count, m.brewery_type

UNION ALL

-- Consulta para outras cervejarias
SELECT 
    b.state,
    b.brewery_count AS total_breweries,
    m.brewery_type,
    COUNT(m.brewery_type) AS type_count,
    'Global' AS source_dataset
FROM 
    breweries_by_state_20250614_194248 AS b
LEFT JOIN 
    micro_states_20250614_194849 AS m ON b.state = m.state
GROUP BY 
    b.state, b.brewery_count, m.brewery_type;

    -- selecção para ver a view
    SELECT *FROM combined_breweries_view;


