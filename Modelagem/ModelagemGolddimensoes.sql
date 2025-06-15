---- Modelagem Dimensional para Análise de Cervejarias (Camada Gold)

-- Criação da dimensão Geográfica (dim_location)
CREATE TABLE dim_location AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY city, estado_provincia, country) AS location_id,
    city,
    estado_provincia AS state,
    country,
    codigo_postal AS postal_code,
    longitude,
    latitude,
    location AS full_location_description
FROM consolidated_breweries
WHERE city IS NOT NULL AND country IS NOT NULL;  -- Filtra linhas onde city e country não são nulos

--Seleção da tabela dim_location para visualização
SELECT * FROM dim_location;

-- Criação da dimensão tipo de cervejarias (dim_brewery_type)
CREATE TABLE dim_tipocervejarias AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY tipos_cervejaria) AS type_id,
    tipos_cervejaria AS brewery_type,
    CASE 
        WHEN tipos_cervejaria = 'micro' THEN 'Microcervejaria'
        WHEN tipos_cervejaria = 'brewpub' THEN 'Pub cervejeiro'
        WHEN tipos_cervejaria = 'large' THEN 'Grande cervejaria'
        WHEN tipos_cervejaria = 'closed' THEN 'Cervejaria fechada'
		WHEN tipos_cervejaria = 'planning' THEN 'Planejando'
		WHEN tipos_cervejaria = 'contract' THEN 'Contrato'
		WHEN tipos_cervejaria = 'regional' THEN 'regional'
        ELSE 'Outro tipo'
    END AS type_description
FROM breweries_consolidated;

--seleção da dimensão tipo
SELECT * FROM dim_tipocervejarias;

-- Criação da dimensão de status levando em consideração quando não tem telefone a cervejaria está inativa (dim_status)
CREATE TABLE dim_status AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY CASE WHEN phone = 'N/A' THEN 'Inativo' ELSE 'Ativo' END) AS status_id,
    CASE 
        WHEN phone = 'N/A' THEN 'Inativo'
        ELSE 'Ativo'
    END AS status,
    CASE 
        WHEN phone = 'N/A' THEN 'Cervejaria não operacional'
        ELSE 'Cervejaria operacional'
    END AS status_description
FROM breweries_consolidated;

--Fornece uma contagem geral de cervejarias agrupada por cidade, estado, país e tipo de cervejaria
SELECT 
    l.city,
    l.state,
    l.country,
    t.brewery_type,
    COUNT(*) AS brewery_count
FROM 
    breweries_consolidated f
JOIN 
    dim_location l ON f.city = l.city AND f.estado_provincia = l.state AND f.country = l.country
JOIN 
    dim_brewery_type t ON f.tipos_cervejaria = t.brewery_type
GROUP BY 
    l.city, l.state, l.country, t.brewery_type
ORDER BY 
    l.country, l.state, l.city, brewery_count DESC;

-    -- avaliando o endpoint Show micro breweries que me gera a tabela micro_states a qual possui dados de estado, número de cervejaria e o tipo micro, atráves deste left join conseguimos informações valiosas. 
    SELECT 
    l.state,
    l.country,
    t.brewery_type,
    COUNT(DISTINCT c.id) AS brewery_count,
    m.brewery_count AS micro_table_count
FROM 
    consolidated_breweries c
JOIN 
    dim_location l ON c.state = l.state AND c.country = l.country
JOIN 
    dim_tipocervejarias t ON c.tipos_cervejaria = t.brewery_type
LEFT JOIN 
    micro_states_20250614_194849 m ON c.state = m.state AND t.brewery_type = m.brewery_type
GROUP BY 
    l.state, l.country, t.brewery_type, m.brewery_count
ORDER BY 
    l.country, l.state;
-- resposta é uma tabela que aresenta a quantidade de cervejarias por tipos e localização.

   --avaliando o endpoint Show South Korean que me gera a tabela korean_breweries_by_state_20250614_192933 a qual possui dados de estado, número de cervejaria do sul Coreano atráves deste left join conseguimos informações valiosas. 
   SELECT 
    l.state,
    l.country,
    c.tipos_cervejaria,
    COUNT(DISTINCT c.id) AS global_brewery_count,
    k.brewery_count AS korean_brewery_count
FROM 
    consolidated_breweries c
JOIN 
    dim_location l ON c.state = l.state AND c.country = l.country
LEFT JOIN 
    korean_breweries_by_state_20250614_192933 k ON c.state = k.state
WHERE
    l.country = 'South Korea' OR k.state IS NOT NULL
GROUP BY 
    l.state, l.country, c.tipos_cervejaria, k.brewery_count
ORDER BY 
    l.country, l.state;
-- -- resposta é uma tabela que aresenta a quantidade de cervejarias e a localização apenas aos dados coreanos.

--- Aqui seria a junção correta com os dados da tabela Coreana
SELECT 
    COALESCE(l.state, k.state) AS state,
    COALESCE(l.country, 'South Korea') AS country,
    c.tipos_cervejaria,
    COUNT(DISTINCT c.id) AS brewery_count,
    k.brewery_count AS korean_brewery_count
FROM 
    consolidated_breweries c
LEFT JOIN 
    dim_location l ON c.state = l.state AND c.country = l.country
LEFT JOIN 
    korean_breweries_by_state_20250614_192933 k ON c.state = k.state
GROUP BY 
    COALESCE(l.state, k.state),
    COALESCE(l.country, 'South Korea'),
    c.tipos_cervejaria,
    k.brewery_count
ORDER BY 
    COALESCE(l.country, 'South Korea'), 
    COALESCE(l.state, k.state);
--resposta é uma tabela que aresenta a quantidade de cervejarias por tipos e localização, tanto as informações da tabela Coreana quanto os demais países