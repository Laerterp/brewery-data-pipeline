 -- Por meio de um seleção foi avaliado muitos campos vazios
    SELECT COUNT(*) 
FROM breweries_consolidated 
WHERE city IS NULL OR country IS NULL;

--/* o Resultado foi 810, 
-- isso pode ser ocasionado por Falha na coleta dos dados (ex: API não retornou city ou country).
-- Cervejarias em planejamento (planning) sem localização definida.
-- Dados históricos de cervejarias fechadas (closed) sem informações completas.

-- a decisão foi Corrigir a tabela fonte (breweries_consolidated)

-- Foi realizar a Atualização do country nulo para 'Unknown'
UPDATE breweries_consolidated
SET country = 'Unknown'
WHERE country IS NULL;

-- realizamos o ajuste da dimensão de localização, para que ignore os registros nulos
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
FROM breweries_consolidated
WHERE city IS NOT NULL AND country IS NOT NULL;  -- Filtra registros nulos

--verificamos a contagem de registros nulos limpos, e os dados sairam de 810 para 105 
SELECT COUNT(*) FROM dim_location;

-- Verificamos se não há mais nulos em campos críticos:
SELECT 
    COUNT(*) AS total,
    SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country
FROM dim_location;

-- Criamos índices para otimizar consultas:
CREATE INDEX idx_dim_location_city ON dim_location(city);
CREATE INDEX idx_dim_location_country ON dim_location(country);

-- verificamos também na dimensão dim_tipocervejarias que apresentava alto volume de valores nulos;
  SELECT COUNT(*) 
FROM breweries_consolidated 
WHERE tipos_cervejaria IS NOT NULL;

-- realizamos mesma lógica para a dim_tipocervejarias

--primeiro realizamos o DROP da tabela dim_tipocervejarias 
DROP TABLE dim_tipocervejarias

--Posteriormente melhoramos nosso código
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
        WHEN tipos_cervejaria = 'regional' THEN 'Regional'  -- Padronizado para maiúscula
        ELSE 'Outro tipo'  -- Cobre casos não previstos (ex: 'nano', 'bar')
    END AS type_description
FROM breweries_consolidated
WHERE tipos_cervejaria IS NOT NULL 
  AND TRIM(tipos_cervejaria) != '';  -- Filtra nulos e strings vazias

  -- Validamos a tabela após a criação
  SELECT 
    brewery_type,
    type_description,
    COUNT(*) AS total
FROM dim_tipocervejarias
GROUP BY brewery_type, type_description
ORDER BY type_id;

-- alteramos o nome da tabela dim_tiposcervejarias para dim_brewery_type
ALTER TABLE dim_tipocervejarias RENAME TO dim_brewery_type;