-- models/staging/stg_comex_exportacao.sql

WITH source AS (
    -- Usando a função source para buscar a tabela de exportação declarada no sources.yml
    SELECT * FROM {{ source('br_me_comex_stat', 'municipio_exportacao') }}
)

-- SELECT final para limpar e padronizar os dados
SELECT
    -- Chaves de identificação
    CAST(ano AS INT64) AS ano,
    CAST(mes AS INT64) AS mes,
    CAST(id_municipio AS STRING) AS id_municipio,
    CAST(id_pais AS STRING) AS id_pais,
    CAST(id_sh4 AS STRING) AS id_produto_sh4,
    CAST(sigla_uf AS STRING) AS sigla_uf,

    -- Métricas (os valores numéricos da análise)
    CAST(valor_fob_dolar AS FLOAT64) AS valor_fob_dolar,
    CAST(peso_liquido_kg AS INT64) AS peso_liquido_kg,

    -- Criando uma coluna de data real a partir do ano e mês para possibilitar melhor visualização no Power BI
    DATE(CAST(ano AS INT64), CAST(mes AS INT64), 1) AS data_mes

FROM
    source