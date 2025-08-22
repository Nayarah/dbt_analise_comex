-- models/staging/stg_comex_importacao.sql

WITH source AS (

    SELECT * FROM {{ source('br_me_comex_stat', 'municipio_importacao') }}

)

SELECT
    -- Chaves de identificação
    CAST(ano AS INT64) AS ano,
    CAST(mes AS INT64) AS mes,
    CAST(id_municipio AS STRING) AS id_municipio,
    CAST(id_pais AS STRING) AS id_pais,
    CAST(id_sh4 AS STRING) AS id_produto_sh4,
    CAST(sigla_uf AS STRING) AS sigla_uf,

    -- Métricas
    CAST(valor_fob_dolar AS FLOAT64) AS valor_fob_dolar,
    CAST(peso_liquido_kg AS INT64) AS peso_liquido_kg,

    -- Coluna de Data
    DATE(CAST(ano AS INT64), CAST(mes AS INT64), 1) AS data_mes

FROM
    source
WHERE id_municipio IS NOT NULL