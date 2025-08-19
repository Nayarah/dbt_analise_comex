-- models/staging/stg_municipios.sql

WITH source AS (
    SELECT * FROM {{ source('br_bd_diretorios_brasil', 'municipio') }}
)

SELECT
    CAST(id_municipio AS STRING) AS id_municipio,
    CAST(nome AS STRING) AS nome_municipio,
    CAST(sigla_uf AS STRING) AS sigla_uf,
    CAST(nome_uf AS STRING) AS nome_uf,
    CAST(nome_regiao AS STRING) AS nome_regiao
FROM
    source