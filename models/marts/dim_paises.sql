-- models/marts/dim_paises.sql

-- Cria uma tabela dimensão para a tradução do país
{{
  config(
    materialized='table'
  )
}}

WITH source_dicionario AS (
    -- Seleciona a tabela dicionário que contém todas as traduções
    SELECT * FROM {{ source('br_me_comex_stat', 'dicionario') }}
)

SELECT
    CAST(chave AS STRING) AS id_pais,
    CAST(valor AS STRING) AS nome_pais
FROM
    source_dicionario
WHERE
    -- Filtra para pegar apenas as linhas que correspondem ao dicionário de países
    nome_coluna = 'id_pais'
    AND chave != '0'
    AND chave != '994'
GROUP BY
  1,2