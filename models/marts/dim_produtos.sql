-- models/marts/dim_produtos.sql

{{
  config(
    materialized='table'
  )
}}

WITH source_dicionario AS (
    SELECT * FROM {{ source('br_me_comex_stat', 'dicionario') }}
)

SELECT
    CAST(chave AS STRING) AS id_produto_sh4,
    CAST(valor AS STRING) AS nome_produto
FROM
    source_dicionario
WHERE
       id_tabela = 'sh4'