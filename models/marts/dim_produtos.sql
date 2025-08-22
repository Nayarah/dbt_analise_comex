-- models/marts/dim_produtos.sql

{{
  config(
    materialized='table'
  )
}}

SELECT
    CAST(id_sh4 AS STRING) AS id_produto_sh4,
    CAST(nome_sh4_portugues AS STRING) AS nome_produto,
    CAST(id_sh2 AS STRING) AS id_produto_sh2,
    CAST(nome_sh2_portugues AS STRING) AS nome_produto_capitulo
FROM
        {{ source('br_bd_diretorios_mundo', 'sistema_harmonizado') }}
WHERE
       id_sh4 IS NOT NULL
GROUP BY  1, 2, 3, 4