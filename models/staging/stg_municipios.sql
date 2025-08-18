
-- O WITH cria uma "tabela temporária" para organizar nosso código
WITH source AS (

    -- A função source() busca a tabela que declaramos no nosso arquivo sources.yml
    -- É a forma profissional de referenciar dados brutos no dbt
    SELECT * FROM {{ source('br_bd_diretorios_brasil', 'municipio') }}

)

-- Nosso SELECT final, onde fazemos a limpeza
SELECT
    -- Convertendo os tipos de dados (casting) e renomeando as colunas para um padrão claro
    CAST(id_municipio AS STRING) AS id_municipio,
    CAST(nome AS STRING) AS nome_municipio,
    CAST(sigla_uf AS STRING) AS sigla_uf,
    CAST(nome_uf AS STRING) AS nome_uf,
    CAST(nome_regiao AS STRING) AS nome_regiao

FROM
    source