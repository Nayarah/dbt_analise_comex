-- models/marts/fct_comex.sql
{{
  config(
    materialized='table'
  )
}}

-- Passo 1: Selecionar e renomear as colunas de importação
WITH importacao AS (
    SELECT
        data_mes,
        id_municipio,
        id_pais,
        id_produto_sh4,
        SUM (valor_fob_dolar) AS valor_fob_dolar_importado,
        SUM (peso_liquido_kg) AS peso_liquido_kg_importado
    FROM
        {{ ref('stg_comex_importacao') }}
    GROUP BY 1, 2, 3, 4
),

-- Passo 2: Selecionar e renomear as colunas de exportação
exportacao AS (
    SELECT
        data_mes,
        id_municipio,
        id_pais,
        id_produto_sh4,
        SUM (valor_fob_dolar) AS valor_fob_dolar_exportado,
        SUM (peso_liquido_kg) AS peso_liquido_kg_exportado
    FROM
        {{ ref('stg_comex_exportacao') }}
    GROUP BY 1, 2, 3, 4
),

-- Passo 3: Juntar importações e exportações em uma única tabela
-- Usamos um FULL OUTER JOIN para garantir que não perderemos meses/produtos/etc. que só tiveram importação ou só exportação
movimentacoes_unificadas AS (
    SELECT
        COALESCE(imp.data_mes, exp.data_mes) AS data_mes,
        COALESCE(imp.id_municipio, exp.id_municipio) AS id_municipio,
        COALESCE(imp.id_pais, exp.id_pais) AS id_pais,
        COALESCE(imp.id_produto_sh4, exp.id_produto_sh4) AS id_produto_sh4,
        imp.valor_fob_dolar_importado,
        imp.peso_liquido_kg_importado,
        exp.valor_fob_dolar_exportado,
        exp.peso_liquido_kg_exportado
    FROM
        importacao AS imp
    FULL OUTER JOIN
        exportacao AS exp
        ON imp.data_mes = exp.data_mes
        AND imp.id_municipio = exp.id_municipio
        AND imp.id_pais = exp.id_pais
        AND imp.id_produto_sh4 = exp.id_produto_sh4
)

-- Passo 4 (Final): Juntar os fatos (números) com as dimensões (legendas)
SELECT
    -- Chaves
    m.data_mes,
    m.id_municipio,
    m.id_pais,
    m.id_produto_sh4,

    -- Dimensões (as legendas que vêm das nossas tabelas dim_)
    mun.nome_municipio,
    mun.sigla_uf,
    p.nome_pais,
    prod.nome_produto,

    -- Fatos (os números)
    COALESCE(m.valor_fob_dolar_importado, 0) AS valor_fob_dolar_importado,
    COALESCE(m.peso_liquido_kg_importado, 0) AS peso_liquido_kg_importado,
    COALESCE(m.valor_fob_dolar_exportado, 0) AS valor_fob_dolar_exportado,
    COALESCE(m.peso_liquido_kg_exportado, 0) AS peso_liquido_kg_exportado

FROM
    movimentacoes_unificadas AS m
LEFT JOIN
    {{ ref('dim_municipio') }} AS mun ON m.id_municipio = mun.id_municipio
LEFT JOIN
    {{ ref('dim_paises') }} AS p ON m.id_pais = p.id_pais
LEFT JOIN
    {{ ref('dim_produtos') }} AS prod ON m.id_produto_sh4 = prod.id_produto_sh4
WHERE
    -- A cláusula WHERE remove o Brasil E os outros códigos especiais
    m.id_pais NOT IN ('105', '0', '994', '990')