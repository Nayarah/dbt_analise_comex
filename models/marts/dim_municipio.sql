-- models/marts/dim_municipio.sql

{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ ref('stg_municipios') }}