{{ config(
    materialized='view'
) }}

SELECT
    *
FROM {{ source('raw', 'ACCOUNTS') }}