{{ config(
    materialized='view'
) }}

SELECT
    *
FROM {{ source('raw', 'SUPPORT_TICKETS') }}