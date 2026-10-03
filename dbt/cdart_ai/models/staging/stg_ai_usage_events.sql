{{ config(
    materialized='view'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'AI_USAGE_EVENTS') }}

),

renamed AS (

    SELECT
        RAW_EVENT:event_id::VARCHAR AS event_id,
        RAW_EVENT:account_id::VARCHAR AS account_id,
        RAW_EVENT:subscription_id::VARCHAR AS subscription_id,
        RAW_EVENT:event_timestamp::TIMESTAMP AS event_timestamp,
        RAW_EVENT:model::VARCHAR AS model_name,
        RAW_EVENT:feature::VARCHAR AS feature_name,
        RAW_EVENT:input_tokens::INTEGER AS input_tokens,
        RAW_EVENT:output_tokens::INTEGER AS output_tokens,
        (
        COALESCE(RAW_EVENT:input_tokens::INTEGER, 0)
        +
        COALESCE(RAW_EVENT:output_tokens::INTEGER, 0)
         ) AS total_tokens,
        RAW_EVENT:latency_ms::INTEGER AS latency_ms,
        RAW_EVENT:status::VARCHAR AS status,
        RAW_EVENT:error_type::VARCHAR AS error_type,
        SOURCE_FILE AS source_file,
        INGESTED_AT AS ingested_at

    FROM source

)

SELECT *
FROM renamed