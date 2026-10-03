

SELECT

    event_id AS ai_usage_key,

    event_id,

    account_id AS customer_key,

    subscription_id AS subscription_key,

    CAST(event_timestamp AS DATE) AS date_key,

    model_name AS ai_model_key,

    feature_name AS feature_key,

    input_tokens,

    output_tokens,

    total_tokens,

    latency_ms,

    status,

    error_type,

    CASE
        WHEN status = 'success'
        THEN 1
        ELSE 0
    END AS success_flag,

    CASE
        WHEN status != 'success'
        THEN 1
        ELSE 0
    END AS failure_flag,

    source_file,

    ingested_at

FROM {{ ref('stg_ai_usage_events') }}