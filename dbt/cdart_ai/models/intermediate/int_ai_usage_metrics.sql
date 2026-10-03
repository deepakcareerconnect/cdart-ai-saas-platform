SELECT

    event_id,

    TRIM(account_id) AS account_id,

    TRIM(subscription_id) AS subscription_id,

    event_timestamp,

    CAST(event_timestamp AS DATE) AS event_date,

    DATE_TRUNC(
        'month',
        event_timestamp
    ) AS event_month,

    model_name,

    feature_name,

    input_tokens,

    output_tokens,

    total_tokens,

    latency_ms,

    UPPER(TRIM(status)) AS status,

    error_type,

    CASE
        WHEN UPPER(TRIM(status)) = 'SUCCESS'
        THEN 1
        ELSE 0
    END AS is_success,

    CASE
        WHEN UPPER(TRIM(status)) IN (
            'FAILED',
            'ERROR',
            'TIMEOUT'
        )
        THEN 1
        ELSE 0
    END AS is_failure,

    CASE
        WHEN COALESCE(total_tokens, 0) = 0
        THEN 0
        ELSE latency_ms / NULLIF(total_tokens, 0)
    END AS latency_per_token,

    source_file,

    ingested_at

FROM {{ ref('stg_ai_usage_events') }}