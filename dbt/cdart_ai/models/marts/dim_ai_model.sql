
SELECT DISTINCT

    model_name AS ai_model_key,

    model_name

FROM {{ ref('stg_ai_usage_events') }}

WHERE model_name IS NOT NULL