

SELECT DISTINCT

    feature_name AS feature_key,

    feature_name

FROM {{ ref('stg_ai_usage_events') }}

WHERE feature_name IS NOT NULL