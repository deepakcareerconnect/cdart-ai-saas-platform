
SELECT
    account_id AS customer_key,
    account_id,
    account_name,
    industry,
    country,
    signup_date,
    referral_source,
    plan_tier,
    is_trial,
    churn_flag

FROM {{ ref('stg_accounts') }}