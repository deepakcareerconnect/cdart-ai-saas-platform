

SELECT
    subscription_id AS subscription_key,

    subscription_id,

    account_id AS customer_key,

    plan_tier,
    start_date,
    billing_frequency,
    seats,
    mrr_amount,
    arr_amount,

    is_trial,
    upgrade_flag,
    downgrade_flag,
    churn_flag,
    auto_renew_flag

FROM {{ ref('stg_subscriptions') }}