WITH accounts AS (

    SELECT
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

),

subscription_metrics AS (

    SELECT
        account_id,

        -- Number of subscriptions for the account
        COUNT(DISTINCT subscription_id) AS subscription_count,

        -- Active subscriptions
        COUNT(
            DISTINCT CASE
                WHEN churn_flag = FALSE
                THEN subscription_id
            END
        ) AS active_subscription_count,

        -- Total seats across subscriptions
        SUM(COALESCE(seats, 0)) AS total_seats,

        -- Current / total MRR
        SUM(COALESCE(mrr_amount, 0)) AS mrr_amount,

        -- Current / total ARR
        SUM(COALESCE(arr_amount, 0)) AS arr_amount,

        -- First subscription date
        MIN(start_date) AS first_subscription_date,

        -- Latest subscription date
        MAX(start_date) AS latest_subscription_date,

        -- Number of upgrades
        COUNT_IF(upgrade_flag = TRUE) AS upgrade_count,

        -- Number of downgrades
        COUNT_IF(downgrade_flag = TRUE) AS downgrade_count,

        -- Number of churned subscriptions
        COUNT_IF(churn_flag = TRUE) AS churned_subscription_count,

        -- Auto-renewing subscriptions
        COUNT_IF(auto_renew_flag = TRUE) AS auto_renew_subscription_count

    FROM {{ ref('stg_subscriptions') }}

    GROUP BY account_id

),

payment_metrics AS (

    SELECT
        account_id,

        COUNT(*) AS payment_count,

        COUNT_IF(
            payment_status = 'Succeeded'
        ) AS successful_payment_count,

        COUNT_IF(
            payment_status = 'Failed'
        ) AS failed_payment_count,

        SUM(
            CASE
                WHEN payment_status = 'Succeeded'
                THEN COALESCE(amount, 0)
                ELSE 0
            END
        ) AS total_revenue,

        SUM(
            CASE
                WHEN payment_status = 'Failed'
                THEN COALESCE(amount, 0)
                ELSE 0
            END
        ) AS failed_payment_amount,

        MIN(
            CASE
                WHEN payment_status = 'Succeeded'
                THEN payment_date
            END
        ) AS first_payment_date,

        MAX(
            CASE
                WHEN payment_status = 'Succeeded'
                THEN payment_date
            END
        ) AS last_payment_date,

        AVG(
            CASE
                WHEN payment_status = 'Succeeded'
                THEN amount
            END
        ) AS average_payment_amount

    FROM {{ ref('stg_payments') }}

    GROUP BY account_id

)

SELECT

    /* =====================================================
       ACCOUNT INFORMATION
       ===================================================== */

    a.account_id,
    a.account_name,
    a.industry,
    a.country,
    a.signup_date,
    a.referral_source,
    a.plan_tier,
    a.is_trial,
    a.churn_flag,

    /* =====================================================
       SUBSCRIPTION METRICS
       ===================================================== */

    COALESCE(
        s.subscription_count,
        0
    ) AS subscription_count,

    COALESCE(
        s.active_subscription_count,
        0
    ) AS active_subscription_count,

    COALESCE(
        s.total_seats,
        0
    ) AS total_seats,

    COALESCE(
        s.mrr_amount,
        0
    ) AS mrr_amount,

    COALESCE(
        s.arr_amount,
        0
    ) AS arr_amount,

    s.first_subscription_date,

    s.latest_subscription_date,

    COALESCE(
        s.upgrade_count,
        0
    ) AS upgrade_count,

    COALESCE(
        s.downgrade_count,
        0
    ) AS downgrade_count,

    COALESCE(
        s.churned_subscription_count,
        0
    ) AS churned_subscription_count,

    COALESCE(
        s.auto_renew_subscription_count,
        0
    ) AS auto_renew_subscription_count,

    /* =====================================================
       PAYMENT METRICS
       ===================================================== */

    COALESCE(
        p.payment_count,
        0
    ) AS payment_count,

    COALESCE(
        p.successful_payment_count,
        0
    ) AS successful_payment_count,

    COALESCE(
        p.failed_payment_count,
        0
    ) AS failed_payment_count,

    COALESCE(
        p.total_revenue,
        0
    ) AS total_revenue,

    COALESCE(
        p.failed_payment_amount,
        0
    ) AS failed_payment_amount,

    p.first_payment_date,

    p.last_payment_date,

    COALESCE(
        p.average_payment_amount,
        0
    ) AS average_payment_amount

FROM accounts a

LEFT JOIN subscription_metrics s
    ON a.account_id = s.account_id

LEFT JOIN payment_metrics p
    ON a.account_id = p.account_id