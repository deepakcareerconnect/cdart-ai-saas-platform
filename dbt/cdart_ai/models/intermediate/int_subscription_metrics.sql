WITH subscriptions AS (

    SELECT
        subscription_id,
        account_id,
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

),

payment_metrics AS (

    SELECT

        subscription_id,

        COUNT(*) AS payment_count,

        COUNT_IF(payment_status = 'Succeeded')
            AS successful_payment_count,

        COUNT_IF(payment_status = 'Failed')
            AS failed_payment_count,

        SUM(
            CASE
                WHEN payment_status = 'Succeeded'
                THEN amount
                ELSE 0
            END
        ) AS total_payment_amount,

        MAX(payment_date) AS last_payment_date

    FROM {{ ref('stg_payments') }}

    GROUP BY subscription_id

),  -- <--- FIXED: Added the missing comma here!

support_metrics AS (

    SELECT

        account_id,

        COUNT(*) AS support_ticket_count,

        AVG(resolution_time_hours)
            AS avg_resolution_time_hours,

        AVG(first_response_time_minutes)
            AS avg_first_response_time_minutes,

        AVG(satisfaction_score)
            AS avg_satisfaction_score,

        COUNT_IF(escalation_flag = TRUE)
            AS escalated_ticket_count

    FROM {{ ref('stg_support_tickets') }}

    GROUP BY account_id

)

SELECT

    s.subscription_id,
    s.account_id,

    s.plan_tier,
    s.start_date,
    s.billing_frequency,
    s.seats,

    s.mrr_amount,
    s.arr_amount,

    s.is_trial,
    s.upgrade_flag,
    s.downgrade_flag,
    s.churn_flag,
    s.auto_renew_flag,

    DATEDIFF(
        'day',
        s.start_date,
        CURRENT_DATE()
    ) AS subscription_age_days,

    COALESCE(p.payment_count, 0)
        AS payment_count,

    COALESCE(p.successful_payment_count, 0)
        AS successful_payment_count,

    COALESCE(p.failed_payment_count, 0)
        AS failed_payment_count,

    COALESCE(p.total_payment_amount, 0)
        AS total_payment_amount,

    p.last_payment_date,

    COALESCE(st.support_ticket_count, 0)
        AS support_ticket_count,

    COALESCE(st.avg_resolution_time_hours, 0)
        AS avg_resolution_time_hours,

    COALESCE(st.avg_first_response_time_minutes, 0)
        AS avg_first_response_time_minutes,

    COALESCE(st.avg_satisfaction_score, 0)
        AS avg_satisfaction_score,

    COALESCE(st.escalated_ticket_count, 0)
        AS escalated_ticket_count

FROM subscriptions s

LEFT JOIN payment_metrics p
    ON s.subscription_id = p.subscription_id

LEFT JOIN support_metrics st
    ON s.account_id = st.account_id