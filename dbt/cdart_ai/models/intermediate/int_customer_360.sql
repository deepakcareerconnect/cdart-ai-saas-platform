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

        COUNT(DISTINCT subscription_id)
            AS subscription_count,

        SUM(mrr_amount)
            AS total_mrr,

        SUM(arr_amount)
            AS total_arr,

        SUM(seats)
            AS total_seats,

        MAX(start_date)
            AS latest_subscription_start_date,

        COUNT_IF(upgrade_flag = TRUE)
            AS upgrade_count,

        COUNT_IF(downgrade_flag = TRUE)
            AS downgrade_count,

        COUNT_IF(churn_flag = TRUE)
            AS churned_subscription_count

    FROM {{ ref('stg_subscriptions') }}

    GROUP BY account_id

),

payment_metrics AS (

    SELECT

        account_id,

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
        ) AS total_revenue

    FROM {{ ref('stg_payments') }}

    GROUP BY account_id

),

support_metrics AS (

    SELECT

        account_id,

        COUNT(*) AS total_support_tickets,

        COUNT_IF(escalation_flag = TRUE)
            AS escalated_ticket_count,

        AVG(resolution_time_hours)
            AS avg_resolution_time_hours,

        AVG(first_response_time_minutes)
            AS avg_first_response_time_minutes,

        AVG(satisfaction_score)
            AS avg_satisfaction_score

    FROM {{ ref('stg_support_tickets') }}

    GROUP BY account_id

)

SELECT

    a.account_id,
    a.account_name,
    a.industry,
    a.country,
    a.signup_date,
    a.referral_source,
    a.plan_tier,
    a.is_trial,
    a.churn_flag,

    /* Subscription */

    COALESCE(sm.subscription_count, 0)
        AS subscription_count,

    COALESCE(sm.total_mrr, 0)
        AS total_mrr,

    COALESCE(sm.total_arr, 0)
        AS total_arr,

    COALESCE(sm.total_seats, 0)
        AS total_seats,

    sm.latest_subscription_start_date,

    COALESCE(sm.upgrade_count, 0)
        AS upgrade_count,

    COALESCE(sm.downgrade_count, 0)
        AS downgrade_count,

    COALESCE(sm.churned_subscription_count, 0)
        AS churned_subscription_count,

    /* Revenue */

    COALESCE(pm.payment_count, 0)
        AS payment_count,

    COALESCE(pm.successful_payment_count, 0)
        AS successful_payment_count,

    COALESCE(pm.failed_payment_count, 0)
        AS failed_payment_count,

    COALESCE(pm.total_revenue, 0)
        AS total_revenue,

    /* Support */

    COALESCE(st.total_support_tickets, 0)
        AS total_support_tickets,

    COALESCE(st.escalated_ticket_count, 0)
        AS escalated_ticket_count,

    COALESCE(st.avg_resolution_time_hours, 0)
        AS avg_resolution_time_hours,

    COALESCE(st.avg_first_response_time_minutes, 0)
        AS avg_first_response_time_minutes,

    COALESCE(st.avg_satisfaction_score, 0)
        AS avg_satisfaction_score,

FROM accounts a

LEFT JOIN subscription_metrics sm
    ON a.account_id = sm.account_id

LEFT JOIN payment_metrics pm
    ON a.account_id = pm.account_id

LEFT JOIN support_metrics st
    ON a.account_id = st.account_id