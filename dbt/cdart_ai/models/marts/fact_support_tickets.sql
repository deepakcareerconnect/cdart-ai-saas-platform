

SELECT

    ticket_id AS support_ticket_key,

    ticket_id,

    account_id AS customer_key,

    CAST(submitted_at AS DATE) AS submitted_date_key,

    CAST(closed_at AS DATE) AS closed_date_key,

    submitted_at,

    closed_at,

    resolution_time_hours,

    priority,

    first_response_time_minutes,

    satisfaction_score,

    escalation_flag,

    CASE
        WHEN closed_at IS NOT NULL
        THEN 1
        ELSE 0
    END AS resolved_flag,

    CASE
        WHEN escalation_flag = TRUE
        THEN 1
        ELSE 0
    END AS escalation_count

FROM {{ ref('stg_support_tickets') }}