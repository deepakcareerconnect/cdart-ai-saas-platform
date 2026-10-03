

SELECT

    payment_id AS payment_key,

    payment_id,

    account_id AS customer_key,

    subscription_id AS subscription_key,

    CAST(payment_date AS DATE) AS date_key,

    amount,

    currency,

    payment_status,

    payment_method,

    CASE
        WHEN payment_status = 'Succeeded'
        THEN 1
        ELSE 0
    END AS successful_payment_flag,

    CASE
        WHEN payment_status = 'Failed'
        THEN 1
        ELSE 0
    END AS failed_payment_flag

FROM {{ ref('stg_payments') }}