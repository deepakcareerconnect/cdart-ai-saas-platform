<<<<<<< HEAD
USE database cdart_ai_saas_platform_DW;
USE schema RAW;
USE warehouse cdart_ai_saas_platform;


COPY INTO cdart_ai_saas_platform_DW.RAW.ACCOUNTS
FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/postgres/accounts/
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO cdart_ai_saas_platform_DW.RAW.SUBSCRIPTIONS
FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/postgres/subscriptions/
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO cdart_ai_saas_platform_DW.RAW.PAYMENTS
FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/external/payments/
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO cdart_ai_saas_platform_DW.RAW.SUPPORT_TICKETS
FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/external/support_tickets/
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO cdart_ai_saas_platform_DW.RAW.AI_USAGE_EVENTS
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()
    FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/events/ai_usage/
)
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.JSON_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';


SELECT
    RAW_EVENT:event_id::VARCHAR AS event_id,
    RAW_EVENT:account_id::VARCHAR AS account_id,
    RAW_EVENT:model::VARCHAR AS model,
    RAW_EVENT:feature::VARCHAR AS feature,
    RAW_EVENT:input_tokens::INTEGER AS input_tokens,
    RAW_EVENT:output_tokens::INTEGER AS output_tokens,
    RAW_EVENT:latency_ms::INTEGER AS latency_ms,
    RAW_EVENT:status::VARCHAR AS status
FROM cdart_ai_saas_platform_DW.RAW.AI_USAGE_EVENTS
LIMIT 5;

SELECT
    RAW_EVENT:event_id::VARCHAR AS EVENT_ID,
    RAW_EVENT:account_id::VARCHAR AS ACCOUNT_ID,
    RAW_EVENT:event_timestamp::TIMESTAMP AS EVENT_TIMESTAMP,
    RAW_EVENT:model_name::VARCHAR AS MODEL_NAME,
    RAW_EVENT:feature_name::VARCHAR AS FEATURE_NAME,
    RAW_EVENT:tokens_used::INTEGER AS TOKENS_USED,
    RAW_EVENT:cost::NUMBER(18,6) AS COST
FROM CDART_AI_SAAS_PLATFORM_DW.RAW.AI_USAGE_EVENTS
LIMIT 10;

=======
USE database cdart_ai_saas_platform_DW;
USE schema RAW;
USE warehouse cdart_ai_saas_platform;


COPY INTO cdart_ai_saas_platform_DW.RAW.ACCOUNTS
FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/postgres/accounts/
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO cdart_ai_saas_platform_DW.RAW.SUBSCRIPTIONS
FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/postgres/subscriptions/
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO cdart_ai_saas_platform_DW.RAW.PAYMENTS
FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/external/payments/
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO cdart_ai_saas_platform_DW.RAW.SUPPORT_TICKETS
FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/external/support_tickets/
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO cdart_ai_saas_platform_DW.RAW.AI_USAGE_EVENTS
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()
    FROM @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/events/ai_usage/
)
FILE_FORMAT = (
    FORMAT_NAME = cdart_ai_saas_platform_DW.RAW.JSON_FORMAT
)
ON_ERROR = 'ABORT_STATEMENT';


SELECT
    RAW_EVENT:event_id::VARCHAR AS event_id,
    RAW_EVENT:account_id::VARCHAR AS account_id,
    RAW_EVENT:model::VARCHAR AS model,
    RAW_EVENT:feature::VARCHAR AS feature,
    RAW_EVENT:input_tokens::INTEGER AS input_tokens,
    RAW_EVENT:output_tokens::INTEGER AS output_tokens,
    RAW_EVENT:latency_ms::INTEGER AS latency_ms,
    RAW_EVENT:status::VARCHAR AS status
FROM cdart_ai_saas_platform_DW.RAW.AI_USAGE_EVENTS
LIMIT 5;

SELECT
    RAW_EVENT:event_id::VARCHAR AS EVENT_ID,
    RAW_EVENT:account_id::VARCHAR AS ACCOUNT_ID,
    RAW_EVENT:event_timestamp::TIMESTAMP AS EVENT_TIMESTAMP,
    RAW_EVENT:model_name::VARCHAR AS MODEL_NAME,
    RAW_EVENT:feature_name::VARCHAR AS FEATURE_NAME,
    RAW_EVENT:tokens_used::INTEGER AS TOKENS_USED,
    RAW_EVENT:cost::NUMBER(18,6) AS COST
FROM CDART_AI_SAAS_PLATFORM_DW.RAW.AI_USAGE_EVENTS
LIMIT 10;

>>>>>>> 448cad1969301acd4faf665775b240bfb03c46fa
LIST @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE/postgres/accounts/;