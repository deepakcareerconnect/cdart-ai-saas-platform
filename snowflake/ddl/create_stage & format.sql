<<<<<<< HEAD
USE database cdart_ai_saas_platform_DW;
USE schema RAW;

CREATE OR REPLACE FILE FORMAT cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE FILE FORMAT cdart_ai_saas_platform_DW.RAW.JSON_FORMAT
TYPE = JSON
STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE STAGE cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE
URL = 's3://cdart-ai-saas-data-platform/raw/'
STORAGE_INTEGRATION = s3_cdart_ai_saas_data_integration;

=======
USE database cdart_ai_saas_platform_DW;
USE schema RAW;

CREATE OR REPLACE FILE FORMAT cdart_ai_saas_platform_DW.RAW.CSV_FORMAT
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE FILE FORMAT cdart_ai_saas_platform_DW.RAW.JSON_FORMAT
TYPE = JSON
STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE STAGE cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE
URL = 's3://cdart-ai-saas-data-platform/raw/'
STORAGE_INTEGRATION = s3_cdart_ai_saas_data_integration;

>>>>>>> 448cad1969301acd4faf665775b240bfb03c46fa
LIST @cdart_ai_saas_platform_DW.RAW.cdart_ai_saas_platform_S3_STAGE;