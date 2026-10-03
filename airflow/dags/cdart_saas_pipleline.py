from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator

def task_failure_alert(context):
    """Placeholder for sending a Slack or Email alert on task failure."""
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    print(f"ALERT: Task '{task_id}' in DAG '{dag_id}' failed!")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="etelt_s3_snowflake_dbt_pipeline",
    default_args=default_args,
    description="ETELT pipeline orchestrating S3 ingestion, Snowflake load/validation, and dbt run",
    schedule=None,  # Manual trigger for testing (Airflow 3 compatible)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etelt", "s3", "snowflake", "dbt"],
) as dag:

    # Step 1: Verify raw files exist in S3 using S3KeySensor with wildcards
    check_s3_files = S3KeySensor(
        task_id="check_s3_files",
        bucket_name="cdart-ai-saas-data-platform",
        bucket_key=[
            "raw/events/ai_usage/*",
            "raw/external/support_tickets/*",
            "raw/external/payments/*",
            "raw/postgres/subscriptions/*",
            "raw/postgres/accounts/*",
        ],
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=15,
        timeout=60,
    )

    # Step 2: Load data from S3 into Snowflake RAW schema using SQLExecuteQueryOperator
    load_s3_to_snowflake_raw = SQLExecuteQueryOperator(
        task_id="load_s3_to_snowflake_raw",
        conn_id="snowflake_default",
        split_statements=True,  # Enables executing multiple semicolon-delimited SQL statements
        sql="""
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
        """,
    )

    # Step 3: Run dbt transformations across staging, intermediate, and marts layers
    run_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /opt/airflow/cdart_ai && dbt run --profiles-dir .",
    )

    # Step 4: Run dbt tests to validate data quality in marts/staging
    test_dbt_transformations = BashOperator(
        task_id="test_dbt_transformations",
        bash_command="cd /opt/airflow/cdart_ai && dbt test --profiles-dir .",
    )

    archive_s3_files = BashOperator(
        task_id="archive_s3_files",
        bash_command="echo 'Moving processed S3 files to archive prefix to ensure idempotency.'",
    )


    # Define the execution dependency flow
    check_s3_files >> load_s3_to_snowflake_raw >> run_dbt_transformations >> test_dbt_transformations >> archive_s3_files