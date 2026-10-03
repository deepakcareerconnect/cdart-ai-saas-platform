<<<<<<< HEAD
import boto3
from pathlib import Path
from datetime import date

BUCKET_NAME = "cdart-ai-saas-data-platform"

s3 = boto3.client("s3")

def upload_file(local_file, s3_key):
    local_file = Path(local_file)

    if not local_file.exists():
        print(f"ERROR: File not found → {local_file}")
        return
    
    print(f"Uploading {local_file}")
    s3.upload_file(str(local_file), BUCKET_NAME, s3_key)
    print(f"Uploaded → s3://{BUCKET_NAME}/{s3_key}")


if __name__ == "__main__":
    today = date.today()

    # Accounts
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\source\source_accounts.csv",
        f"raw/postgres/accounts/ingestion_date={today}/accounts.csv"
    )

    # Subscriptions
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\source\source_subscriptions.csv",
        f"raw/postgres/subscriptions/ingestion_date={today}/subscriptions.csv"
    )

    # Support
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\raw\source_support_tickets.csv",
        f"raw/external/support_tickets/ingestion_date={today}/support_tickets.csv"
    )

    # Payments
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\raw\source_payments.csv",
        f"raw/external/payments/ingestion_date={today}/payments.csv"
    )

    # AI events
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\raw\source_ai_usage_events.json", # <-- Comma fixed here!
        f"raw/events/ai_usage/year=2026/month=09/ai_usage_events.json"
    )
=======
import boto3
from pathlib import Path
from datetime import date

BUCKET_NAME = "cdart-ai-saas-data-platform"

s3 = boto3.client("s3")

def upload_file(local_file, s3_key):
    local_file = Path(local_file)

    if not local_file.exists():
        print(f"ERROR: File not found → {local_file}")
        return
    
    print(f"Uploading {local_file}")
    s3.upload_file(str(local_file), BUCKET_NAME, s3_key)
    print(f"Uploaded → s3://{BUCKET_NAME}/{s3_key}")


if __name__ == "__main__":
    today = date.today()

    # Accounts
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\source\source_accounts.csv",
        f"raw/postgres/accounts/ingestion_date={today}/accounts.csv"
    )

    # Subscriptions
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\source\source_subscriptions.csv",
        f"raw/postgres/subscriptions/ingestion_date={today}/subscriptions.csv"
    )

    # Support
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\raw\source_support_tickets.csv",
        f"raw/external/support_tickets/ingestion_date={today}/support_tickets.csv"
    )

    # Payments
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\raw\source_payments.csv",
        f"raw/external/payments/ingestion_date={today}/payments.csv"
    )

    # AI events
    upload_file(
        r"C:\Users\jades\Documents\Deepak Project\cdart_ai_saas_platform\data\raw\source_ai_usage_events.json", # <-- Comma fixed here!
        f"raw/events/ai_usage/year=2026/month=09/ai_usage_events.json"
    )
>>>>>>> 448cad1969301acd4faf665775b240bfb03c46fa
