import os
import pandas as pd
import base64
import requests
import psycopg2
from requests_oauthlib import OAuth1
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

RESTLET_URL = os.getenv("RESTLET_URL")
CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
TOKEN_SECRET = os.getenv("TOKEN_SECRET")
REALM = os.getenv("REALM")

def fetch_data():
    """Fetch data from the database for yesterday's payment completion date."""
    try:
        # Calculate yesterday's date
        payment_completion_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        print(f"📡 Fetching data for payment_completion_date: {payment_completion_date}...")

        query = f"""
        WITH tiktok_orders AS (
            SELECT name AS order_name,
                   tags,
                   (regexp_matches(tags, 'SO\\d+'))[1] AS po_number,
                   (regexp_matches(tags, 'TikTokOrderID:(\\d+)'))[1] AS tiktok_order_id
            FROM raw.raw_shpfy_orders
        )
        SELECT
            sfd.statement_date_utc,
            sfd.statement_id,
            sfd.payment_id,
            sfd.order_adjustment_id,
            sso.order_name AS "NETSUITE PO #",
            stp.payment_initiation_date_utc,
            stp.payment_amount,
            stp.payment_completion_date_utc
        FROM stage.stg_tiktok_financial_details sfd
        LEFT JOIN tiktok_orders sso 
        ON sso.tiktok_order_id::bigint = sfd.order_adjustment_id::bigint
        LEFT JOIN stage.stg_tiktok_payment_details stp 
        ON stp.payment_id::bigint = sfd.payment_id::bigint
        WHERE DATE(stp.payment_completion_date_utc) = '{payment_completion_date}'
        ORDER BY stp.payment_initiation_date_utc ASC;
        """

        with psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        ) as conn:
            df = pd.read_sql(query, conn)

        print("✅ Data fetched successfully.")

        # Correct column mappings
        column_mapping = {
            "statement_date_utc": "Statement date (UTC)",
            "statement_id": "Statement ID",
            "payment_id": "Payment ID",
            "order_adjustment_id": "Order/adjustment ID",
            "order_name": "NETSUITE PO #",
            "payment_initiation_date_utc": "Payment initiation date (UTC)",
            "payment_amount": "Payment amount",
            "payment_completion_date_utc": "Payment completion date (UTC)"
        }
        df.rename(columns=column_mapping, inplace=True)

        # Ensure correct column order
        column_order = [
            "Statement date (UTC)", "Statement ID", "Payment ID", "Order/adjustment ID",
            "NETSUITE PO #", "Payment initiation date (UTC)", "Payment amount", "Payment completion date (UTC)"
        ]
        df = df[column_order]

        # Convert date columns to required format
        date_columns = ["Statement date (UTC)", "Payment initiation date (UTC)", "Payment completion date (UTC)"]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m/%d/%Y')

        return df, payment_completion_date
    except Exception as e:
        print("❌ Error fetching data:", str(e))
        return None, None

def upload_to_netsuite(df, payment_completion_date):
    """Upload the DataFrame to NetSuite without saving it locally."""
    try:
        if not RESTLET_URL or not all([CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET, REALM]):
            raise ValueError("❌ Missing API credentials!")

        # Convert DataFrame to CSV in memory
        csv_data = df.to_csv(index=False).encode("utf-8")
        base64_encoded_csv = base64.b64encode(csv_data).decode("utf-8")

        # Dynamically generate filename
        csv_filename = f"TikTok_Settlement_{payment_completion_date.replace('-', '_')}.csv"

        payload = {
            "fileName": csv_filename,
            "fileData": base64_encoded_csv
        }

        auth = OAuth1(
            CONSUMER_KEY,
            CONSUMER_SECRET,
            ACCESS_TOKEN,
            TOKEN_SECRET,
            realm=REALM,
            signature_method="HMAC-SHA256"
        )

        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }

        response = requests.post(RESTLET_URL, headers=headers, auth=auth, json=payload)

        print("📡 NetSuite Response - Status Code:", response.status_code)
        print("📡 Response:", response.text)
    except Exception as e:
        print("❌ Error uploading CSV:", str(e))

def main():
    """Main function to fetch and upload data."""
    print("🚀 Starting TikTok Settlement Job...")
    df, payment_completion_date = fetch_data()
    if df is not None and payment_completion_date:
        upload_to_netsuite(df, payment_completion_date)
    print("✅ Job completed.")

if __name__ == "__main__":
    main()