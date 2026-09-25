from datetime import datetime, timedelta
import sys
sys.path.insert(0, "/Users/mayca/sp500-etl-pipeline")

from airflow.sdk import dag, task
from extract.get_prices import get_prices, save_prices_locally, upload_to_s3

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
OUTPUT_DIR = "data/raw"
BUCKET_NAME = "sp500-etl-raw-mcs2026"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),

}

@dag(
    dag_id="sp500_daily_pipeline",
    schedule= "@daily",
    start_date=datetime(2026,9,1),
    catchup=False,
    default_args= default_args,
    tags=["sp500","et1"],
)

def sp500_pipeline():

    @task
    def extract():
        return get_prices(TICKERS)

    @task
    def save_locally(data):
        return save_prices_locally(data, OUTPUT_DIR)

    @task
    def upload(filepath):
        upload_to_s3(filepath, BUCKET_NAME)

    raw_data = extract()
    local_path = save_locally(raw_data)
    upload(local_path)

sp500_pipeline()