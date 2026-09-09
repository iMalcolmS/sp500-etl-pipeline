import yfinance as yf
import boto3
from datetime import date
import os

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
OUTPUT_DIR = "data/raw"
BUCKET_NAME = "sp500-etl-raw-mcs2026"

def get_prices(tickers, period="5d"):
    data = yf.download(tickers, period=period)
    return data

def save_prices_locally(data, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    today = date.today().isoformat()
    filepath = os.path.join(output_dir, f"{today}_prices.parquet")
    data.to_parquet(filepath)
    print(f"Saved locally to {filepath}")
    return filepath

def upload_to_s3(filepath, bucket_name):
    s3 = boto3.client("s3")
    filename = os.path.basename(filepath)
    s3_key = f"raw/{filename}"
    s3.upload_file(filepath, bucket_name, s3_key)
    print(f"Uploaded to s3://{bucket_name}/{s3_key}")

if __name__ == "__main__":
    prices = get_prices(TICKERS)
    local_path = save_prices_locally(prices, OUTPUT_DIR)
    upload_to_s3(local_path, BUCKET_NAME)
