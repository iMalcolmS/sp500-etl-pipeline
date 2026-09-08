import yfinance as yf
import pandas as pd
from datetime import date
import os

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
OUTPUT_DIR = "data/raw"

def get_prices(tickers, period="5d"):
    data = yf.download(tickers, period=period)
    return data

def save_prices(data, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    today = date.today().isoformat()
    filepath = os.path.join(output_dir, f"{today}_prices.parquet")
    data.to_parquet(filepath)
    print(f"Saved data to {filepath}")

if __name__ == "__main__":
    prices = get_prices(TICKERS)
    save_prices(prices, OUTPUT_DIR)
