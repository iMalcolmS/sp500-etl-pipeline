import yfinance as yf

tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

data = yf.download(tickers, period="5d")

print(data["Close"])
