from monthly_candles import fetch

symbols = ["BTCUSDT", "ETHUSDT"]
df = fetch(symbols, timeframe="1h", start="2019-08", end="2019-10")
print(df)
