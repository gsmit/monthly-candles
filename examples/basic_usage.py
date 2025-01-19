from monthly_candles import fetch

# Define the symbol, timeframe, and months
symbol = "BTCUSDT"
timeframe = "1h"
start = "2019-08"
end = "2019-10"

# Fetch the monthly candles
df = fetch(symbol, timeframe, start, end)
print(df.head())
