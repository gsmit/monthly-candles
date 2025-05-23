# Monthly Candles
A lightweight Python library for retrieving historical monthly OHLCV data from Binance. The candle data is sourced from Binance's public spot market data repository, available at https://data.binance.vision/. Retrieved candles are returned as a Polars DataFrame, enabling seamless analysis and further data processing.

## Installation
Install by cloning this repository:
```bash
https://github.com/gsmit/monthly-candles.git
cd sliding-window-dataset
pip install -e .
```

## Usage
To fetch some candles, simply run the following:
```python
>>> from monthly_candles import fetch
>>> fetch("BTCUSDT", timeframe="1h", start="2019-08", end="2019-10")
shape: (2_208, 7)
┌─────────┬─────────────────────┬──────────┬──────────┬──────────┬──────────┬─────────────┐
│ symbol  ┆ timestamp           ┆ open     ┆ high     ┆ low      ┆ close    ┆ volume      │
│ ---     ┆ ---                 ┆ ---      ┆ ---      ┆ ---      ┆ ---      ┆ ---         │
│ str     ┆ datetime[ms]        ┆ f64      ┆ f64      ┆ f64      ┆ f64      ┆ f64         │
╞═════════╪═════════════════════╪══════════╪══════════╪══════════╪══════════╪═════════════╡
│ BTCUSDT ┆ 2019-08-01 00:00:00 ┆ 10080.53 ┆ 10155.06 ┆ 10030.11 ┆ 10043.02 ┆ 2234.513056 │
│ BTCUSDT ┆ 2019-08-01 01:00:00 ┆ 10043.0  ┆ 10065.07 ┆ 9980.0   ┆ 10020.29 ┆ 1239.042617 │
│ BTCUSDT ┆ 2019-08-01 02:00:00 ┆ 10022.0  ┆ 10038.56 ┆ 9988.88  ┆ 10029.27 ┆ 881.44564   │
│ BTCUSDT ┆ 2019-08-01 03:00:00 ┆ 10027.47 ┆ 10029.88 ┆ 9966.38  ┆ 9988.02  ┆ 1168.458852 │
│ BTCUSDT ┆ 2019-08-01 04:00:00 ┆ 9989.61  ┆ 9999.96  ┆ 9935.39  ┆ 9977.28  ┆ 1241.601555 │
│ …       ┆ …                   ┆ …        ┆ …        ┆ …        ┆ …        ┆ …           │
│ BTCUSDT ┆ 2019-10-31 19:00:00 ┆ 9229.96  ┆ 9250.0   ┆ 9206.89  ┆ 9211.04  ┆ 1090.639003 │
│ BTCUSDT ┆ 2019-10-31 20:00:00 ┆ 9211.02  ┆ 9243.47  ┆ 9123.33  ┆ 9185.98  ┆ 2463.66525  │
│ BTCUSDT ┆ 2019-10-31 21:00:00 ┆ 9182.82  ┆ 9200.0   ┆ 9013.0   ┆ 9126.2   ┆ 4114.414399 │
│ BTCUSDT ┆ 2019-10-31 22:00:00 ┆ 9126.22  ┆ 9157.07  ┆ 9083.86  ┆ 9122.69  ┆ 1348.611773 │
│ BTCUSDT ┆ 2019-10-31 23:00:00 ┆ 9123.92  ┆ 9157.0   ┆ 9105.07  ┆ 9140.85  ┆ 1083.614566 │
└─────────┴─────────────────────┴──────────┴──────────┴──────────┴──────────┴─────────────┘
```
