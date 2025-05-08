"""Constants to be used in the core module."""

import polars as pl

LOCAL_CACHE_DIR = ".monthly_candles"

BINANCE_KLINE_URL = "https://data.binance.vision/data/spot/monthly/klines"

BINANCE_KLINE_SCHEMA = {
    "timestamp": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "close_time": pl.Int64,
    "quote_asset_volume": pl.Float64,
    "number_of_trades": pl.Int64,
    "taker_buy_volume": pl.Float64,
    "taker_buy_quote_asset_volume": pl.Float64,
    "ignore": pl.Int64,
}

OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
