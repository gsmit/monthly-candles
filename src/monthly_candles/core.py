import io
import os
import shutil
import zipfile
from datetime import datetime
from typing import Optional

import polars as pl
import requests
from dateutil.relativedelta import relativedelta

__all__ = ["clear_cache", "fetch"]

_BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

_CACHE_DIR = ".monthly_candles"

_SCHEMA = {
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

_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


def _construct_url(symbol: str, timeframe: str, month: str) -> str:
    """Constructs the full URL for the data file.

    Args:
        symbol (str): The trading pair symbol (e.g., "BTCUSDT").
        timeframe (str): The timeframe (e.g., "1m").
        month (str): The month in "YYYY-MM" format (e.g., "2024-12").

    Returns:
        str: The constructed URL.
    """
    file_name = f"{symbol}-{timeframe}-{month}.zip"
    return f"{_BASE_URL}/{symbol}/{timeframe}/{file_name}"


def _download_zip_file(url: str) -> io.BytesIO:
    """Downloads a zip file from the given URL.

    Args:
        url (str): The URL of the zip file.

    Returns:
        io.BytesIO: The downloaded zip file content as a BytesIO object.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching data from {url}: {e}")


def _extract_csv_from_zip(zip_content: io.BytesIO) -> io.TextIOWrapper:
    """Extracts the first CSV file from the zip content.

    Args:
        zip_content (io.BytesIO): The content of the zip file.

    Returns:
        io.TextIOWrapper: A file-like object for the extracted CSV file.
    """
    with zipfile.ZipFile(zip_content) as z:
        csv_file_name = z.namelist()[0]  # Assuming there's only one file
        return io.TextIOWrapper(z.open(csv_file_name), encoding="utf-8")


def _csv_to_dataframe(csv_file: io.TextIOWrapper) -> pl.DataFrame:
    """Reads a CSV file-like object into a Polars DataFrame.

    Args:
        csv_file (io.TextIOWrapper): A file-like object for the CSV file.

    Returns:
        pl.DataFrame: The OHLCV data as a Polars DataFrame.
    """
    df = pl.read_csv(csv_file, has_header=False, schema=_SCHEMA)
    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ms")))
    return df.select(_COLUMNS)  # Keep OHLCV columns only


def _add_missing_timestamps(
    df: pl.DataFrame,
    timeframe: str,
    month: str,
) -> pl.DataFrame:
    """Adds any missing timestamps in the DataFrame.

    Args:
        df (pl.DataFrame): The input DataFrame.
        timeframe (str): The timeframe (e.g., "1m").
        month (str): The month in "YYYY-MM" format (e.g., "2024-12").

    Returns:
        pl.DataFrame: The DataFrame with filled timestamps.
    """
    start = datetime.strptime(month, "%Y-%m")
    end = start + relativedelta(months=1)

    timestamp_range = pl.datetime_range(
        start=start,
        end=end,
        interval=timeframe,
        time_unit="ms",
        closed="left",
        eager=True,
    )
    timestamps = pl.DataFrame({"timestamp": timestamp_range})

    return timestamps.join(df, on="timestamp", how="left")


def _add_symbol_column(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    """Adds a symbol column to the DataFrame.

    Args:
        df (pl.DataFrame): The input DataFrame.
        symbol (str): The trading pair symbol (e.g., "BTCUSDT").

    Returns:
        pl.DataFrame: The DataFrame with the "symbol" column.
    """
    df = df.with_columns(pl.lit(symbol).alias("symbol"))
    return df.select(["symbol"] + _COLUMNS)


def _get_cache_path(symbol: str, timeframe: str, month: str) -> str:
    """Generates a file path for the cache file.

    Args:
        symbol (str): The trading pair symbol (e.g., "BTCUSDT").
        timeframe (str): The timeframe (e.g., "1m").
        month (str): The month in "YYYY-MM" format (e.g., "2024-12").

    Returns:
        str: The cache file path.
    """
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, f"{symbol}-{timeframe}-{month}.parquet")


def _load_from_cache(cache_path: str) -> pl.DataFrame:
    """Loads a Polars DataFrame from a cache file.

    Args:
        cache_path (str): The path to the cache file.

    Returns:
        pl.DataFrame: The cached DataFrame.
    """
    return pl.read_parquet(cache_path)


def _save_to_cache(df: pl.DataFrame, cache_path: str) -> None:
    """Saves a Polars DataFrame to a cache file.

    Args:
        df (pl.DataFrame): The DataFrame to save.
        cache_path (str): The path to the cache file.
    """
    df.write_parquet(cache_path)


def _fetch_data_from_source(
    symbol: str,
    timeframe: str,
    month: str,
) -> pl.DataFrame:
    """Loads data from the source and returns it as a Polars DataFrame.

    Args:
        symbol (str): The trading pair symbol (e.g., "BTCUSDT").
        timeframe (str): The timeframe (e.g., "1m").
        month (str): The month in "YYYY-MM" format (e.g., "2024-12").

    Returns:
        pl.DataFrame: The OHLCV data as a Polars DataFrame.
    """
    url = _construct_url(symbol, timeframe, month)
    zip_content = _download_zip_file(url)
    csv_file = _extract_csv_from_zip(zip_content)
    df = _csv_to_dataframe(csv_file)
    df = _add_missing_timestamps(df, timeframe, month)
    df = _add_symbol_column(df, symbol)
    return df


def _fetch_monthly_candles(
    symbol: str,
    timeframe: str,
    month: str,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Returns a single month of candles as a Polars DataFrame.

    Args:
        symbol (str): The trading pair symbol (e.g., "BTCUSDT").
        timeframe (str): The timeframe (e.g., "1m").
        month (str): The month in "YYYY-MM" format (e.g., "2024-12").
        use_cache (bool): Whether to use cache. Defaults to True.

    Returns:
        pl.DataFrame: The OHLCV data as a Polars DataFrame.
    """
    cache_path = _get_cache_path(symbol, timeframe, month)

    # Load from cache if enabled and available
    if use_cache and os.path.exists(cache_path):
        return _load_from_cache(cache_path)

    # Fetch data from source
    df = _fetch_data_from_source(symbol, timeframe, month)

    # Save to cache if enabled
    if use_cache:
        _save_to_cache(df, cache_path)

    return df


def _get_months(start: str, end: Optional[str] = None) -> list[str]:
    """Returns a list of months between a start and end date.

    Args:
        start (str): Start month in "YYYY-MM" format (e.g., "2023-01").
        end (Optional[str]): End month in "YYYY-MM" format (e.g., "2023-12").
            If None, only the start month is returned.

    Returns:
        list[str]: A list of months in "YYYY-MM" format.
    """
    start_date = datetime.strptime(start, "%Y-%m")
    end_date = datetime.strptime(end, "%Y-%m") if end else start_date

    months = []
    current_date = start_date

    while current_date <= end_date:
        months.append(current_date.strftime("%Y-%m"))
        current_date += relativedelta(months=1)

    return months


def fetch(
    symbols: str | list[str],
    timeframe: str,
    start: str,
    end: Optional[str] = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Returns multiple months of candles as a Polars DataFrame.

    Args:
        symbol (str | list[str]): The trading symbols (e.g., "BTCUSDT").
        timeframe (str): The timeframe (e.g., "1m").
        start (str): Start month in "YYYY-MM" format (e.g., "2023-01").
        end (Optional[str]): End month in "YYYY-MM" format (e.g., "2023-12").
            If None, only the start month is returned.
        use_cache (bool): Whether to use cache. Defaults to True.

    Returns:
        pl.DataFrame: The OHLCV data as a Polars DataFrame.
    """
    if isinstance(symbols, str):
        symbols = [symbols]

    months = _get_months(start, end)
    candles = []

    for symbol in symbols:
        for month in months:
            candles.append(
                _fetch_monthly_candles(symbol, timeframe, month, use_cache)
            )

    return pl.concat(candles, how="vertical")


def clear_cache() -> None:
    """Clears the cache directory."""
    if os.path.exists(_CACHE_DIR):
        shutil.rmtree(_CACHE_DIR)
