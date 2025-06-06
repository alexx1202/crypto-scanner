"""Core helpers for fetching Bybit market data and calculating indicators."""

import os
import time
import json
import random
import logging
import hashlib
from datetime import datetime, timezone

import requests
from tqdm import tqdm
from volume_math import calculate_volume_change
import correlation_math
import volatility_math

MAX_DUPLICATE_RETRIES = 3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def get_debug_logger() -> logging.Logger:
    """Return a shared debug logger writing to ``logs/scanlog.txt``."""
    if not hasattr(get_debug_logger, "cached_logger"):
        logger = logging.getLogger("debug_logger")
        logger.setLevel(logging.DEBUG)
        log_path = os.path.join(LOG_DIR, "scanlog.txt")
        handler = logging.FileHandler(log_path, mode="a")
        handler.setFormatter(
            logging.Formatter("[%(levelname)s] %(asctime)s %(message)s")
        )
        logger.addHandler(handler)
        get_debug_logger.cached_logger = logger
    return get_debug_logger.cached_logger


def get_auth_headers() -> dict:
    """Return request headers with API key or a generic user agent."""
    api_key = os.getenv("BYBIT_API_KEY")
    if api_key:
        return {"X-BYBIT-API-KEY": api_key}
    return {"User-Agent": "VolumeScannerBot/1.0"}


def get_tradeable_symbols_sorted_by_volume() -> list:
    """Return USDT symbols sorted by 24h turnover descending."""
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    try:
        with tqdm(total=1, desc="Fetching symbols") as pbar:
            response = requests.get(url, headers=get_auth_headers(), timeout=10)
            response.raise_for_status()
            pbar.update(1)
        tickers = response.json().get("result", {}).get("list", [])
        filtered = [
            (item["symbol"], float(item.get("turnover24h", 0)))
            for item in tickers
            if item.get("symbol", "").endswith("USDT")
        ]
        sorted_filtered = sorted(filtered, key=lambda x: x[1], reverse=True)
        return sorted_filtered
    except requests.RequestException as err:
        logging.getLogger("volume_logger").error(
            "Failed to fetch and sort symbols by volume: %s", err
        )
        return []


def stable_chunk_hash(chunk: list) -> str:
    """Return a deterministic MD5 hash for a kline chunk."""
    return hashlib.md5(
        json.dumps(chunk, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def build_kline_url(symbol: str, interval: str, start: int) -> str:
    """Construct the kline API URL for a symbol/interval starting time."""
    return (
        "https://api.bybit.com/v5/market/kline?category=linear"
        + f"&symbol={symbol}"
        + f"&interval={interval}"
        + f"&start={start}"
        + "&limit=1000"
    )


def get_kline_end_time() -> int:
    """Return the current UTC timestamp rounded down to the nearest 15m."""
    now = datetime.now(timezone.utc)
    floored = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    return int(floored.timestamp() * 1000)


def fetch_with_backoff(url: str, symbol: str, logger: logging.Logger) -> list:
    """Fetch a URL with basic retry/backoff handling for rate limits."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=get_auth_headers(), timeout=10)
            if response.status_code == 429:
                delay = round(random.uniform(1.0, 2.5), 2)
                logger.warning(
                    "[%s] Rate limit hit (429). Retrying in %.2fs...",
                    symbol,
                    delay
                )
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json().get("result", {}).get("list", [])
        except requests.RequestException as err:
            logger.warning(
                "[%s] Request error on attempt %d: %s",
                symbol,
                attempt + 1,
                err
            )
            time.sleep(1)
    logger.error("[%s] Failed all retries. Giving up.", symbol)
    return []


def fetch_recent_klines(
    symbol: str,
    interval: str = "1",
    total: int = 5040,
) -> list:
    """Return ``total`` klines for ``symbol`` using backoff retry logic."""
    logger = get_debug_logger()
    main_logger = logging.getLogger("volume_logger")

    seen_chunks = set()
    consecutive_duplicates = 0
    all_klines = []
    end_time = get_kline_end_time()

    try:
        while len(all_klines) < total:
            start_time = end_time - (1000 * 60 * 1000)
            url = build_kline_url(symbol, interval, start_time)
            chunk = fetch_with_backoff(url, symbol, logger)

            if not chunk:
                logger.debug("[%s] Empty chunk received. Ending fetch.", symbol)
                break

            chunk_key = stable_chunk_hash(chunk)
            if chunk_key in seen_chunks:
                consecutive_duplicates += 1
                logger.debug("[%s] Duplicate chunk #%d detected.", symbol, consecutive_duplicates)
                if consecutive_duplicates >= MAX_DUPLICATE_RETRIES:
                    logger.warning(
                        "%s: Max duplicate retries hit. Only %d klines gathered, expected %d.",
                        symbol,
                        len(all_klines),
                        total
                    )
                    break
                end_time = start_time
                continue

            seen_chunks.add(chunk_key)
            all_klines = chunk + all_klines
            logger.debug("[%s] Total klines so far: %d", symbol, len(all_klines))
            end_time = start_time
            consecutive_duplicates = 0
    except requests.RequestException as err:
        logger.error("[%s] Failed to fetch klines: %s", symbol, err)

    if len(all_klines) < 315:
        main_logger.warning("%s: Only %d klines returned, skipping.", symbol, len(all_klines))
        return []

    return all_klines[-total:]


def get_funding_rate(symbol: str) -> tuple[float, int]:
    """Return the latest funding rate and timestamp for ``symbol``."""
    url = (
        "https://api.bybit.com/v5/market/tickers"
        f"?category=linear&symbol={symbol}"
    )
    fetch_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        response = requests.get(url, headers=get_auth_headers(), timeout=10)
        response.raise_for_status()
        data = response.json()
        item = data.get("result", {}).get("list", [])[0]
        rate = float(item.get("fundingRate", 0))
        ts = int(data.get("time", fetch_time))
        return rate, ts
    except (IndexError, ValueError, KeyError, requests.RequestException):
        logging.getLogger("volume_logger").warning(
            "Failed to fetch funding rate for %s", symbol
        )
        return 0.0, 0


def get_open_interest_change(symbol: str, interval: str = "1h", limit: int = 24) -> float:
    """Return the open interest percentage change for ``symbol``."""
    url = (
        "https://api.bybit.com/v5/market/open-interest"
        f"?category=linear&symbol={symbol}&intervalTime={interval}&limit={limit}"
    )
    try:
        response = requests.get(url, headers=get_auth_headers(), timeout=10)
        response.raise_for_status()
        rows = response.json().get("result", {}).get("list", [])
        if len(rows) < 2:
            raise ValueError("insufficient data")
        rows_sorted = sorted(rows, key=lambda r: int(r.get("timestamp", 0)))
        first = float(rows_sorted[0].get("openInterest", 0))
        last = float(rows_sorted[-1].get("openInterest", 0))
        if first == 0:
            return 0.0
        return (last - first) / first * 100
    except (ValueError, KeyError, requests.RequestException):
        logging.getLogger("volume_logger").warning(
            "Failed to fetch open interest change for %s", symbol
        )
        return 0.0

def process_symbol(symbol: str, logger: logging.Logger) -> dict:
    """Return volume percentage change metrics for ``symbol``."""
    klines = fetch_recent_klines(symbol)
    if not klines:
        logger.warning("%s skipped: No valid klines returned.", symbol)
        return None
    return {
        "Symbol": symbol,
        "5M": round(calculate_volume_change(klines, 5), 4),
        "15M": round(calculate_volume_change(klines, 15), 4),
        "30M": round(calculate_volume_change(klines, 30), 4),
        "1H": round(calculate_volume_change(klines, 60), 4),
        "4H": round(calculate_volume_change(klines, 240), 4),
    }

def process_symbol_correlation(symbol: str, btc_klines: list, logger: logging.Logger) -> dict:
    """Return correlation metrics for ``symbol`` vs BTCUSDT."""
    klines = fetch_recent_klines(symbol)
    if not klines or not btc_klines:
        logger.warning("%s skipped: No valid klines returned for correlation.", symbol)
        return None
    return {
        "Symbol": symbol,
        "5M": round(correlation_math.calculate_price_correlation(klines, btc_klines, 5), 4),
        "15M": round(correlation_math.calculate_price_correlation(klines, btc_klines, 15), 4),
        "30M": round(correlation_math.calculate_price_correlation(klines, btc_klines, 30), 4),
        "1H": round(correlation_math.calculate_price_correlation(klines, btc_klines, 60), 4),
        "4H": round(correlation_math.calculate_price_correlation(klines, btc_klines, 240), 4),
    }


OPEN_INTEREST_INTERVALS = {
    "5M": "5min",
    "15M": "15min",
    "30M": "30min",
    "1H": "1h",
    "4H": "4h",
}


def get_open_interest_changes(symbol: str) -> dict:
    """Return open interest % change for multiple intervals."""
    result: dict[str, float] = {}
    for name, interval in OPEN_INTEREST_INTERVALS.items():
        result[name] = round(get_open_interest_change(symbol, interval, 2), 4)
    return result


def process_symbol_open_interest(symbol: str, _logger: logging.Logger) -> dict:
    """Return open interest change metrics for ``symbol``."""
    result = {"Symbol": symbol}
    result.update(get_open_interest_changes(symbol))
    return result


def process_symbol_funding(symbol: str, _logger: logging.Logger) -> dict:
    """Return the latest funding rate for ``symbol``."""
    rate, _ = get_funding_rate(symbol)
    return {"Symbol": symbol, "Funding Rate": rate}


def process_symbol_volatility(symbol: str, logger: logging.Logger) -> dict:
    """Return price range percentage movement metrics for ``symbol``."""
    klines = fetch_recent_klines(symbol)
    if not klines:
        logger.warning("%s skipped: No valid klines returned for volatility.", symbol)
        return None
    return {
        "Symbol": symbol,
        "5M": round(volatility_math.calculate_price_range_percent(klines, 5), 4),
        "15M": round(volatility_math.calculate_price_range_percent(klines, 15), 4),
        "30M": round(volatility_math.calculate_price_range_percent(klines, 30), 4),
        "1H": round(volatility_math.calculate_price_range_percent(klines, 60), 4),
        "4H": round(volatility_math.calculate_price_range_percent(klines, 240), 4),
    }
