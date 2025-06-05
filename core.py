"""
Core logic for volume anomaly detection.
Handles symbol fetch, kline retrieval, deduplication, and volume math.
"""

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

KLINE_CACHE = {}
SORTED_KLINES_CACHE = {}

def get_debug_logger() -> logging.Logger:
    """Return a memoized logger for debugging kline fetches."""
    if not hasattr(get_debug_logger, "cached_logger"):
        logger = logging.getLogger("debug_logger")
        logger.setLevel(logging.DEBUG)
        home = os.path.expanduser("~")
        log_path = os.path.join(
            home,
            "OneDrive",
            "Documents",
            "CRYPTO",
            "PYTHON",
            "WORK_IN_PROGRESS",
            "cryptoscanner",
            "logs",
            "klines_debug.log",
        )
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        handler = logging.FileHandler(log_path, mode="w")
        handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
        logger.addHandler(handler)
        get_debug_logger.cached_logger = logger
    return get_debug_logger.cached_logger

def get_auth_headers() -> dict:
    """Return API headers using BYBIT_API_KEY from environment."""
    api_key = os.getenv("BYBIT_API_KEY")
    if api_key:
        return {"X-BYBIT-API-KEY": api_key}
    return {}

def get_tradeable_symbols_sorted_by_volume() -> list:
    """Fetch and sort USDT perpetual symbols by 24h turnover descending."""
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
        print("[ERROR] Failed to fetch and sort symbols by volume: %s", err)
        return []

def stable_chunk_hash(chunk: list) -> str:
    """Generate a stable hash for a chunk of klines regardless of list identity."""
    return hashlib.md5(
        json.dumps(chunk, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

def build_kline_url(symbol: str, interval: str, start: int) -> str:
    """Construct and return a Bybit kline API URL for the given symbol and time window."""
    return (
        "https://api.bybit.com/v5/market/kline?category=linear"
        + f"&symbol={symbol}"
        + f"&interval={interval}"
        + f"&start={start}"
        + "&limit=1000"
    )

def get_kline_end_time() -> int:
    """Return the UTC timestamp of the most recent rounded 15m block."""
    now = datetime.now(timezone.utc)
    floored = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    return int(floored.timestamp() * 1000)

def fetch_with_backoff(url: str, symbol: str, logger: logging.Logger) -> list:
    """Send a GET request with backoff on HTTP 429 (rate limit)."""
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

def fetch_recent_klines(symbol: str, interval: str = "1", total: int = 5040) -> list:
    """Fetch latest klines for a given symbol, deduplicating chunks to avoid stale loops."""
    logger = get_debug_logger()
    main_logger = logging.getLogger("volume_logger")

    if symbol in KLINE_CACHE:
        return KLINE_CACHE[symbol][-total:]

    seen_chunks = set()
    consecutive_duplicates = 0
    max_consecutive_duplicates = 3
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
                if consecutive_duplicates >= max_consecutive_duplicates:
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

    KLINE_CACHE[symbol] = all_klines
    return all_klines[-total:]

def process_symbol(symbol: str, logger: logging.Logger) -> dict:
    """Fetch klines and compute volume changes for 5m, 15m, 30m, 1h, and 4h blocks."""
    klines = fetch_recent_klines(symbol)
    if not klines:
        logger.warning("%s skipped: No valid klines returned.", symbol)
        return None
    return {
        "Symbol": symbol,
        "5m Volume % Change": round(calculate_volume_change(klines, 5), 4),
        "15m Volume % Change": round(calculate_volume_change(klines, 15), 4),
        "30m Volume % Change": round(calculate_volume_change(klines, 30), 4),
        "1h Volume % Change": round(calculate_volume_change(klines, 60), 4),
        "4h Volume % Change": round(calculate_volume_change(klines, 240), 4),
    }
