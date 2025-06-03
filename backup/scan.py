"""
scan.py - Volume Spike Scanner for Bybit USDT Perpetuals
Fetches 1m klines, computes 15m block volume change vs historical average,
and exports results to Excel with logging and cleanup.
"""

import os
import sys
import logging
from datetime import datetime, timezone
import pandas as pd
import requests
from tqdm import tqdm

# pylint: disable=broad-exception-caught

def get_tradeable_symbols_sorted_by_volume() -> list:
    """Fetch USDT trading pairs and sort them by 24h volume."""
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    try:
        with tqdm(total=1, desc="Fetching symbols") as pbar:
            response = requests.get(url, timeout=10)
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
    except Exception as e:
        print("[ERROR] Failed to fetch and sort symbols by volume: %s", e)
        return []

def fetch_recent_klines(symbol: str, interval: str = "1", count: int = 315) -> list:
    """Fetch recent klines for a symbol, aligned to 15-min TradingView boundaries."""
    now = datetime.now(timezone.utc)
    floored = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    end_time = int(floored.timestamp() * 1000)
    start_time = end_time - (count * 60 * 1000)

    url = (
        f"https://api.bybit.com/v5/market/kline?category=linear"
        f"&symbol={symbol}&interval={interval}&start={start_time}&limit={count}"
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        klines = response.json().get("result", {}).get("list", [])
        if len(klines) != 315:
            logger = logging.getLogger("volume_logger")
            logger.warning("%s: Only %d klines returned, skipping.", symbol, len(klines))
            return []
        return klines
    except Exception as e:
        print("[ERROR] Failed to fetch klines for %s: %s", symbol, e)
        return []

def calculate_volume_change(klines: list) -> float:
    """Compare the latest 15m block volume to the average of the last 20 non-overlapping blocks."""
    try:
        sorted_klines = sorted(klines, key=lambda k: int(k[0]))
        blocks = [
            sorted_klines[i:i + 15] for i in range(0, len(sorted_klines) - 14, 15)
            if len(sorted_klines[i:i + 15]) == 15
        ]
        logger = logging.getLogger("volume_logger")
        logger.debug("Valid 15-minute blocks found: %d", len(blocks))
        last_21_blocks = blocks[-21:]
        if len(last_21_blocks) < 21:
            logger.warning("Less than 21 full 15m blocks available, skipping.")
            return 0.0

        latest_block = last_21_blocks[-1]
        previous_blocks = last_21_blocks[:-1]

        sum_latest = sum(float(k[5]) for k in latest_block)
        avg_previous = sum(
            sum(float(k[5]) for k in block) for block in previous_blocks
        ) / len(previous_blocks)

        if avg_previous == 0:
            return 0.0

        pct = ((sum_latest - avg_previous) / avg_previous) * 100

        logger.debug("Total volume: %.4f contracts", sum_latest)
        logger.debug("Average of last 20 blocks: %.4f contracts", avg_previous)
        logger.debug("Percent change: %.4f%%", pct)

        return pct
    except Exception:
        return 0.0

def setup_logging() -> logging.Logger:
    """Initialize and configure logging to file and stdout."""
    logger = logging.getLogger("volume_logger")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter("[%(levelname)s] %(asctime)s %(message)s")

    fh = logging.FileHandler("scanlog.txt", mode="w")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger

def clean_existing_excels() -> None:
    """Delete all .xlsx files in the current directory."""
    for file in os.listdir():
        if file.endswith(".xlsx"):
            try:
                os.remove(file)
            except OSError as e:
                print("[WARNING] Failed to delete %s: %s", file, e)

def main() -> None:
    """Orchestrates fetching symbols, klines, and exporting volume % changes."""
    logger = setup_logging()
    try:
        logger.info("Fetching USDT perpetual futures from Bybit...")
        sorted_symbols = get_tradeable_symbols_sorted_by_volume()
        logger.info("Total pairs found: %d", len(sorted_symbols))

        if not sorted_symbols:
            logger.warning("No symbols retrieved. Skipping export.")
            return

        clean_existing_excels()

        logger.info("Fetching volume data and calculating percentage changes...")
        rows = []
        for symbol, _ in tqdm(sorted_symbols, desc="Scanning symbols"):
            klines = fetch_recent_klines(symbol)
            if not klines:
                continue
            pct_change = round(calculate_volume_change(klines), 4)
            rows.append({
                "Symbol": symbol,
                "Volume % Change": pct_change
            })
            logger.info("%s: %.4f%%", symbol, pct_change)

        df = pd.DataFrame(rows)

        logger.info("Exporting data to results.xlsx")
        with pd.ExcelWriter("results.xlsx", engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
            worksheet = writer.sheets["Sheet1"]
            worksheet.freeze_panes(1, 0)

        logger.info("Export complete: results.xlsx")

    except Exception as e:
        logger.exception("Script failed: %s", str(e))

if __name__ == "__main__":
    main()
