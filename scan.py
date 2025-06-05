import os
import sys
import logging
import hashlib
from datetime import datetime, timezone
import pandas as pd
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# pylint: disable=broad-exception-caught

_sorted_klines_cache = {}

def get_auth_headers() -> dict:
    api_key = os.getenv("BYBIT_API_KEY")
    if api_key:
        return {"X-BYBIT-API-KEY": api_key}
    return {}

def get_tradeable_symbols_sorted_by_volume() -> list:
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
    except Exception as e:
        print("[ERROR] Failed to fetch and sort symbols by volume: %s", e)
        return []

def fetch_recent_klines(symbol: str, interval: str = "1", total: int = 30240) -> list:

    klines = []
    seen_chunks = set()
    now = datetime.now(timezone.utc)
    floored = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    end_time = int(floored.timestamp() * 1000)
    count = 1000

    while len(klines) < total:
        start_time = end_time - (count * 60 * 1000)
        url = (
            f"https://api.bybit.com/v5/market/kline?category=linear"
            f"&symbol={symbol}&interval={interval}&start={start_time}&limit={count}"
        )
        try:
            response = requests.get(url, headers=get_auth_headers(), timeout=10)
            response.raise_for_status()
            chunk = response.json().get("result", {}).get("list", [])
            if not chunk:
                break

            chunk_key = hashlib.md5(str([row[0] for row in chunk]).encode()).hexdigest()
            if chunk_key in seen_chunks:
                print("[DEBUG] Duplicate chunk detected, breaking early.")
                return []
            seen_chunks.add(chunk_key)
            klines = chunk + klines

            end_time = start_time
        except Exception as e:
            print("[ERROR] Failed to fetch klines for %s: %s", symbol, e)
            break

    if len(klines) < 315:
        logger = logging.getLogger("volume_logger")
        logger.warning("%s: Only %d klines returned, skipping.", symbol, len(klines))
        return []

    return klines[-total:]

def calculate_volume_change(klines: list, block_size: int) -> float:
    try:
        cache_key = id(klines)
        if cache_key not in _sorted_klines_cache:
            _sorted_klines_cache[cache_key] = sorted(klines, key=lambda k: int(k[0]))
        sorted_klines = _sorted_klines_cache[cache_key]

        blocks = [
            sorted_klines[i:i + block_size]
            for i in range(0, len(sorted_klines) - (block_size - 1), block_size)
            if len(sorted_klines[i:i + block_size]) == block_size
        ]
        if len(blocks) < 21:
            return 0.0

        latest_block = blocks[-1]
        previous_blocks = blocks[-21:-1]

        sum_latest = sum(float(k[5]) for k in latest_block)
        avg_previous = sum(
            sum(float(k[5]) for k in block) for block in previous_blocks
        ) / len(previous_blocks)

        if avg_previous == 0:
            return 0.0

        return ((sum_latest - avg_previous) / avg_previous) * 100
    except Exception:
        return 0.0

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("volume_logger")
    logger.setLevel(logging.INFO)
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
    for file in os.listdir():
        if file.endswith(".xlsx"):
            try:
                os.remove(file)
            except OSError as e:
                print("[WARNING] Failed to delete %s: %s", file, e)

def process_symbol(symbol: str) -> dict:
    klines = fetch_recent_klines(symbol)
    if not klines:
        return None
    return {
        "Symbol": symbol,
        "15m Volume % Change": round(calculate_volume_change(klines, 15), 4),
        "1h Volume % Change": round(calculate_volume_change(klines, 60), 4),
        "1d Volume % Change": round(calculate_volume_change(klines, 1440), 4),
    }

def main() -> None:
    logger = setup_logging()
    try:
        logger.info("Fetching USDT perpetual futures from Bybit...")
        sorted_symbols = get_tradeable_symbols_sorted_by_volume()[:100]
        logger.info("Total pairs found: %d", len(sorted_symbols))

        if not sorted_symbols:
            logger.warning("No symbols retrieved. Skipping export.")
            return

        clean_existing_excels()

        logger.info("Scanning symbols in parallel...")
        rows = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(process_symbol, symbol): symbol for symbol, _ in sorted_symbols}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Scanning"):
                result = future.result()
                if result:
                    rows.append(result)

        if not rows:
            logger.warning("No valid data collected. Skipping export.")
            return

        symbol_order = [symbol for symbol, _ in sorted_symbols]
        df = pd.DataFrame(rows)
        df["__sort_order"] = df["Symbol"].map({s: i for i, s in enumerate(symbol_order)})
        df = df.sort_values("__sort_order").drop(columns=["__sort_order"])

        logger.info("Exporting data to results.xlsx")
        with pd.ExcelWriter("results.xlsx", engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
            worksheet = writer.sheets["Sheet1"]
            worksheet.freeze_panes(1, 0)

            red_format = writer.book.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
            green_format = writer.book.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})
            for col in range(1, 4):
                col_letter = chr(ord('A') + col)
                worksheet.conditional_format(f"{col_letter}2:{col_letter}1048576", {
                    "type": "cell",
                    "criteria": ">",
                    "value": 0,
                    "format": green_format
                })
                worksheet.conditional_format(f"{col_letter}2:{col_letter}1048576", {
                    "type": "cell",
                    "criteria": "<",
                    "value": 0,
                    "format": red_format
                })

        logger.info("Export complete: results.xlsx")

    except Exception as e:
        logger.exception("Script failed: %s", str(e))

if __name__ == "__main__":
    main()
