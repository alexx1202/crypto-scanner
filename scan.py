"""Utilities for running a Bybit volume scan and exporting results."""

import os
import logging
import platform
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib

import pandas as pd
from tqdm import tqdm

import core
from scan_utils import wait_for_file_close


def get_toast_notifier():
    """Return the ``ToastNotifier`` class if ``win10toast`` is available."""
    try:  # pragma: no cover - optional dependency
        module = importlib.import_module("win10toast")
        return module.ToastNotifier
    except (ImportError, AttributeError):
        return None


def setup_logging() -> logging.Logger:
    """Configure and return the main scanner logger."""
    logger = logging.getLogger("volume_logger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler("logs/scanlog.txt")
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    return logger


def clean_existing_excels(logger: logging.Logger | None = None) -> None:
    """Delete existing Excel files in the working directory."""
    if logger is None:
        logger = logging.getLogger("volume_logger")
    for file in os.listdir():
        if file.endswith(".xlsx"):
            wait_for_file_close(file, logger)
            try:
                os.remove(file)
            except OSError:
                logger.warning("Failed to delete %s", file)


def send_push_notification(title: str, message: str, logger: logging.Logger) -> None:
    """Show a Windows toast notification if supported."""
    if platform.system() != "Windows":
        logger.info("Windows notifications not supported on this OS. Skipping.")
        return

    notifier_class = get_toast_notifier()
    if notifier_class is None:
        logger.info("win10toast not installed. Skipping notification.")
        return

    try:
        notifier = notifier_class()
        if hasattr(notifier, "on_destroy"):
            original = notifier.on_destroy

            def _on_destroy(hwnd, msg, wparam, lparam) -> int:
                original(hwnd, msg, wparam, lparam)
                return 0

            notifier.on_destroy = _on_destroy

        notifier.show_toast(title, message, duration=5)
        logger.info("Windows notification sent")
    except (OSError, TypeError) as exc:  # pragma: no cover - platform specific error
        logger.warning("Failed to send notification: %s", exc)


def export_to_excel(
    df: pd.DataFrame,
    symbol_order: list,
    logger: logging.Logger,
    filename: str = "Crypto_Volume.xlsx",
    header: str = "% Distance Below or Above 20 Bar Moving Average Volume Indicator",
    *,
    apply_conditional_formatting: bool = True,
) -> None:
    # pylint: disable=too-many-locals,too-many-arguments
    """Write ``df`` to ``filename`` with formatting."""
    df["__sort_order"] = df["Symbol"].map({s: i for i, s in enumerate(symbol_order)})
    df = df.sort_values("__sort_order").drop(columns=["__sort_order"])

    if "Funding Rate" in df.columns and "24h USD Volume" in df.columns:
        cols = df.columns.tolist()
        fr_idx = cols.index("Funding Rate")
        vol_idx = cols.index("24h USD Volume")
        if fr_idx < vol_idx:
            cols[fr_idx], cols[vol_idx] = cols[vol_idx], cols[fr_idx]
            df = df[cols]

    logger.info("Exporting data to Excel: %s", filename)
    wait_for_file_close(filename, logger)
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1", startrow=1)
        worksheet = writer.sheets["Sheet1"]
        header_format = writer.book.add_format({"bold": True})
        span = "B1:I1" if "Open Interest Change" in df.columns else "B1:H1"
        worksheet.merge_range(span, header, header_format)
        worksheet.freeze_panes(2, 0)

        red_format = writer.book.add_format({
            "bg_color": "#FFC7CE",
            "font_color": "#9C0006",
        })
        green_format = writer.book.add_format({
            "bg_color": "#C6EFCE",
            "font_color": "#006100",
        })
        currency_format = writer.book.add_format({"num_format": "$#,##0.00"})
        percent_format = writer.book.add_format({"num_format": '0.00"%"'})
        funding_format = writer.book.add_format({"num_format": '0.0000000%'})

        if "24h USD Volume" in df.columns:
            col_idx = df.columns.get_loc("24h USD Volume")
            worksheet.set_column(col_idx, col_idx, None, currency_format)

        percent_columns = [
            name for name in ["5M", "15M", "30M", "1H", "4H", "Open Interest Change"]
            if name in df.columns
        ]
        for name in percent_columns:
            col = df.columns.get_loc(name)
            worksheet.set_column(col, col, None, percent_format)

        if "Funding Rate" in df.columns:
            idx = df.columns.get_loc("Funding Rate")
            worksheet.set_column(idx, idx, None, funding_format)

        if apply_conditional_formatting:
            columns_to_format = [
                name for name in [
                    "5M",
                    "15M",
                    "30M",
                    "1H",
                    "4H",
                    "Open Interest Change",
                    "Funding Rate",
                ] if name in df.columns
            ]
            for name in columns_to_format:
                col = df.columns.get_loc(name)
                col_letter = chr(ord("A") + col)
                cell_range = f"{col_letter}3:{col_letter}1048576"
                worksheet.conditional_format(cell_range, {
                    "type": "cell",
                    "criteria": ">",
                    "value": 0,
                    "format": green_format
                })
                worksheet.conditional_format(cell_range, {
                    "type": "cell",
                    "criteria": "<",
                    "value": 0,
                    "format": red_format
                })


def submit_symbol_futures(symbols: list[str], executor: ThreadPoolExecutor,
                           logger: logging.Logger, func) -> dict:
    """Return a mapping of futures to their corresponding symbol."""
    return {
        executor.submit(func, symbol, logger): symbol
        for symbol in symbols
    }


def scan_and_collect_results(symbols: list[str],
                             logger: logging.Logger,
                             func=core.process_symbol) -> tuple[list, list]:
    """Process all symbols concurrently and collect successes and failures."""
    rows: list[dict] = []
    failed: list[str] = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = submit_symbol_futures(symbols, executor, logger, func)
        for future in tqdm(as_completed(futures), total=len(futures),
                           desc="Scanning"):
            symbol = futures[future]
            result = future.result()
            if result:
                rows.append(result)
            else:
                failed.append(symbol)
    return rows, failed


def run_scan(logger: logging.Logger) -> None:
    """Fetch symbols, run analysis, and export results to Excel."""
    logger.info("Fetching USDT perpetual futures from Bybit...")
    all_symbols = core.get_tradeable_symbols_sorted_by_volume()
    logger.info("Total pairs found: %d", len(all_symbols))

    if not all_symbols:
        logger.warning("No symbols retrieved. Skipping export.")
        return

    clean_existing_excels(logger)
    logger.info("Scanning volume metrics...")

    volume_rows, failed = scan_and_collect_results(
        [s for s, _ in all_symbols],
        logger,
        core.process_symbol,
    )

    volume_map = dict(all_symbols)
    for row in volume_rows:
        row["24h USD Volume"] = volume_map.get(row["Symbol"], 0)

    logger.info("Scanning funding rates...")
    funding_rows, _ = scan_and_collect_results(
        [s for s, _ in all_symbols],
        logger,
        core.process_symbol_funding,
    )
    for row in funding_rows:
        row["24h USD Volume"] = volume_map.get(row["Symbol"], 0)

    logger.info("Scanning open interest changes...")
    oi_rows, _ = scan_and_collect_results(
        [s for s, _ in all_symbols],
        logger,
        core.process_symbol_open_interest,
    )
    for row in oi_rows:
        row["24h USD Volume"] = volume_map.get(row["Symbol"], 0)

    if failed:
        logger.warning("%d symbols failed: %s", len(failed), ", ".join(failed))

    export_to_excel(pd.DataFrame(volume_rows), [s for s, _ in all_symbols], logger)
    logger.info("Export complete: Crypto_Volume.xlsx")

    export_to_excel(
        pd.DataFrame(funding_rows),
        [s for s, _ in all_symbols],
        logger,
        filename="Funding_Rates.xlsx",
        header="Latest Funding Rates",
    )
    logger.info("Export complete: Funding_Rates.xlsx")

    export_to_excel(
        pd.DataFrame(oi_rows),
        [s for s, _ in all_symbols],
        logger,
        filename="Open_Interest.xlsx",
        header="% Change in Open Interest",
    )
    logger.info("Export complete: Open_Interest.xlsx")

    send_push_notification(
        "Volume scan complete",
        "Crypto_Volume.xlsx has been exported.",
        logger,
    )


def run_correlation_scan(logger: logging.Logger) -> None:
    """Compute correlation of each symbol to BTCUSDT and export."""
    logger.info("Starting correlation scan...")
    all_symbols = core.get_tradeable_symbols_sorted_by_volume()
    if not all_symbols:
        logger.warning("No symbols retrieved. Skipping correlation export.")
        return

    btc_klines = core.fetch_recent_klines("BTCUSDT")
    rows: list[dict] = []
    failed: list[str] = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {
            executor.submit(
                core.process_symbol_correlation, symbol, btc_klines, logger
            ): symbol
            for symbol, _ in all_symbols
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Correlation"):
            symbol = futures[future]
            result = future.result()
            if result:
                rows.append(result)
            else:
                failed.append(symbol)

    if failed:
        logger.warning("%d symbols failed: %s", len(failed), ", ".join(failed))

    df = pd.DataFrame(rows)
    for col in ["5M", "15M", "30M", "1H", "4H"]:
        if col in df.columns:
            df[col] = df[col] * 100

    export_to_excel(
        df,
        [s for s, _ in all_symbols],
        logger,
        filename="Crypto_Correlation.xlsx",
        header="% Correlation of Each Symbol to BTC",
    )
    logger.info("Export complete: Crypto_Correlation.xlsx")
    send_push_notification(
        "Correlation scan complete",
        "Crypto_Correlation.xlsx has been exported.",
        logger,
    )


def run_volatility_scan(logger: logging.Logger) -> None:
    """Compute high-low price movement for each symbol and export."""
    logger.info("Starting volatility scan...")
    all_symbols = core.get_tradeable_symbols_sorted_by_volume()
    if not all_symbols:
        logger.warning("No symbols retrieved. Skipping volatility export.")
        return

    rows, failed = scan_and_collect_results(
        [s for s, _ in all_symbols],
        logger,
        core.process_symbol_volatility,
    )

    if failed:
        logger.warning("%d symbols failed: %s", len(failed), ", ".join(failed))

    export_to_excel(
        pd.DataFrame(rows),
        [s for s, _ in all_symbols],
        logger,
        filename="Price_Movement.xlsx",
        header="% Price Movement",
        apply_conditional_formatting=False,
    )
    logger.info("Export complete: Price_Movement.xlsx")
    send_push_notification(
        "Volatility scan complete",
        "Price_Movement.xlsx has been exported.",
        logger,
    )


def main() -> None:
    """Entry point for running the scanner from the command line."""
    logger = setup_logging()
    try:
        run_scan(logger)
        run_correlation_scan(logger)
        run_volatility_scan(logger)
    except (RuntimeError, ValueError, TypeError) as exc:
        logger.exception("Script failed: %s", exc)


if __name__ == "__main__":
    main()
