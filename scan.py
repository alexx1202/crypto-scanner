"""
Orchestration for Bybit USDT Perpetual Volume Scanner.
Handles logging, threading, Excel export, scan loop.
"""

import os
import sys
import logging
import smtplib
from email.message import EmailMessage
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm
import core

def wait_for_file_close(filename: str, logger: logging.Logger) -> None:
    """Pause execution until the given file can be opened for writing."""
    if not os.path.exists(filename):
        return
    while True:
        try:
            with open(filename, "a", encoding="utf-8"):
                break
        except OSError:
            logger.warning(
                "%s is still open. Please close it before continuing.", filename
            )
            input("Press Enter after closing the file...")

def setup_logging() -> logging.Logger:
    """Configure and return a logger writing to file and stdout."""
    logger = logging.getLogger("volume_logger")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("[%(levelname)s] %(asctime)s %(message)s")

    os.makedirs(core.LOG_DIR, exist_ok=True)
    log_path = os.path.join(core.LOG_DIR, "scanlog.txt")
    fh = logging.FileHandler(log_path, mode="w")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger

def clean_existing_excels(logger: logging.Logger | None = None) -> None:
    """Delete any existing .xlsx files in the working directory."""
    if logger is None:
        logger = logging.getLogger("volume_logger")
    for file in os.listdir():
        if file.endswith(".xlsx"):
            wait_for_file_close(file, logger)
            try:
                os.remove(file)
            except OSError:
                logger.warning("Failed to delete %s", file)

def send_email_alert(subject: str, body: str, logger: logging.Logger) -> None:
    """Send an email alert using SMTP credentials from env vars."""
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "0"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    to_addr = os.getenv("EMAIL_TO")
    from_addr = os.getenv("EMAIL_FROM", user or "")

    if not all([host, port, user, password, to_addr]):
        logger.info("Email not configured. Skipping email alert.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port) as smtp:
            smtp.starttls()
            smtp.login(user, password)
            smtp.send_message(msg)
        logger.info("Email alert sent to %s", to_addr)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Failed to send email alert: %s", exc)

def export_to_excel(df: pd.DataFrame, symbol_order: list, logger: logging.Logger) -> None:
    """Export results to Excel with formatting."""  # pylint: disable=too-many-locals
    df["__sort_order"] = df["Symbol"].map({s: i for i, s in enumerate(symbol_order)})
    df = df.sort_values("__sort_order").drop(columns=["__sort_order"])

    logger.info("Exporting data to Excel: Crypto_Volume.xlsx")
    wait_for_file_close("Crypto_Volume.xlsx", logger)
    with pd.ExcelWriter("Crypto_Volume.xlsx", engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1", startrow=1)
        worksheet = writer.sheets["Sheet1"]

        header_format = writer.book.add_format({"bold": True})
        worksheet.merge_range(
            "B1:H1",
            "% Distance Below or Above 20 Bar Moving Average Volume Indicator",
            header_format,
        )
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

        for col in range(1, 6):
            worksheet.set_column(col, col, None, percent_format)

        if "Funding Rate" in df.columns:
            idx = df.columns.get_loc("Funding Rate")
            worksheet.set_column(idx, idx, None, funding_format)

        for col in range(1, 7):
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

def submit_symbol_futures(symbols, executor, logger):
    """Submit symbol processing tasks and return the future map."""
    return {
        executor.submit(core.process_symbol, symbol, logger): symbol
        for symbol in symbols
    }

def scan_and_collect_results(symbols: list, logger: logging.Logger) -> tuple:
    """Scan all symbols and return results and failures."""
    rows = []
    failed = []

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = submit_symbol_futures(symbols, executor, logger)
        for future in tqdm(as_completed(futures), total=len(futures), desc="Scanning"):
            symbol = futures[future]
            result = future.result()
            if result:
                rows.append(result)
            else:
                failed.append(symbol)

    return rows, failed

def run_scan(logger: logging.Logger) -> None:
    """Fetch symbol list, scan each, and export Excel results."""
    logger.info("Fetching USDT perpetual futures from Bybit...")
    all_symbols = core.get_tradeable_symbols_sorted_by_volume()
    logger.info("Total pairs found: %d", len(all_symbols))

    if not all_symbols:
        logger.warning("No symbols retrieved. Skipping export.")
        return

    clean_existing_excels(logger)
    logger.info("Scanning symbols in parallel...")

    rows, failed = scan_and_collect_results([s for s, _ in all_symbols], logger)

    volume_map = dict(all_symbols)
    for row in rows:
        row["24h USD Volume"] = volume_map.get(row["Symbol"], 0)

    if failed:
        logger.warning("%d symbols failed: %s", len(failed), ", ".join(failed))

    export_to_excel(pd.DataFrame(rows), [s for s, _ in all_symbols], logger)
    logger.info("Export complete: Crypto_Volume.xlsx")
    send_email_alert(
        "Volume scan complete",
        "Crypto_Volume.xlsx has been exported.",
        logger,
    )

def main() -> None:
    """Main entry point."""
    logger = setup_logging()
    try:
        run_scan(logger)
    except (RuntimeError, ValueError, TypeError) as e:
        logger.exception("Script failed: %s", str(e))

if __name__ == "__main__":
    main()
