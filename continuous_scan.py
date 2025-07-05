"""Run Bybit scans at a fixed interval and save each result."""

import time
from datetime import datetime
import pandas as pd

import scan
import core


def run_periodic_scans() -> None:
    """Refresh each metric at its own interval."""
    logger = scan.setup_logging()
    logger.info("Continuous scan started. Press Ctrl+C to stop.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"Scan_{timestamp}.xlsx"

    intervals = {
        "volume": 9 * 60,
        "funding": 60,
        "oi": 60,
        "corr": 3 * 60,
        "price": 21 * 60,
    }
    next_run = {key: 0 for key in intervals}

    volume_df = pd.DataFrame()
    funding_df = pd.DataFrame()
    oi_df = pd.DataFrame()
    price_df = pd.DataFrame()
    matrix_map: dict[str, pd.DataFrame] = {}
    symbol_order: list[str] = []

    while True:
        now = time.time()
        try:
            if (
                now >= next_run["volume"]
                or now >= next_run["funding"]
                or now >= next_run["oi"]
            ):
                logger.info("Fetching USDT perpetual futures from Bybit...")
                all_symbols = core.get_tradeable_symbols_sorted_by_volume()
                logger.info("Total pairs found: %d", len(all_symbols))

                if not all_symbols:
                    logger.warning("No symbols retrieved. Skipping export.")
                else:
                    volume_df, funding_df, oi_df, symbol_order = scan.run_scan(all_symbols, logger)
                    if not price_df.empty:
                        scan.export_all_data(
                            volume_df,
                            funding_df,
                            oi_df,
                            price_df,
                            symbol_order,
                            logger,
                            filename=filename,
                        )
                        scan.send_push_notification(
                            "Scan complete",
                            f"{filename} has been exported.",
                            logger,
                        )
                    if now >= next_run["volume"]:
                        next_run["volume"] = now + intervals["volume"]
                    if now >= next_run["funding"]:
                        next_run["funding"] = now + intervals["funding"]
                    if now >= next_run["oi"]:
                        next_run["oi"] = now + intervals["oi"]

            if now >= next_run["corr"]:
                logger.info("Updating correlation matrix")
                all_symbols = core.get_tradeable_symbols_sorted_by_volume()
                matrix_map = scan.run_correlation_matrix_scan(all_symbols, logger)
                scan.export_correlation_matrices(matrix_map, logger)
                scan.send_push_notification(
                    "Correlation matrix complete",
                    "Correlation_Matrix.xlsx has been exported.",
                    logger,
                )
                next_run["corr"] = now + intervals["corr"]

            if now >= next_run["price"]:
                logger.info("Updating price change data")
                all_symbols = core.get_tradeable_symbols_sorted_by_volume()
                price_df = scan.run_price_change_scan(all_symbols, logger)
                scan.export_all_data(
                    volume_df,
                    funding_df,
                    oi_df,
                    price_df,
                    symbol_order,
                    logger,
                    filename=filename,
                )
                next_run["price"] = now + intervals["price"]

        except (RuntimeError, ValueError, TypeError) as exc:
            logger.exception("Script failed: %s", exc)

        time.sleep(60)


if __name__ == "__main__":
    run_periodic_scans()
