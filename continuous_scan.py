"""Run Bybit scans at a fixed interval and save each result."""

import time
from datetime import datetime

import scan
import core


def run_periodic_scans(interval_minutes: int = 30) -> None:
    """Run a volume scan every ``interval_minutes`` minutes."""
    logger = scan.setup_logging()
    logger.info("Continuous scan started. Press Ctrl+C to stop.")
    while True:
        logger.info("Starting periodic scan")
        try:
            logger.info("Fetching USDT perpetual futures from Bybit...")
            all_symbols = core.get_tradeable_symbols_sorted_by_volume()
            logger.info("Total pairs found: %d", len(all_symbols))

            if not all_symbols:
                logger.warning("No symbols retrieved. Skipping export.")
                logger.info(
                    "Waiting %d minutes for next scan...", interval_minutes
                )
                time.sleep(interval_minutes * 60)
                continue

            volume_df, funding_df, oi_df, symbol_order = scan.run_scan(all_symbols, logger)
            corr_df = scan.run_correlation_scan(all_symbols, logger)
            vol_df = scan.run_volatility_scan(all_symbols, logger)
            price_df = scan.run_price_change_scan(all_symbols, logger)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"Scan_{timestamp}.xlsx"
            scan.export_all_data(
                volume_df,
                funding_df,
                oi_df,
                corr_df,
                vol_df,
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
        except (RuntimeError, ValueError, TypeError) as exc:
            logger.exception("Script failed: %s", exc)
        logger.info("Waiting %d minutes for next scan...", interval_minutes)
        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    run_periodic_scans()
