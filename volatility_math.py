"""Compute percentage price range for a given block of klines."""

from __future__ import annotations


def calculate_price_range_percent(klines: list, block_size: int) -> float:
    """Return the high-low percentage movement of the most recent block."""
    import core  # pylint: disable=import-outside-toplevel

    try:
        k_id = id(klines)
        if k_id in core.SORTED_KLINES_CACHE:
            sorted_klines = core.SORTED_KLINES_CACHE[k_id]
        else:
            sorted_klines = sorted(klines, key=lambda k: int(k[0]))
            core.SORTED_KLINES_CACHE[k_id] = sorted_klines

        if len(sorted_klines) < block_size:
            return 0.0

        latest_block = sorted_klines[-block_size:]
        highs = [float(k[2]) for k in latest_block]
        lows = [float(k[3]) for k in latest_block]
        high = max(highs)
        low = min(lows)
        if low == 0:
            return 0.0
        return (high - low) / low * 100
    except (ValueError, IndexError, TypeError):
        return 0.0
