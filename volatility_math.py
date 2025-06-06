"""Compute percentage price range for a given block of klines."""

from __future__ import annotations

from typing import Iterable


def calculate_price_range_percent(klines: Iterable, minutes: int) -> float:
    """Return the high-low percentage movement of the latest ``minutes``."""
    try:
        sorted_klines = sorted(klines, key=lambda k: int(k[0]))

        if len(sorted_klines) < minutes:
            return 0.0

        subset = sorted_klines[-minutes:]
        highs = [float(k[2]) for k in subset]
        lows = [float(k[3]) for k in subset]
        high = max(highs)
        low = min(lows)
        if low == 0:
            return 0.0
        return (high - low) / low * 100
    except (ValueError, IndexError, TypeError):
        return 0.0
