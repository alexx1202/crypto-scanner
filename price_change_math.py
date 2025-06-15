"""Compute percentage price change for a block of klines."""

from __future__ import annotations


def calculate_price_change_percent(klines: list, block_size: int) -> float:
    """Return the close-to-close percentage change over ``block_size`` minutes."""
    try:
        sorted_klines = sorted(klines, key=lambda k: int(k[0]))
        if len(sorted_klines) < block_size + 1:
            return 0.0
        subset = sorted_klines[-(block_size + 1):]
        start = float(subset[0][4])
        end = float(subset[-1][4])
        if start == 0:
            return 0.0
        return (end - start) / start * 100
    except (ValueError, IndexError, TypeError):
        return 0.0
