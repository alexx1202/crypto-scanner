"""Compute percentage price range for a given block of klines."""

from __future__ import annotations

import pandas as pd


def calculate_price_range_percent(klines: list, minutes: int) -> float:
    """Return the high-low percentage movement of the latest ``minutes``."""
    import core  # pylint: disable=import-outside-toplevel

    try:
        k_id = id(klines)
        if k_id in core.SORTED_KLINES_CACHE:
            sorted_klines = core.SORTED_KLINES_CACHE[k_id]
        else:
            sorted_klines = sorted(klines, key=lambda k: int(k[0]))
            core.SORTED_KLINES_CACHE[k_id] = sorted_klines

        if len(sorted_klines) < minutes:
            return 0.0

        df = pd.DataFrame(sorted_klines, columns=[
            "timestamp", "open", "high", "low", "close", "volume"
        ])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("timestamp", inplace=True)
        subset = df[["high", "low"]].astype(float).iloc[-minutes:]
        high = subset["high"].max()
        low = subset["low"].min()
        if low == 0:
            return 0.0
        return (high - low) / low * 100
    except (ValueError, IndexError, TypeError, KeyError):
        return 0.0
