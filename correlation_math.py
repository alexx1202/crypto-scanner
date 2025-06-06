"""Functions for computing price correlation."""

from __future__ import annotations

import pandas as pd

import core


def calculate_price_correlation(
    symbol_klines: list,
    btc_klines: list,
    minutes: int,
) -> float:
    """Return the Pearson correlation of minute returns over ``minutes``."""
    try:
        sym_id = id(symbol_klines)
        btc_id = id(btc_klines)
        if sym_id in core.SORTED_KLINES_CACHE:
            s_sorted = core.SORTED_KLINES_CACHE[sym_id]
        else:
            s_sorted = sorted(symbol_klines, key=lambda k: int(k[0]))
            core.SORTED_KLINES_CACHE[sym_id] = s_sorted

        if btc_id in core.SORTED_KLINES_CACHE:
            b_sorted = core.SORTED_KLINES_CACHE[btc_id]
        else:
            b_sorted = sorted(btc_klines, key=lambda k: int(k[0]))
            core.SORTED_KLINES_CACHE[btc_id] = b_sorted

        if len(s_sorted) < minutes + 1 or len(b_sorted) < minutes + 1:
            return 0.0

        s_closes = [float(k[4]) for k in s_sorted[-(minutes + 1):]]
        b_closes = [float(k[4]) for k in b_sorted[-(minutes + 1):]]

        s_ret = [
            (s_closes[i + 1] - s_closes[i]) / s_closes[i]
            for i in range(minutes)
        ]
        b_ret = [
            (b_closes[i + 1] - b_closes[i]) / b_closes[i]
            for i in range(minutes)
        ]

        if len(set(s_ret)) <= 1 or len(set(b_ret)) <= 1:
            return 0.0

        return float(pd.Series(s_ret).corr(pd.Series(b_ret)))
    except (IndexError, ValueError, TypeError):
        return 0.0
