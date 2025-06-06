"""Compute percentage price range for a given block of klines."""

from __future__ import annotations

        high = max(highs)
        low = min(lows)
        if low == 0:
            return 0.0
        return (high - low) / low * 100
    except (ValueError, IndexError, TypeError):
        return 0.0
