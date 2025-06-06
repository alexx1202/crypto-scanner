"""
Unit test suite for Bybit volume anomaly scanner.
Tests include symbol sorting, kline deduplication, volume spike/dip detection,
and Excel export behavior. Supports pytest + pylint 10/10 compliance.
"""

import logging
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
import pandas as pd
import core
import scan
from volume_math import calculate_volume_change
import correlation_math

def test_get_tradeable_symbols_sorted_by_volume():
    """Test symbol sorting by 24h volume descending order."""
    mock_response = {
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "turnover24h": "500000000"},
                {"symbol": "ETHUSDT", "turnover24h": "300000000"},
                {"symbol": "DOGEUSDT", "turnover24h": "100000000"}
            ]
        }
    }
    with patch("core.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        result = core.get_tradeable_symbols_sorted_by_volume()
        assert result == [
            ("BTCUSDT", 500000000.0),
            ("ETHUSDT", 300000000.0),
            ("DOGEUSDT", 100000000.0)
        ]

def test_fetch_recent_klines_exact_count():
    """Test fetch returns the exact number of klines requested."""
    mock_klines = [[str(1717382400000 + i * 60000), "", "", "", "", "1"] for i in range(5040)]
    mock_response = {"result": {"list": mock_klines}}
    with patch("core.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        result = core.fetch_recent_klines("BTCUSDT", total=5040)
        assert isinstance(result, list)
        assert len(result) == 5040

def test_fetch_recent_klines_insufficient():
    """Test deduplication logic by simulating repeated stale API responses."""
    def build_chunk():
        return [[str(1717382400000), "", "", "", "", "1"] for _ in range(100)]

    mock_response = {"result": {"list": build_chunk()}}
    responses = [mock_response, mock_response, mock_response]

    def side_effect(*_, **__):
        if responses:
            return MagicMock(status_code=200, json=lambda: responses.pop(0))
        return MagicMock(status_code=200, json=lambda: {"result": {"list": []}})

    with patch("core.requests.get", side_effect=side_effect):
        core.KLINE_CACHE.clear()
        result = core.fetch_recent_klines("BTCUSDT", total=5040)
        assert result == []

def test_clean_existing_excels(tmp_path):
    """Test Excel file cleanup removes files as expected."""
    dummy_file = tmp_path / "file.xlsx"
    dummy_file.write_text("data")

    with patch("scan.os.listdir", return_value=["file.xlsx"]), \
         patch("scan.os.remove") as mock_remove, \
         patch("scan.wait_for_file_close") as mock_wait:
        scan.clean_existing_excels()
        mock_wait.assert_called_once()
        mock_remove.assert_called_once_with("file.xlsx")

def test_setup_logging():
    """Test logger is created with correct config."""
    logger = scan.setup_logging()
    assert isinstance(logger, logging.Logger)
    assert logger.name == "volume_logger"
    assert logger.level == logging.INFO

def test_process_symbol_with_mocked_logger():
    """Ensure process_symbol runs with valid klines and mocked logger."""
    mock_klines = [[str(i), "", "", "", "", "2"] for i in range(5040)]
    with patch("core.fetch_recent_klines", return_value=mock_klines), \
         patch("core.get_open_interest_change", return_value=5.0):
        result = core.process_symbol("BTCUSDT", MagicMock())
        assert isinstance(result, dict)
        assert result["Symbol"] == "BTCUSDT"
        assert "5M" in result
        assert "15M" in result
        assert "30M" in result
        assert "1H" in result
        assert "4H" in result
        assert "Funding Rate" in result
        assert "Open Interest Change" in result
        assert "Funding Rate Timestamp" not in result


def test_calculate_price_correlation_perfect():
    """Correlation should be 1.0 for identical series."""
    klines = [[str(i), "", "", "", str(i), "1"] for i in range(10)]
    result = correlation_math.calculate_price_correlation(klines, klines, 5)
    assert round(result, 6) == 1.0


def test_process_symbol_correlation_with_mocked_logger():
    """Ensure correlation processing returns expected keys."""
    mock_klines = [[str(i), "", "", "", str(i), "2"] for i in range(5040)]
    with patch("core.fetch_recent_klines", return_value=mock_klines), \
         patch("core.get_open_interest_change", return_value=5.0):
        result = core.process_symbol_correlation("ETHUSDT", mock_klines, MagicMock())
        assert isinstance(result, dict)
        assert result["Symbol"] == "ETHUSDT"
        assert "5M" in result
        assert "Funding Rate" in result

def test_get_funding_rate_success_timestamp():
    """Ensure timestamp reflects when the rate was fetched."""
    ts = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp() * 1000)
    mock_data = {
        "result": {"list": [{"fundingRate": "0.0001"}]},
        "time": str(ts)
    }
    with patch("core.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_data
        rate, ts_returned = core.get_funding_rate("BTCUSDT")
        assert rate == 0.0001
        assert ts_returned == ts

def test_get_open_interest_change():
    """Calculate correct open interest percentage change."""
    mock_data = {
        "result": {
            "list": [
                {"openInterest": "100"},
                {"openInterest": "110"}
            ]
        }
    }
    with patch("core.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_data
        change = core.get_open_interest_change("BTCUSDT")
        assert round(change, 4) == 10.0


def test_get_open_interest_change_sorts_by_timestamp():
    """Ensure change calculation sorts rows by timestamp."""
    mock_data = {
        "result": {
            "list": [
                {"openInterest": "110", "timestamp": "2"},
                {"openInterest": "100", "timestamp": "1"},
            ]
        }
    }
    with patch("core.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_data
        change = core.get_open_interest_change("BTCUSDT")
        assert round(change, 4) == 10.0

# -------------------------------
# Tests for volume_math.calculate_volume_change
# -------------------------------

def test_calculate_volume_change_5m():
    """Detects a volume spike in 5m block size (5 klines)."""
    baseline = [[str(i), "", "", "", "", "1"] for i in range(5)]
    spike = [[str(i + 10000), "", "", "", "", "10"] for i in range(5)]
    klines = (baseline * 20) + spike
    result = calculate_volume_change(klines, 5)
    assert result > 800

def test_calculate_volume_change_30m():
    """Detects a volume spike in 30m block size (30 klines)."""
    baseline = [[str(i), "", "", "", "", "3"] for i in range(30)]
    spike = [[str(i + 50000), "", "", "", "", "15"] for i in range(30)]
    klines = (baseline * 20) + spike
    result = calculate_volume_change(klines, 30)
    assert result > 300

def test_calculate_volume_change_exactly_21_blocks():
    """Returns 0.0 when all blocks have identical volume."""
    one_block = [[str(i), "", "", "", "", "5"] for i in range(15)]
    klines = one_block * 21
    result = calculate_volume_change(klines, 15)
    assert isinstance(result, float)
    assert result == 0.0

def test_calculate_volume_change_latest_spike():
    """Detects a large positive volume spike in the latest block."""
    baseline = [[str(i), "", "", "", "", "2"] for i in range(15)]
    spike = [[str(i + 3000), "", "", "", "", "20"] for i in range(15)]
    klines = (baseline * 20) + spike
    result = calculate_volume_change(klines, 15)
    assert result > 800

def test_calculate_volume_change_latest_drop():
    """Detects a large negative volume drop in the latest block."""
    baseline = [[str(i), "", "", "", "", "10"] for i in range(15)]
    drop = [[str(i + 3000), "", "", "", "", "1"] for i in range(15)]
    klines = (baseline * 20) + drop
    result = calculate_volume_change(klines, 15)
    assert result < -85

def test_calculate_volume_change_insufficient_blocks():
    """Returns 0.0 if fewer than 21 blocks are available."""
    block = [[str(i), "", "", "", "", "3"] for i in range(15)]
    klines = block * 19
    result = calculate_volume_change(klines, 15)
    assert result == 0.0

def test_calculate_volume_change_invalid_data():
    """Returns 0.0 when volume data is malformed."""
    bad_klines = [["abc", "", "", "", "", "X"] for _ in range(315)]
    result = calculate_volume_change(bad_klines, 15)
    assert result == 0.0

def test_calculate_volume_change_4h():
    """Detects a volume shift in 4h block size (240 klines)."""
    baseline = [[str(i), "", "", "", "", "5"] for i in range(240)]
    spike = [[str(i + 100000), "", "", "", "", "30"] for i in range(240)]
    klines = (baseline * 20) + spike
    result = calculate_volume_change(klines, 240)
    assert result > 400


def test_calculate_volume_change_cache_usage():
    """Ensure sorted klines are cached and reused for identical objects."""
    core.SORTED_KLINES_CACHE.clear()
    klines = [[str(i), "", "", "", "", "1"] for i in range(105)]

    calculate_volume_change(klines, 5)
    assert len(core.SORTED_KLINES_CACHE) == 1

    calculate_volume_change(klines, 5)
    assert len(core.SORTED_KLINES_CACHE) == 1




def test_send_push_notification_runs_subprocess_on_windows():
    """Notification launched via subprocess on Windows."""
    logger = MagicMock()
    with patch("scan.platform.system", return_value="Windows"), \
         patch("scan.get_toast_notifier", return_value=object()), \
         patch("scan.subprocess.run") as mock_run:
        scan.send_push_notification("title", "msg", logger)
        mock_run.assert_called_once()


def test_send_push_notification_skips_on_non_windows():
    """No notification subprocess when not running on Windows."""
    logger = MagicMock()
    with patch("scan.platform.system", return_value="Linux"), \
         patch("scan.get_toast_notifier") as mock_get, \
         patch("scan.subprocess.run") as mock_run:
        scan.send_push_notification("title", "msg", logger)
        mock_get.assert_not_called()
        mock_run.assert_not_called()


def test_export_to_excel_swaps_column_order():
    """24h volume column should precede funding rate in exported sheet."""
    df = pd.DataFrame([
        {
            "Symbol": "BTCUSDT",
            "Funding Rate": 0.001,
            "24h USD Volume": 1000,
            "5M": 1,
            "Open Interest Change": 2,
        }
    ])
    logger = MagicMock()
    captured = {}
    with patch("scan.pd.ExcelWriter") as mock_writer, \
         patch("scan.wait_for_file_close"), \
         patch("pandas.DataFrame.to_excel", autospec=True) as mock_to_excel:
        writer = MagicMock()
        writer.book.add_format.return_value = MagicMock()
        writer.sheets = {"Sheet1": MagicMock()}
        mock_writer.return_value.__enter__.return_value = writer

        def capture(self, *args, **kwargs):  # pylint: disable=unused-argument
            captured["cols"] = list(self.columns)

        mock_to_excel.side_effect = capture

        scan.export_to_excel(df, ["BTCUSDT"], logger)
        cols = captured.get("cols")
        assert cols.index("24h USD Volume") < cols.index("Funding Rate")
