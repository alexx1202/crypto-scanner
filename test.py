"""
Unit test suite for Bybit volume anomaly scanner.
Tests include symbol sorting, kline deduplication, volume spike/dip detection,
and Excel export behavior. Supports pytest + pylint 10/10 compliance.
"""

import logging
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
import os
import core
import scan
from volume_math import calculate_volume_change

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
    with patch("core.fetch_recent_klines", return_value=mock_klines):
        result = core.process_symbol("BTCUSDT", MagicMock())
        assert isinstance(result, dict)
        assert result["Symbol"] == "BTCUSDT"
        assert "5M" in result
        assert "15M" in result
        assert "30M" in result
        assert "1H" in result
        assert "4H" in result
        assert "Funding Rate" in result
        assert "Funding Rate Timestamp" in result

def test_get_funding_rate_success_timestamp():
    """Ensure timestamp reflects when the rate was fetched."""
    mock_data = {"result": {"list": [{"fundingRate": "0.0001"}]}}
    with patch("core.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_data
    ts = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp() * 1000)
    mock_data = {
        "result": {"list": [{"fundingRate": "0.0001", "fundingRateTimestamp": str(ts)}]}
    }
    with patch("core.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_data
        rate, ts_returned = core.get_funding_rate("BTCUSDT")
        assert rate == 0.0001
        assert ts_returned == ts

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


def test_send_email_alert_sends_message():
    """send_email_alert logs in and sends a message when configured."""
    logger = MagicMock()
    env = {
        "SMTP_HOST": "smtp.example.com",
        "SMTP_PORT": "587",
        "SMTP_USER": "user",
        "SMTP_PASS": "pass",
        "EMAIL_TO": "to@example.com",
        "EMAIL_FROM": "from@example.com",
    }
    with patch.dict(os.environ, env, clear=True), \
         patch("scan.smtplib.SMTP") as mock_smtp:
        smtp_instance = mock_smtp.return_value.__enter__.return_value
        scan.send_email_alert("sub", "body", logger)
        smtp_instance.starttls.assert_called_once()
        smtp_instance.login.assert_called_once_with("user", "pass")
        smtp_instance.send_message.assert_called_once()


def test_send_email_alert_skips_if_missing_env():
    """No SMTP connection attempted when config vars are absent."""
    logger = MagicMock()
    with patch.dict(os.environ, {}, clear=True), \
         patch("scan.smtplib.SMTP") as mock_smtp:
        scan.send_email_alert("sub", "body", logger)
        mock_smtp.assert_not_called()
