import pytest
import os
import logging
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
import scan

def test_get_tradeable_symbols_sorted_by_volume():
    mock_response = {
        "result": {
            "list": [
                {"symbol": "BTCUSDT", "turnover24h": "500000000"},
                {"symbol": "ETHUSDT", "turnover24h": "300000000"},
                {"symbol": "DOGEUSDT", "turnover24h": "100000000"}
            ]
        }
    }
    with patch("scan.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        result = scan.get_tradeable_symbols_sorted_by_volume()
        assert result == [
            ("BTCUSDT", 500000000.0),
            ("ETHUSDT", 300000000.0),
            ("DOGEUSDT", 100000000.0)
        ]

def test_fetch_recent_klines_exact_count():
    mock_klines = [[str(1717382400000 + i * 60000), "", "", "", "", "1"] for i in range(315)]
    mock_response = {"result": {"list": mock_klines}}
    with patch("scan.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        result = scan.fetch_recent_klines("BTCUSDT")
        assert isinstance(result, list)
        assert len(result) == 315

def test_fetch_recent_klines_insufficient():
    mock_klines = [[str(1717382400000 + i * 60000), "", "", "", "", "1"] for i in range(200)]
    mock_response = {"result": {"list": mock_klines}}
    with patch("scan.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        result = scan.fetch_recent_klines("BTCUSDT")
        assert result == []

def test_calculate_volume_change_valid():
    one_block = [[str(i), "", "", "", "", "2"] for i in range(15)]
    klines = one_block * 21
    pct = scan.calculate_volume_change(klines)
    assert isinstance(pct, float)
    assert pct == 0.0

def test_calculate_volume_change_insufficient_blocks():
    one_block = [[str(i), "", "", "", "", "1"] for i in range(15)]
    klines = one_block * 20
    pct = scan.calculate_volume_change(klines)
    assert pct == 0.0

def test_clean_existing_excels(tmp_path):
    dummy_file = tmp_path / "file.xlsx"
    dummy_file.write_text("data")

    with patch("scan.os.listdir", return_value=["file.xlsx"]), \
         patch("scan.os.remove") as mock_remove:
        scan.clean_existing_excels()
        mock_remove.assert_called_once_with("file.xlsx")

def test_setup_logging():
    logger = scan.setup_logging()
    assert isinstance(logger, logging.Logger)
    assert logger.name == "volume_logger"
    assert logger.level == logging.DEBUG
