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
        result = scan.fetch_recent_klines("BTCUSDT", total=315)
        assert isinstance(result, list)
        assert len(result) == 315

def test_fetch_recent_klines_insufficient():
    def build_chunk():
        return [[str(1717382400000), "", "", "", "", "1"] for _ in range(100)]

    mock_response = {"result": {"list": build_chunk()}}
    responses = [mock_response, mock_response, mock_response]

    def side_effect(*args, **kwargs):
        if responses:
            return MagicMock(status_code=200, json=lambda: responses.pop(0))
        return MagicMock(status_code=200, json=lambda: {"result": {"list": []}})

    with patch("scan.requests.get", side_effect=side_effect):
        result = scan.fetch_recent_klines("BTCUSDT", total=315)
        assert result == []

def test_calculate_volume_change_valid():
    one_block = [[str(i), "", "", "", "", "2"] for i in range(15)]
    klines = one_block * 21
    pct = scan.calculate_volume_change(klines, 15)
    assert isinstance(pct, float)
    assert pct == 0.0

def test_calculate_volume_change_insufficient_blocks():
    one_block = [[str(i), "", "", "", "", "1"] for i in range(15)]
    klines = one_block * 20
    pct = scan.calculate_volume_change(klines, 15)
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
    assert logger.level == logging.INFO


def test_calculate_volume_change_cache_reuse():
    one_block = [[str(i), "", "", "", "", "1"] for i in range(15)]
    klines = one_block * 21
    scan._sorted_klines_cache.clear()
    scan.calculate_volume_change(klines, 15)
    # After first call the cache should have a single entry keyed by id(klines)
    assert len(scan._sorted_klines_cache) == 1
    scan.calculate_volume_change(klines, 60)
    # Cache should still contain only one entry
    assert len(scan._sorted_klines_cache) == 1


def test_main_no_results():
    with patch("scan.get_tradeable_symbols_sorted_by_volume", return_value=[("BTCUSDT", 1)]), \
         patch("scan.fetch_recent_klines", return_value=[]), \
         patch("scan.pd.ExcelWriter") as mock_writer, \
         patch("scan.setup_logging") as mock_logging:
        mock_logging.return_value = logging.getLogger("test")
        scan.main()
        mock_writer.assert_not_called()
