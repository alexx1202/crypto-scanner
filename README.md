# Bybit Volume Scanner

This project scans Bybit USDT perpetual markets for unusual volume activity.
The resulting spreadsheet includes each pair's latest funding rate, 24​hr trading volume,
and the percentage change in open interest over the same period.
The "24h USD Volume" column is exported before the "Funding Rate" column.

## Installation

Install the required Python packages using `pip`:

```bash
pip install -r requirements.txt
```

## Running Tests

After installing the dependencies, run `run_checks.py` to execute
both pylint and pytest with progress bars:

```bash
python run_checks.py
```

## Running the Scan

Once the lint checks and tests pass, execute the scanner directly to fetch
market data and export `Crypto_Volume.xlsx`:

```bash
python scan.py
```

## Grouping Debug Logs

Use `group_logs.py` to organise `scanlog.txt` entries so that all lines for
each symbol appear together:

```bash
python group_logs.py /path/to/scanlog.txt
```

The script writes a new file with `_grouped` appended to the original name.

## Log Files

All logs are written to the `logs` directory in the project root:

```
<project_root>/logs
```

`scan.py` writes `scanlog.txt` here, while `run_checks.py` creates
`pylint.log` and `pytest.log` in the same directory.


