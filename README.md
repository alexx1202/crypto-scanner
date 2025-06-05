# Bybit Volume Scanner

This project scans Bybit USDT perpetual markets for unusual volume activity.
The resulting spreadsheet now includes a column showing each pair's total 24​hr trading volume.

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

## Grouping Debug Logs

Use `group_logs.py` to organise `scanlog.txt` entries so that all lines for
each symbol appear together:

```bash
python group_logs.py /path/to/scanlog.txt
```

The script writes a new file with `_grouped` appended to the original name.

## Log Files

All logs are written to:

```
~/OneDrive/Documents/CRYPTO/PYTHON/WORK_IN_PROGRESS/cryptoscanner/logs
```

`scan.py` writes `scanlog.txt` here, while `run_checks.py` creates
`pylint.log` and `pytest.log` in the same directory.

