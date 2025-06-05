# Bybit Volume Scanner

This project scans Bybit USDT perpetual markets for unusual volume activity.
The resulting spreadsheet now includes a column showing each pair's total 24​hr trading volume.

## Installation

Install the required Python packages using `pip`:

```bash
pip install -r requirements.txt
```

## Running Tests

After installing the dependencies, run the unit tests with:

```bash
pytest test.py
```

## Grouping Debug Logs

Use `group_logs.py` to organise `scanlog.txt` entries so that all lines for
each symbol appear together:

```bash
python group_logs.py /path/to/scanlog.txt
```

The script writes a new file with `_grouped` appended to the original name.

