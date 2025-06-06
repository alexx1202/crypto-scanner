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

## Email Alerts

Set the following environment variables to receive an email after each scan:

```
SMTP_HOST   # SMTP server hostname
SMTP_PORT   # SMTP server port
SMTP_USER   # Username for authentication
SMTP_PASS   # Password for authentication
EMAIL_TO    # Recipient email address (defaults to alexx1202@gmail.com)
EMAIL_FROM  # Sender address (defaults to SMTP_USER)
```

If any variables are missing, the scan logs will list which ones were absent
before skipping email alerts.

## Push Notifications

To receive a Pushover notification when the scan completes, set the following
environment variables:

```
PUSHOVER_USER_KEY   # Your Pushover user key
PUSHOVER_API_TOKEN  # API token for your Pushover application
```

As with email alerts, the script will log any missing variables and skip the
notification if configuration is incomplete.

