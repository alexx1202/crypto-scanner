"""Utilities for running a Bybit volume scan and exporting results."""

import os
import logging
import platform
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib
import subprocess
import webbrowser
import asyncio

from typing import Callable

import pandas as pd
from tqdm import tqdm
from xlsxwriter.utility import xl_col_to_name

import core
import correlation_math
from scan_utils import wait_for_file_close


def get_toast_notifier():
    """Return the ``ToastNotifier`` class if ``win10toast`` is available."""
    try:  # pragma: no cover - optional dependency
        module = importlib.import_module("win10toast")
        return module.ToastNotifier
    except (ImportError, AttributeError):
        return None


def setup_logging() -> logging.Logger:
    """Configure and return the main scanner logger."""
    logger = logging.getLogger("volume_logger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler("logs/scanlog.txt")
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    return logger


def clean_existing_excels(logger: logging.Logger | None = None) -> None:
    """Delete existing Excel files in the working directory."""
    if logger is None:
        logger = logging.getLogger("volume_logger")
    for file in os.listdir():
        if file.endswith(".xlsx"):
            wait_for_file_close(file, logger)
            try:
                os.remove(file)
            except OSError:
                logger.warning("Failed to delete %s", file)


def send_push_notification(title: str, message: str, logger: logging.Logger) -> None:
    """Show a Windows toast notification if supported."""
    if platform.system() != "Windows":
        logger.info("Windows notifications not supported on this OS. Skipping.")
        return

    notifier_class = get_toast_notifier()
    if notifier_class is None:
        logger.info("win10toast not installed. Skipping notification.")
        return

    try:
        notifier = notifier_class()
        if hasattr(notifier, "on_destroy"):
            original = notifier.on_destroy

            def _on_destroy(hwnd, msg, wparam, lparam) -> int:
                original(hwnd, msg, wparam, lparam)
                return 0

            notifier.on_destroy = _on_destroy

        notifier.show_toast(title, message, duration=5)
        logger.info("Windows notification sent")
    except (OSError, TypeError) as exc:  # pragma: no cover - platform specific error
        logger.warning("Failed to send notification: %s", exc)


def export_to_excel(
    df: pd.DataFrame,
    symbol_order: list,
    logger: logging.Logger,
    filename: str = "Crypto_Volume.xlsx",
    header: str = "% Distance Below or Above 20 Bar Moving Average Volume Indicator",
    *,
    apply_conditional_formatting: bool = True,
    writer: pd.ExcelWriter | None = None,
    sheet_name: str = "Sheet1",
) -> None:
    # pylint: disable=too-many-locals,too-many-arguments
    """Write ``df`` to ``filename`` with formatting."""
    if "Symbol" in df.columns and not df.empty:
        df["__sort_order"] = df["Symbol"].map({s: i for i, s in enumerate(symbol_order)})
        df = df.sort_values("__sort_order").drop(columns=["__sort_order"])
    elif "Symbol" not in df.columns:
        logger.warning("'Symbol' column missing. Skipping sorting for sheet '%s'", sheet_name)

    if "Funding Rate" in df.columns and "24h USD Volume" in df.columns:
        cols = df.columns.tolist()
        fr_idx = cols.index("Funding Rate")
        vol_idx = cols.index("24h USD Volume")
        if fr_idx < vol_idx:
            cols[fr_idx], cols[vol_idx] = cols[vol_idx], cols[fr_idx]
            df = df[cols]

    manage_writer = writer is None
    if manage_writer:
        logger.info("Exporting data to Excel: %s", filename)
        wait_for_file_close(filename, logger)
        writer = pd.ExcelWriter(filename, engine="xlsxwriter")
    else:
        logger.info("Exporting sheet: %s", sheet_name)

    df.to_excel(writer, index=False, sheet_name=sheet_name, startrow=1)
    worksheet = writer.sheets[sheet_name]
    header_format = writer.book.add_format({"bold": True})
    worksheet.write("A1", header, header_format)
    worksheet.freeze_panes(2, 0)

    red_format = writer.book.add_format({
        "bg_color": "#FFC7CE",
        "font_color": "#9C0006",
    })
    green_format = writer.book.add_format({
        "bg_color": "#C6EFCE",
        "font_color": "#006100",
    })
    currency_format = writer.book.add_format({"num_format": "$#,##0.00"})
    percent_format = writer.book.add_format({"num_format": '0.00"%"'})
    funding_format = writer.book.add_format({"num_format": '0.0000000%'})

    if "24h USD Volume" in df.columns:
        col_idx = df.columns.get_loc("24h USD Volume")
        worksheet.set_column(col_idx, col_idx, None, currency_format)

    percent_columns = [
        name
        for name in [
            "5M",
            "15M",
            "30M",
            "1H",
            "4H",
            "5M Percentile",
            "15M Percentile",
            "30M Percentile",
            "1H Percentile",
            "4H Percentile",
            "1D",
            "1W",
            "1M",
            "Open Interest Change",
        ]
        if name in df.columns
    ]
    for name in percent_columns:
        col = df.columns.get_loc(name)
        worksheet.set_column(col, col, None, percent_format)

    if "Funding Rate" in df.columns:
        idx = df.columns.get_loc("Funding Rate")
        worksheet.set_column(idx, idx, None, funding_format)

    if apply_conditional_formatting:
        columns_to_format = [
            name
            for name in [
                "5M",
                "15M",
                "30M",
                "1H",
                "4H",
                "1D",
                "1W",
                "1M",
                "Open Interest Change",
                "Funding Rate",
            ]
            if name in df.columns
        ]
        for name in columns_to_format:
            col = df.columns.get_loc(name)
            col_letter = xl_col_to_name(col)
            cell_range = f"{col_letter}3:{col_letter}1048576"
            worksheet.conditional_format(cell_range, {
                "type": "cell",
                "criteria": ">",
                "value": 0,
                "format": green_format
            })
            worksheet.conditional_format(cell_range, {
                "type": "cell",
                "criteria": "<",
                "value": 0,
                "format": red_format
            })
    if manage_writer:
        writer.close()


_OPENED_PATHS: set[str] = set()


def open_in_edge(file_path: str, logger: logging.Logger) -> None:
    """Open ``file_path`` in Microsoft Edge only once per session."""
    if file_path in _OPENED_PATHS:
        logger.info("Edge already open for %s", file_path)
        return

    _OPENED_PATHS.add(file_path)

    if platform.system() == "Windows":
        try:
            subprocess.Popen(  # pylint: disable=consider-using-with
                ["cmd", "/c", "start", "msedge", file_path]
            )
        except OSError as exc:  # pragma: no cover - platform specific
            logger.warning("Failed to open Edge: %s", exc)
    else:  # pragma: no cover - platform specific
        webbrowser.open(file_path)


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
def export_to_html(
    df: pd.DataFrame,
    symbol_order: list,
    logger: logging.Logger,
    filename: str,
    header: str,
    *,
    include_sort_buttons: bool = False,
    refresh_seconds: int = 60,
) -> None:
    """Write ``df`` to ``filename`` with a dark theme and auto-refresh."""
    if "Symbol" in df.columns and not df.empty:
        df["__sort_order"] = df["Symbol"].map({s: i for i, s in enumerate(symbol_order)})
        df = df.sort_values("__sort_order").drop(columns=["__sort_order"])
    elif "Symbol" not in df.columns:
        logger.warning("'Symbol' column missing. Skipping sorting for file '%s'", filename)

    os.makedirs("html", exist_ok=True)
    path = os.path.join("html", filename)
    logger.info("Exporting data to HTML: %s", path)

    def style_cell(val: float) -> str:
        try:
            num = float(val)
        except (TypeError, ValueError):
            return ""
        return (
            "background-color:#C6EFCE;color:#006100"
            if num >= 0
            else "background-color:#FFC7CE;color:#9C0006"
        )

    numeric_cols = df.select_dtypes("number").columns
    highlight_cols = [
        c
        for c in numeric_cols
        if "Percentile" not in c and c != "24h USD Volume"
    ]
    styled = df.style.map(style_cell, subset=highlight_cols)

    format_dict: dict[str, Callable] = {}
    if "24h USD Volume" in numeric_cols:
        format_dict["24h USD Volume"] = lambda x: f"${x:,.0f}"
    for col in numeric_cols:
        if col == "Funding Rate":
            format_dict[col] = lambda x: f"{x:.7f}%"
        elif "Percentile" in col:
            format_dict[col] = lambda x: f"{x * 100:.2f}%"
        elif col != "24h USD Volume":
            format_dict[col] = lambda x: f"{x:.2f}%"

    styled = styled.format(format_dict)
    html_table = styled.to_html(index=False, table_uuid="data-table")
    html_table = html_table.replace('id="T_data-table"', 'id="data-table"')

    nav = ""
    if include_sort_buttons:
        timeframes = ["5M", "15M", "30M", "1H", "4H", "1D", "1W", "1M"]
        sort_buttons = "".join(
            f"<button onclick=\"sortBy('{tf}')\">{tf}</button>"
            for tf in timeframes
            if tf in df.columns
        )
        nav = (
            "<div style='display:flex;justify-content:flex-start;"
            "align-items:center;gap:4px;margin-bottom:8px'>"
            f"{sort_buttons}</div>"
        )

    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "<html><head><meta charset='utf-8'>"
            f"<meta http-equiv='refresh' content='{refresh_seconds}'>"
            "<style>"
            "body{background:#121212;color:#fff;font-family:Arial,Helvetica,sans-serif;}"
            "table{background:#1e1e1e;color:#fff;border-collapse:collapse;width:100%;}"
            "th,td{border:1px solid #333;padding:4px;text-align:right;}"
            "th{background:#333;}"
            "td:first-child,th:first-child{text-align:left;}"
            "button{background:#333;color:#fff;border:1px solid #555;padding:4px 8px;"
            "margin-right:4px;cursor:pointer;}"
            "button:hover{background:#444;}"
            "</style>"
            f"<title>{header}</title></head><body>"
        )
        if nav:
            f.write(nav)
        f.write(f"<h1 style='margin-top:8px'>{header}</h1>")
        f.write(html_table)
        if include_sort_buttons:
            f.write(
                "<script>"
                "const sortDirections = {};"
                "function sortBy(col){"
                "  const table=document.getElementById('data-table');"
                "  const headers=Array.from(table.rows[0].cells).map(c=>c.textContent.trim());"
                "  const idx=headers.indexOf(col);"
                "  if(idx===-1)return;"
                "  const dir=sortDirections[col]||'desc';"
                "  const rows=Array.from(table.tBodies[0].rows);"
                "  rows.sort((a,b)=>{"
                "    const aVal=parseFloat(a.cells[idx].textContent.replace(/[%,$]/g,'')"
                ".replace(/,/g,''))||0;"
                "    const bVal=parseFloat(b.cells[idx].textContent.replace(/[%,$]/g,'')"
                ".replace(/,/g,''))||0;"
                "    return dir==='desc'?bVal-aVal:aVal-bVal;"
                "  });"
                "  rows.forEach(r=>table.tBodies[0].appendChild(r));"
                "  sortDirections[col]=dir==='desc'?'asc':'desc';"
                "}"
                "</script>"
            )
        f.write("</body></html>")
    open_in_edge(os.path.abspath(path), logger)

def submit_symbol_futures(symbols: list[str], executor: ThreadPoolExecutor,
                           logger: logging.Logger, func) -> dict:
    """Return a mapping of futures to their corresponding symbol."""
    return {
        executor.submit(func, symbol, logger): symbol
        for symbol in symbols
    }


def scan_and_collect_results(symbols: list[str],
                             logger: logging.Logger,
                             func=core.process_symbol) -> tuple[list, list]:
    """Process all symbols concurrently and collect successes and failures."""
    rows: list[dict] = []
    failed: list[str] = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = submit_symbol_futures(symbols, executor, logger, func)
        for future in tqdm(as_completed(futures), total=len(futures),
                           desc="Scanning"):
            symbol = futures[future]
            result = future.result()
            if result:
                rows.append(result)
            else:
                failed.append(symbol)
    return rows, failed


def run_scan(
    all_symbols: list[tuple],
    logger: logging.Logger,
    klines_cache: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    """Fetch volume, funding and open interest metrics."""

    logger.info("Scanning volume metrics...")

    volume_rows, failed = scan_and_collect_results(
        [s for s, _ in all_symbols],
        logger,
        lambda s, log: core.process_symbol(s, log, klines_cache),
    )

    volume_map = dict(all_symbols)
    for row in volume_rows:
        row["24h USD Volume"] = volume_map.get(row["Symbol"], 0)
    volume_df = pd.DataFrame(volume_rows)
    export_to_html(
        volume_df,
        [s for s, _ in all_symbols],
        logger,
        filename="volume.html",
        header="% Distance Below or Above 20 Bar Moving Average Volume Indicator",
        refresh_seconds=900,
        include_sort_buttons=True,
    )

    logger.info("Scanning funding rates...")
    funding_rows, _ = scan_and_collect_results(
        [s for s, _ in all_symbols],
        logger,
        core.process_symbol_funding,
    )
    funding_df = pd.DataFrame(funding_rows)
    export_to_html(
        funding_df,
        [s for s, _ in all_symbols],
        logger,
        filename="funding_rates.html",
        header="Latest Funding Rates",
        refresh_seconds=60,
        include_sort_buttons=True,
    )

    logger.info("Scanning open interest changes...")
    oi_rows, _ = scan_and_collect_results(
        [s for s, _ in all_symbols],
        logger,
        core.process_symbol_open_interest,
    )
    oi_df = pd.DataFrame(oi_rows)
    export_to_html(
        oi_df,
        [s for s, _ in all_symbols],
        logger,
        filename="open_interest.html",
        header="% Change in Open Interest",
        refresh_seconds=60,
        include_sort_buttons=True,
    )

    if failed:
        logger.warning("%d symbols failed: %s", len(failed), ", ".join(failed))

    return (
        volume_df,
        funding_df,
        oi_df,
        [s for s, _ in all_symbols],
    )



def run_correlation_matrix_scan(
    all_symbols: list[tuple],
    logger: logging.Logger,
    klines_cache: dict | None = None,
) -> dict[str, pd.DataFrame]:
    """Return correlation matrices for each timeframe."""
    logger.info("Starting correlation matrix scan...")

    if not all_symbols:
        logger.warning("No symbols retrieved. Skipping correlation matrix export.")
        return {}

    minutes_map = {"5M": 5, "15M": 15, "30M": 30, "1H": 60, "4H": 240}
    returns_data = {key: {} for key in minutes_map}

    def _get_returns(symbol: str) -> tuple[str, dict[str, list]]:
        klines = core.fetch_recent_klines(symbol, cache=klines_cache)
        if not klines:
            logger.warning(
                "%s skipped: No valid klines returned for correlation matrix.",
                symbol,
            )
            return symbol, {}
        data: dict[str, list] = {}
        for label, minutes in minutes_map.items():
            ret = correlation_math.calculate_returns(klines, minutes)
            if len(ret) == minutes:
                data[label] = ret
        return symbol, data

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {
            executor.submit(_get_returns, s): s for s, _ in all_symbols
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="CorrMatrix"):
            symbol, data = future.result()
            for label, ret in data.items():
                returns_data[label][symbol] = ret

    matrices: dict[str, pd.DataFrame] = {}
    for label, data in returns_data.items():
        df = pd.DataFrame(data)
        matrices[label] = df.corr() * 100 if not df.empty else pd.DataFrame()

    return matrices


def run_price_change_scan(
    all_symbols: list[tuple],
    logger: logging.Logger,
    klines_cache: dict | None = None,
) -> pd.DataFrame:
    """Compute close price change for each symbol."""
    logger.info("Starting price change scan...")
    # This scan only relies on price data and remains fully functional even
    # when other scans such as the old volatility scan are disabled. It
    # fetches klines for each symbol independently and does not use any
    # results from other metrics.
    if not all_symbols:
        logger.warning("No symbols retrieved. Skipping price change export.")
        return pd.DataFrame()

    rows, failed = scan_and_collect_results(
        [s for s, _ in all_symbols],
        logger,
        lambda s, log: core.process_symbol_price_change(s, log, klines_cache),
    )

    if failed:
        logger.warning("%d symbols failed: %s", len(failed), ", ".join(failed))

    df = pd.DataFrame(rows)
    logger.info("Price change data collected for %d symbols", len(df))
    for col in [
        "5M Percentile",
        "15M Percentile",
        "30M Percentile",
        "1H Percentile",
        "4H Percentile",
    ]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    export_to_html(
        df,
        [s for s, _ in all_symbols],
        logger,
        filename="price_change.html",
        header="% Price Change",
        refresh_seconds=1800,
        include_sort_buttons=True,
    )

    return df


def export_all_data(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    volume_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    oi_df: pd.DataFrame,
    price_df: pd.DataFrame,
    symbol_order: list[str],
    logger: logging.Logger,
    filename: str = "Scan.xlsx",
) -> None:
    """Write all metric DataFrames to an Excel file."""

    wait_for_file_close(filename, logger)
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        export_to_excel(
            volume_df,
            symbol_order,
            logger,
            header="% Distance Below or Above 20 Bar Moving Average Volume Indicator",
            writer=writer,
            sheet_name="Volume",
        )
        export_to_excel(
            funding_df,
            symbol_order,
            logger,
            header="Latest Funding Rates",
            writer=writer,
            sheet_name="Funding Rates",
        )
        export_to_excel(
            oi_df,
            symbol_order,
            logger,
            header="% Change in Open Interest",
            writer=writer,
            sheet_name="Open Interest",
        )
        export_to_excel(
            price_df,
            symbol_order,
            logger,
            header="% Price Change",
            writer=writer,
            sheet_name="Price Change",
        )
    logger.info("Export complete: %s", filename)


def export_correlation_matrices(
    matrices: dict[str, pd.DataFrame],
    logger: logging.Logger,
    filename: str = "Correlation_Matrix.xlsx",
) -> None:
    """Write correlation matrices to ``filename`` as separate sheets."""

    if not matrices:
        logger.warning("No correlation matrix data to export.")
        return

    wait_for_file_close(filename, logger)
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        percent_format = writer.book.add_format({"num_format": '0.00"%"'})
        red_format = writer.book.add_format({
            "bg_color": "#FFC7CE",
            "font_color": "#9C0006",
        })
        green_format = writer.book.add_format({
            "bg_color": "#C6EFCE",
            "font_color": "#006100",
        })
        for sheet, df in matrices.items():
            if df.empty:
                continue
            df.to_excel(writer, sheet_name=sheet)
            worksheet = writer.sheets[sheet]
            worksheet.freeze_panes(1, 1)
            for idx in range(1, len(df.columns) + 1):
                worksheet.set_column(idx, idx, None, percent_format)
                col_letter = xl_col_to_name(idx)
                cell_range = f"{col_letter}2:{col_letter}1048576"
                worksheet.conditional_format(cell_range, {
                    "type": "cell",
                    "criteria": ">",
                    "value": 0,
                    "format": green_format,
                })
                worksheet.conditional_format(cell_range, {
                    "type": "cell",
                    "criteria": "<",
                    "value": 0,
                    "format": red_format,
                })
    logger.info("Export complete: %s", filename)


def export_correlation_matrix_html(
    matrices: dict[str, pd.DataFrame],
    logger: logging.Logger,
    filename: str = "correlation_matrix.html",
    *,
    refresh_seconds: int = 180,
) -> None:
    """Write correlation matrices to a single HTML file with timeframe buttons."""

    if not matrices:
        logger.warning("No correlation matrix data to export.")
        return

    os.makedirs("html", exist_ok=True)
    path = os.path.join("html", filename)
    logger.info("Exporting data to HTML: %s", path)

    def matrix_to_html(df: pd.DataFrame, label: str) -> str:
        """Return a plain HTML table for ``df`` with CSS classes for colours."""

        headers = ["<th id='corner'></th>"] + [f"<th>{c}</th>" for c in df.columns]
        rows = ["<thead><tr>" + "".join(headers) + "</tr></thead><tbody>"]

        for idx, row in df.iterrows():
            cells = [f"<th>{idx}</th>"]
            for val in row:
                try:
                    num = float(val)
                except (TypeError, ValueError):
                    cls = ""
                    text = ""
                else:
                    cls = "pos" if num >= 0 else "neg"
                    text = f"{num:.2f}%"
                cells.append(f"<td class='{cls}'>{text}</td>")
            rows.append("<tr>" + "".join(cells) + "</tr>")

        rows.append("</tbody>")
        table = [
            "<div class='table-wrapper'>",
            f"<table id='table-{label}'>",
            "".join(rows),
            "</table></div>",
        ]
        return "".join(table)
    html_parts = [
        "<html><head><meta charset='utf-8'>",
        f"<meta http-equiv='refresh' content='{refresh_seconds}'>",
        "<style>",
        "body{background:#121212;color:#fff;font-family:Arial,Helvetica,sans-serif;}",
        "table{background:#1e1e1e;color:#fff;border-collapse:collapse;width:max-content;}",
        ".table-wrapper{max-height:90vh;overflow:auto;}",
        "th,td{border:1px solid #333;padding:4px;text-align:right;}",
        "thead th{background:#333;position:sticky;top:0;z-index:2;}",
        "tbody th{background:#333;left:0;position:sticky;z-index:1;text-align:left;}",
        "td:first-child{left:0;position:sticky;background:#1e1e1e;z-index:1;text-align:left;}",
        "#corner{z-index:3;}",
        "button{background:#333;color:#fff;border:1px solid #555;padding:4px 8px;",
        "margin-right:4px;cursor:pointer;}",
        "button:hover{background:#444;}",
        ".pos{background:#C6EFCE;color:#006100;}",
        ".neg{background:#FFC7CE;color:#9C0006;}",
        "</style>",
        "<title>Correlation Matrix</title></head><body>",
    ]

    buttons = "".join(
        f"<button onclick=\"showMatrix('{label}')\">{label}</button>"
        for label in matrices
    )

    symbols = sorted(next(iter(matrices.values())).columns)
    options = "".join(f"<option value='{s}'>{s}</option>" for s in symbols)
    dropdowns = (
        f"<select id='filter1' onchange='applyFilter()'>"
        f"<option value=''>Symbol 1</option>{options}</select>"
        f"<select id='filter2' onchange='applyFilter()'>"
        f"<option value=''>Symbol 2</option>{options}</select>"
        "<button onclick='resetFilter()'>Reset Filter</button>"
    )

    html_parts.append(f"<div>{buttons}{dropdowns}</div>")

    first = True
    for label, df in matrices.items():
        html_table = matrix_to_html(df, label)
        display = "" if first else " style='display:none'"
        html_parts.append(f"<div id='div-{label}'{display}>{html_table}</div>")
        first = False

    html_parts.extend([
        "<script>",
        "function showMatrix(label){",
        "  document.querySelectorAll('[id^=div-]').forEach(d=>d.style.display='none');",
        "  document.getElementById('div-'+label).style.display='block';",
        "}",
        "function applyFilter(){",
        "  const sym1=document.getElementById('filter1').value;",
        "  const sym2=document.getElementById('filter2').value;",
        "  document.querySelectorAll('[id^=div-] table').forEach(t=>{",
        "    const allowed=(!sym1||!sym2)?null:[sym1,sym2];",
        "    t.querySelectorAll('tbody tr').forEach(r=>{",
        "      const sym=r.querySelector('th').textContent;",
        "      r.style.display=!allowed||allowed.includes(sym)?'':'none';",
        "    });",
        "    const heads=t.querySelectorAll('thead th');",
        "    heads.forEach((th,i)=>{",
        "      if(i===0)return;",
        "      const sym=th.textContent;",
        "      const show=!allowed||allowed.includes(sym);",
        "      th.style.display=show?'':'none';",
        "      t.querySelectorAll('tbody tr').forEach(row=>{",
        "        const cells=row.querySelectorAll('td');",
        "        if(cells[i-1]) cells[i-1].style.display=show?'':'none';",
        "      });",
        "    });",
        "  });",
        "}",
        "function resetFilter(){",
        "  document.getElementById('filter1').selectedIndex=0;",
        "  document.getElementById('filter2').selectedIndex=0;",
        "  applyFilter();",
        "}",
        "</script>",
        "</body></html>",
    ])

    html_content = "".join(html_parts)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_content)

    open_in_edge(os.path.abspath(path), logger)
def export_all_data_html(
    volume_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    oi_df: pd.DataFrame,
    price_df: pd.DataFrame,
    symbol_order: list[str],
    logger: logging.Logger,
) -> None:
    """Write all metric DataFrames to individual HTML files."""  # pylint: disable=too-many-arguments,too-many-positional-arguments
    export_to_html(
        volume_df,
        symbol_order,
        logger,
        filename="volume.html",
        header="% Distance Below or Above 20 Bar Moving Average Volume Indicator",
        refresh_seconds=900,
        include_sort_buttons=True,
    )
    export_to_html(
        funding_df,
        symbol_order,
        logger,
        filename="funding_rates.html",
        header="Latest Funding Rates",
        refresh_seconds=60,
        include_sort_buttons=True,
    )
    export_to_html(
        oi_df,
        symbol_order,
        logger,
        filename="open_interest.html",
        header="% Change in Open Interest",
        refresh_seconds=60,
        include_sort_buttons=True,
    )
    export_to_html(
        price_df,
        symbol_order,
        logger,
        filename="price_change.html",
        header="% Price Change",
        refresh_seconds=1800,
        include_sort_buttons=True,
    )


def main() -> None:
    """Entry point for running the scanner from the command line."""
    logger = setup_logging()
    try:
        logger.info("Fetching USDT perpetual futures from Bybit...")
        all_symbols = core.get_tradeable_symbols_sorted_by_volume()
        logger.info("Total pairs found: %d", len(all_symbols))

        if not all_symbols:
            logger.warning("No symbols retrieved. Skipping export.")
            return

        clean_existing_excels(logger)

        symbols_only = [s for s, _ in all_symbols]
        logger.info("Fetching klines asynchronously for %d symbols", len(symbols_only))
        klines_cache = asyncio.run(core.fetch_all_recent_klines_async(symbols_only))

        volume_df, funding_df, oi_df, symbol_order = run_scan(all_symbols, logger, klines_cache)
        matrix_map = run_correlation_matrix_scan(all_symbols, logger, klines_cache)
        price_df = run_price_change_scan(all_symbols, logger, klines_cache)

        export_correlation_matrices(matrix_map, logger)
        send_push_notification(
            "Correlation matrix complete",
            "Correlation_Matrix.xlsx has been exported.",
            logger,
        )

        export_all_data(
            volume_df,
            funding_df,
            oi_df,
            price_df,
            symbol_order,
            logger,
        )
    except (RuntimeError, ValueError, TypeError) as exc:
        logger.exception("Script failed: %s", exc)


if __name__ == "__main__":
    main()
