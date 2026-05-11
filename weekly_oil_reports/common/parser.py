"""Parser and blob layout helpers for the EIA Weekly Petroleum Status Report.

The source CSV (https://ir.eia.gov/wpsr/table9.csv) has the shape:

    Source URL: https://ir.eia.gov/wpsr/table9.csv
    Title: table9.csv
    <blank>
    "STUB_1","STUB_2","M/D/YY","M/D/YY","M/D/YY","M/D/YY","M/D/YY","M/D/YY"
    "<section>","<series>","<v1>","<v2>","<v3>","<v4>","<v5>","<v6>"
    ...

The 6 numeric columns are, in order:
    weekly_current, weekly_prior, weekly_year_ago, weekly_year_ago_2,
    four_week_avg_current, four_week_avg_year_ago

Numbers may be formatted with commas ("13,573"). Missing values appear as the
literal token "� �" (or empty). This module turns the CSV into a tidy long
DataFrame and provides a stable blob storage layout.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import pandas as pd

BLOB_PREFIX = "weekly_oil_reports"
RAW_PREFIX = f"{BLOB_PREFIX}/raw"
PARSED_PREFIX = f"{BLOB_PREFIX}/parsed"

COLUMN_TYPES: tuple[str, ...] = (
    "weekly_current",
    "weekly_prior",
    "weekly_year_ago",
    "weekly_year_ago_2",
    "four_week_avg_current",
    "four_week_avg_year_ago",
)

PARSED_COLUMNS: tuple[str, ...] = (
    "report_date",
    "as_of_date",
    "column_type",
    "category",
    "series",
    "value",
)


@dataclass(frozen=True)
class ReportHeader:
    """The 6 date columns from the report header."""

    report_date: date  # the most recent week-ending date (col 1)
    column_dates: tuple[date, date, date, date, date, date]


def _parse_short_date(s: str) -> date:
    """Parse a M/D/YY date from the EIA header (two-digit year, US order)."""
    s = s.strip().strip('"')
    return datetime.strptime(s, "%m/%d/%y").date()


def _parse_value(raw: str) -> float | None:
    """Parse a numeric cell. Returns None for missing/placeholder values."""
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "")
    if not s or "�" in s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _strip_quoted(s: str) -> str:
    return s.strip().strip('"').strip()


def _iter_rows(csv_text: str) -> Iterable[list[str]]:
    """Yield non-empty rows from the CSV, skipping the preamble metadata lines."""
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue
        first = row[0].strip()
        # Skip the preamble lines: 'Source URL: ...' and 'Title: ...'.
        # They are emitted as a single quoted cell with no comma.
        if len(row) == 1 and (first.startswith("Source URL") or first.startswith("Title")):
            continue
        yield row


def parse_header(csv_text: str) -> ReportHeader:
    """Return the report header (the 6 date columns)."""
    for row in _iter_rows(csv_text):
        if len(row) >= 8 and _strip_quoted(row[0]) == "STUB_1":
            dates = tuple(_parse_short_date(c) for c in row[2:8])
            return ReportHeader(report_date=dates[0], column_dates=dates)  # type: ignore[arg-type]
    raise ValueError("Header row not found in CSV")


def parse_report(csv_text: str) -> pd.DataFrame:
    """Parse the EIA Table 9 CSV into a long-format DataFrame.

    Columns: report_date, as_of_date, column_type, category, series, value.
    """
    header = parse_header(csv_text)
    records: list[dict] = []
    for row in _iter_rows(csv_text):
        if len(row) < 8:
            continue
        category = _strip_quoted(row[0])
        series = _strip_quoted(row[1])
        if category == "STUB_1" or not category or not series:
            continue
        for i, col_type in enumerate(COLUMN_TYPES):
            value = _parse_value(row[2 + i])
            if value is None:
                continue
            records.append(
                {
                    "report_date": header.report_date,
                    "as_of_date": header.column_dates[i],
                    "column_type": col_type,
                    "category": category,
                    "series": series,
                    "value": value,
                }
            )
    df = pd.DataFrame.from_records(records, columns=list(PARSED_COLUMNS))
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def raw_blob_key(report_date: date) -> str:
    return f"{RAW_PREFIX}/{report_date.isoformat()}.csv"


def parsed_blob_key(report_date: date) -> str:
    return f"{PARSED_PREFIX}/{report_date.isoformat()}.parquet"


def report_date_from_key(key: str) -> date | None:
    """Extract the YYYY-MM-DD report date from a raw or parsed blob key."""
    name = key.rsplit("/", 1)[-1]
    stem = name.rsplit(".", 1)[0]
    try:
        return datetime.strptime(stem, "%Y-%m-%d").date()
    except ValueError:
        return None
