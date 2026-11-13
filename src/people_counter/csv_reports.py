"""Spreadsheet-safe CSV exports, separate from machine-readable source data."""

from collections.abc import Iterable, Mapping, Sequence
import csv
import io
from typing import Any


def spreadsheet_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: "'" + value
        if isinstance(value, str) and (
            value.lstrip().startswith(("=", "+", "-", "@"))
            or value.startswith(("\t", "\r", "\n"))
        ) else value
        for key, value in row.items()
    }


def spreadsheet_csv(fields: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> Iterable[str]:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writerow(spreadsheet_row(dict(zip(fields, fields))))
    yield stream.getvalue()
    for row in rows:
        stream.seek(0)
        stream.truncate()
        writer.writerow(spreadsheet_row(row))
        yield stream.getvalue()
