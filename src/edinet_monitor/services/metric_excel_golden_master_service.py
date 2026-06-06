from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import json
import math
from pathlib import Path
from typing import Any

from openpyxl import load_workbook

from edinet_monitor.config.settings import PROJECT_ROOT
from edinet_monitor.services.metric_excel_audit_service import (
    HEADER_COMPANY_NAME,
    HEADER_CURRENT_PERIOD_END,
    HEADER_DECISION,
    HEADER_INDUSTRY,
    HEADER_MARKET,
    HEADER_METRIC,
    HEADER_ROW_KIND,
    HEADER_SECURITY_CODE,
    PERIOD_PERIOD_SUFFIX,
    PERIOD_RANK_SUFFIX,
    PERIOD_RATIO_SUFFIX,
    PERIOD_UNIT_SUFFIX,
    PERIOD_VALUE_SUFFIX,
)
from edinet_monitor.services.metric_excel_export_service import (
    CONDITION_SHEET,
    PERIOD_LABEL_BY_OFFSET,
    SUMMARY_SHEET,
)


DEFAULT_GOLDEN_MASTER_DIR = PROJECT_ROOT / "config" / "excel" / "golden_master"
EXCLUDED_SUMMARY_KEYS = {"generated_at"}
REQUIRED_METRIC_HEADERS = {
    HEADER_SECURITY_CODE,
    HEADER_COMPANY_NAME,
    HEADER_INDUSTRY,
    HEADER_MARKET,
    HEADER_DECISION,
    HEADER_ROW_KIND,
    HEADER_CURRENT_PERIOD_END,
    HEADER_METRIC,
}


@dataclass(frozen=True)
class MetricExcelNormalizedResult:
    excel_path: Path
    output_path: Path
    sheet_count: int
    row_count: int


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return value
    return value


def _header_map(header_row: tuple[Any, ...]) -> dict[str, int]:
    return {_clean_text(value): index for index, value in enumerate(header_row) if _clean_text(value)}


def _value_at(values: tuple[Any, ...], index: int | None) -> Any:
    if index is None or index >= len(values):
        return None
    return values[index]


def _period_columns(headers: dict[str, int]) -> dict[int, dict[str, int]]:
    columns: dict[int, dict[str, int]] = {}
    for offset, label in PERIOD_LABEL_BY_OFFSET.items():
        prefix = f"{label}_"
        mapping: dict[str, int] = {}
        for key, suffix in [
            ("period", PERIOD_PERIOD_SUFFIX),
            ("value", PERIOD_VALUE_SUFFIX),
            ("unit", PERIOD_UNIT_SUFFIX),
            ("ratio", PERIOD_RATIO_SUFFIX),
            ("rank", PERIOD_RANK_SUFFIX),
        ]:
            header = f"{prefix}{suffix}"
            if header in headers:
                mapping[key] = headers[header]
        if mapping:
            columns[offset] = mapping
    return columns


def _ordered_period_offsets(period_columns: dict[int, dict[str, int]]) -> list[int]:
    return sorted(period_columns, key=lambda offset: min(period_columns[offset].values()))


def _normalize_summary_sheet(workbook: Any) -> dict[str, Any]:
    if SUMMARY_SHEET not in workbook.sheetnames:
        return {}
    summary: dict[str, Any] = {}
    ws = workbook[SUMMARY_SHEET]
    for row in ws.iter_rows(values_only=True):
        if not row:
            continue
        key = _clean_text(row[0] if len(row) > 0 else "")
        if not key or key in EXCLUDED_SUMMARY_KEYS:
            continue
        value = row[1] if len(row) > 1 else None
        if _is_blank(value):
            continue
        summary[key] = _json_value(value)
    return summary


def _base_row_values(values: tuple[Any, ...], headers: dict[str, int]) -> dict[str, Any]:
    fields = [
        ("security_code", HEADER_SECURITY_CODE),
        ("company_name", HEADER_COMPANY_NAME),
        ("industry", HEADER_INDUSTRY),
        ("market", HEADER_MARKET),
        ("decision_label", HEADER_DECISION),
        ("row_kind", HEADER_ROW_KIND),
        ("current_period_end", HEADER_CURRENT_PERIOD_END),
        ("metric_label", HEADER_METRIC),
    ]
    row: dict[str, Any] = {}
    for key, header in fields:
        value = _value_at(values, headers.get(header))
        if not _is_blank(value):
            row[key] = _json_value(value)
    return row


def _period_values(
    values: tuple[Any, ...],
    period_columns: dict[int, dict[str, int]],
) -> list[dict[str, Any]]:
    periods: list[dict[str, Any]] = []
    for offset in _ordered_period_offsets(period_columns):
        columns = period_columns[offset]
        item: dict[str, Any] = {
            "offset": offset,
            "label": PERIOD_LABEL_BY_OFFSET[offset],
        }
        for key in ["period", "value", "unit", "ratio", "rank"]:
            value = _value_at(values, columns.get(key))
            if not _is_blank(value):
                item[key] = _json_value(value)
        if len(item) > 2:
            periods.append(item)
    return periods


def _normalize_metric_sheet(sheet_name: str, ws: Any) -> dict[str, Any] | None:
    iterator = ws.iter_rows(values_only=True)
    try:
        header_row = next(iterator)
    except StopIteration:
        return None
    headers = _header_map(header_row)
    if not REQUIRED_METRIC_HEADERS.issubset(headers):
        return None
    period_columns = _period_columns(headers)
    rows: list[dict[str, Any]] = []
    for values in iterator:
        row = _base_row_values(values, headers)
        if not row.get("metric_label") and not row.get("row_kind"):
            continue
        periods = _period_values(values, period_columns)
        if periods:
            row["periods"] = periods
        rows.append(row)
    return {
        "sheet_name": sheet_name,
        "rows": rows,
    }


def normalize_metric_excel_workbook(excel_path: str | Path) -> dict[str, Any]:
    path = Path(excel_path)
    workbook = load_workbook(path, data_only=True, read_only=True)
    try:
        sheets: list[dict[str, Any]] = []
        for sheet_name in workbook.sheetnames:
            if sheet_name in {SUMMARY_SHEET, CONDITION_SHEET}:
                continue
            normalized = _normalize_metric_sheet(sheet_name, workbook[sheet_name])
            if normalized is not None:
                sheets.append(normalized)
        return {
            "format_version": 1,
            "source_excel_name": path.name,
            "summary": _normalize_summary_sheet(workbook),
            "sheets": sheets,
        }
    finally:
        workbook.close()


def default_normalized_json_path(excel_path: str | Path, output_dir: str | Path) -> Path:
    path = Path(excel_path)
    return Path(output_dir) / f"{path.stem}.normalized.json"


def write_metric_excel_normalized_json(
    excel_path: str | Path,
    *,
    output_path: str | Path | None = None,
    output_dir: str | Path = DEFAULT_GOLDEN_MASTER_DIR,
) -> MetricExcelNormalizedResult:
    normalized = normalize_metric_excel_workbook(excel_path)
    destination = Path(output_path) if output_path is not None else default_normalized_json_path(
        excel_path,
        output_dir,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    row_count = sum(len(sheet.get("rows", [])) for sheet in normalized.get("sheets", []))
    return MetricExcelNormalizedResult(
        excel_path=Path(excel_path),
        output_path=destination,
        sheet_count=len(normalized.get("sheets", [])),
        row_count=row_count,
    )
