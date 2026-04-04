from __future__ import annotations

from typing import Any, NotRequired, TypedDict


class XbrlFilePaths(TypedDict):
    file1: list[str]
    file2: list[str]
    file3: list[str]


class LoopInput(TypedDict):
    slot: int | None
    company_code: str | None
    company_name: str | None
    has_half: bool | None
    source_zips: list[str]
    output_root: str | None
    xbrl_file_paths: XbrlFilePaths
    excel_file_path: NotRequired[str]
    final_excel_file_path: NotRequired[str]


class PhaseResult(TypedDict):
    ok: bool
    sec: float | None


class LoopEvent(TypedDict):
    ts: str
    run_id: str
    slot: int | None
    excel: str | None
    security_code: str | None
    company_code: str | None
    company_name: str | None
    has_half: bool | None
    source_zips: list[str]
    phases: dict[str, PhaseResult]
    counts: dict[str, int]
    errors: list[str]
    accounting_standard: NotRequired[str | None]
    missing_named_ranges: NotRequired[list[str]]


class ProcessLoopResult(TypedDict, total=False):
    slot: int | None
    company_code: str | None
    company_name: str | None
    status: str
    stock_status: str | None
    output_excel: str | None
    failure_reason: str | None
    error_detail: str | None


class ExcelPrepareStageResult(TypedDict):
    selected_file: str | None
    excel_file_path: str | None
    excel_base_name: str | None
    failed_result: ProcessLoopResult | None


class ParseStageResult(TypedDict):
    x1: dict[str, Any] | None
    x2: dict[str, Any] | None
    meta2: dict[str, Any] | None
    security_code: str | None
    base_year: int | None
    use_half: bool


class RuntimeFlags(TypedDict):
    write_raw_sheet: bool
    enable_stock: bool


class WorkbookStageResult(TypedDict):
    stock_status: str | None
