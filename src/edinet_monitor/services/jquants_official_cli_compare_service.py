from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
import csv
import json
import shutil
import sqlite3
import subprocess
from typing import Any, Callable

from edinet_monitor.config.settings import OPERATION_LOG_ROOT, PROJECT_ROOT
from edinet_monitor.services.jquants.mapper import normalize_security_code


SUMMARY_COMPARE_FIELDS = (
    "DiscDate",
    "DiscTime",
    "Code",
    "DiscNo",
    "DocType",
    "Sales",
    "OP",
    "OdP",
    "NP",
    "EPS",
    "BPS",
    "TA",
    "Eq",
    "CFO",
    "CFI",
    "CFF",
    "CashEq",
    "ShOutFY",
    "TrShFY",
)
DAILY_COMPARE_FIELDS = (
    "Date",
    "Code",
    "O",
    "H",
    "L",
    "C",
    "Vo",
    "Va",
    "AdjFactor",
    "AdjO",
    "AdjH",
    "AdjL",
    "AdjC",
    "AdjVo",
)
SUPPORTED_ENDPOINTS = {"fins.summary", "eq.daily"}


@dataclass(frozen=True)
class OfficialCliFieldDiff:
    endpoint: str
    row_key: str
    field_name: str
    official_value: str
    db_value: str


@dataclass(frozen=True)
class OfficialCliCompareResult:
    endpoint: str
    command: list[str]
    official_rows: int
    db_rows: int
    matched_rows: int
    missing_in_db: int
    extra_in_db: int
    field_diff_rows: int
    diff_count: int
    txt_path: Path
    tsv_path: Path


Runner = Callable[[list[str]], subprocess.CompletedProcess[str]]


class OfficialCliCompareError(RuntimeError):
    pass


def run_jquants_official_cli_compare(
    conn: sqlite3.Connection,
    *,
    endpoint: str,
    date_value: str | None = None,
    code: str | None = None,
    output_dir: str | Path = OPERATION_LOG_ROOT,
    official_cli: str | None = None,
    runner: Runner | None = None,
) -> OfficialCliCompareResult:
    endpoint = endpoint.strip().lower()
    if endpoint == "fins.details":
        raise OfficialCliCompareError(
            "fins.details is Premium-only in the official jquants-cli plan matrix and is intentionally unsupported for Standard-plan checks."
        )
    if endpoint not in SUPPORTED_ENDPOINTS:
        raise OfficialCliCompareError(f"Unsupported endpoint: {endpoint}. Supported: {', '.join(sorted(SUPPORTED_ENDPOINTS))}")
    if not date_value and not code:
        raise OfficialCliCompareError("Specify at least one of date_value or code.")

    command = _build_official_cli_command(
        endpoint=endpoint,
        date_value=date_value,
        code=code,
        official_cli=official_cli,
    )
    completed = (runner or _default_runner)(command)
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise OfficialCliCompareError(f"official jquants-cli failed with exit_code={completed.returncode}: {stderr}")

    official_rows = _json_rows(completed.stdout)
    db_rows = _load_db_rows(conn, endpoint=endpoint, date_value=date_value, code=code)
    compare_fields = SUMMARY_COMPARE_FIELDS if endpoint == "fins.summary" else DAILY_COMPARE_FIELDS

    missing = []
    diffs: list[OfficialCliFieldDiff] = []
    matched = 0
    for key, official in official_rows.items():
        db_row = db_rows.get(key)
        if db_row is None:
            missing.append(key)
            continue
        matched += 1
        for field_name in compare_fields:
            official_value = official.get(field_name)
            db_value = db_row.get(field_name)
            if not _same_value(official_value, db_value):
                diffs.append(
                    OfficialCliFieldDiff(
                        endpoint=endpoint,
                        row_key=key,
                        field_name=field_name,
                        official_value=_display_value(official_value),
                        db_value=_display_value(db_value),
                    )
                )

    extra = sorted(set(db_rows) - set(official_rows))
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_endpoint = endpoint.replace(".", "_")
    txt_path = output_root / f"jquants_official_cli_compare_{safe_endpoint}_{timestamp}.txt"
    tsv_path = output_root / f"jquants_official_cli_compare_{safe_endpoint}_{timestamp}.tsv"
    _write_reports(
        txt_path=txt_path,
        tsv_path=tsv_path,
        endpoint=endpoint,
        command=command,
        official_count=len(official_rows),
        db_count=len(db_rows),
        matched=matched,
        missing=missing,
        extra=extra,
        diffs=diffs,
    )
    return OfficialCliCompareResult(
        endpoint=endpoint,
        command=command,
        official_rows=len(official_rows),
        db_rows=len(db_rows),
        matched_rows=matched,
        missing_in_db=len(missing),
        extra_in_db=len(extra),
        field_diff_rows=len({diff.row_key for diff in diffs}),
        diff_count=len(diffs),
        txt_path=txt_path,
        tsv_path=tsv_path,
    )


def _default_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def _build_official_cli_command(
    *,
    endpoint: str,
    date_value: str | None,
    code: str | None,
    official_cli: str | None,
) -> list[str]:
    executable = _resolve_official_cli(official_cli)
    command = [executable, "--output", "json"]
    if endpoint == "fins.summary":
        command.extend(["fins", "summary"])
    elif endpoint == "eq.daily":
        command.extend(["eq", "daily"])
    if date_value:
        command.extend(["--date", date_value])
    if code:
        command.extend(["--code", _official_code(code)])
    return command


def _resolve_official_cli(official_cli: str | None) -> str:
    if official_cli:
        return official_cli
    found = shutil.which("jquants.exe") or shutil.which("jquants")
    if found:
        return found
    fallback = PROJECT_ROOT / "tools" / "jquants-cli" / "jquants.exe"
    if fallback.exists():
        return str(fallback)
    raise OfficialCliCompareError("jquants executable was not found in PATH or tools/jquants-cli/jquants.exe.")


def _official_code(code: str) -> str:
    text = str(code or "").strip()
    if len(text) == 4 and text.isdigit():
        return f"{text}0"
    return text


def _json_rows(stdout: str) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise OfficialCliCompareError(f"official jquants-cli did not return valid JSON: {exc}") from exc
    if isinstance(payload, dict):
        for key in ("data", "fin_summary", "summary", "eq_bars_daily", "bars"):
            value = payload.get(key)
            if isinstance(value, list):
                payload = value
                break
    if not isinstance(payload, list):
        raise OfficialCliCompareError("official jquants-cli JSON must be a list or a dict containing a list.")
    rows: dict[str, dict[str, Any]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        key = _official_row_key(row)
        if key:
            rows[key] = row
    return rows


def _official_row_key(row: dict[str, Any]) -> str:
    disclosure = str(row.get("DiscNo") or row.get("DisclosureNumber") or "").strip()
    if disclosure:
        return disclosure
    date_value = _normalize_date(row.get("Date"))
    code = _official_code(str(row.get("Code") or "").strip())
    if date_value and code:
        return f"{date_value}|{code}"
    return ""


def _load_db_rows(
    conn: sqlite3.Connection,
    *,
    endpoint: str,
    date_value: str | None,
    code: str | None,
) -> dict[str, dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    if endpoint == "fins.summary":
        return _load_statement_raw_rows(conn, date_value=date_value, code=code)
    return _load_daily_quote_rows(conn, date_value=date_value, code=code)


def _load_statement_raw_rows(
    conn: sqlite3.Connection,
    *,
    date_value: str | None,
    code: str | None,
) -> dict[str, dict[str, Any]]:
    where = []
    params: list[Any] = []
    if date_value:
        where.append("disclosed_date = ?")
        params.append(_normalize_date(date_value))
    code_values = _db_code_values(code)
    if code_values:
        placeholders = ",".join("?" for _ in code_values)
        where.append(f"(local_code IN ({placeholders}) OR security_code IN ({placeholders}))")
        params.extend(code_values)
        params.extend(code_values)
    sql = "SELECT disclosure_number, raw_json FROM jquants_statement_raw"
    if where:
        sql += " WHERE " + " AND ".join(where)
    rows = {}
    for row in conn.execute(sql, params).fetchall():
        raw = _safe_json(row["raw_json"])
        key = str(raw.get("DiscNo") or row["disclosure_number"] or "").strip()
        if key:
            rows[key] = raw
    return rows


def _load_daily_quote_rows(
    conn: sqlite3.Connection,
    *,
    date_value: str | None,
    code: str | None,
) -> dict[str, dict[str, Any]]:
    where = []
    params: list[Any] = []
    if date_value:
        where.append("trade_date = ?")
        params.append(_normalize_date(date_value))
    code_values = _db_code_values(code)
    if code_values:
        placeholders = ",".join("?" for _ in code_values)
        where.append(f"(local_code IN ({placeholders}) OR security_code IN ({placeholders}))")
        params.extend(code_values)
        params.extend(code_values)
    sql = "SELECT local_code, trade_date, raw_json FROM jquants_daily_quotes"
    if where:
        sql += " WHERE " + " AND ".join(where)
    rows = {}
    for row in conn.execute(sql, params).fetchall():
        raw = _safe_json(row["raw_json"])
        key = _official_row_key(
            {
                "Date": raw.get("Date") or row["trade_date"],
                "Code": raw.get("Code") or row["local_code"],
            }
        )
        if key:
            rows[key] = raw
    return rows


def _db_code_values(code: str | None) -> list[str]:
    text = str(code or "").strip()
    if not text:
        return []
    normalized = normalize_security_code(text)
    values = {text, _official_code(text)}
    if normalized:
        values.add(normalized)
    return sorted(values)


def _safe_json(text: Any) -> dict[str, Any]:
    if not text:
        return {}
    try:
        value = json.loads(str(text))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _normalize_date(value: Any) -> str:
    text = str(value or "").strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _same_value(left: Any, right: Any) -> bool:
    if left in (None, "") and right in (None, ""):
        return True
    left_decimal = _decimal_or_none(left)
    right_decimal = _decimal_or_none(right)
    if left_decimal is not None and right_decimal is not None:
        return left_decimal == right_decimal
    return str(left).strip() == str(right).strip()


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _display_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _write_reports(
    *,
    txt_path: Path,
    tsv_path: Path,
    endpoint: str,
    command: list[str],
    official_count: int,
    db_count: int,
    matched: int,
    missing: list[str],
    extra: list[str],
    diffs: list[OfficialCliFieldDiff],
) -> None:
    txt_path.write_text(
        "\n".join(
            [
                "J-Quants official CLI compare",
                f"endpoint={endpoint}",
                f"command={' '.join(command)}",
                f"official_rows={official_count}",
                f"db_rows={db_count}",
                f"matched_rows={matched}",
                f"missing_in_db={len(missing)}",
                f"extra_in_db={len(extra)}",
                f"field_diff_rows={len({diff.row_key for diff in diffs})}",
                f"diff_count={len(diffs)}",
                f"missing_keys={','.join(missing[:50])}",
                f"extra_keys={','.join(extra[:50])}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    with tsv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["endpoint", "row_key", "field_name", "official_value", "db_value"],
            delimiter="\t",
        )
        writer.writeheader()
        for diff in diffs:
            writer.writerow(
                {
                    "endpoint": diff.endpoint,
                    "row_key": diff.row_key,
                    "field_name": diff.field_name,
                    "official_value": diff.official_value,
                    "db_value": diff.db_value,
                }
            )
