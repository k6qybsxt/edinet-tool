from __future__ import annotations

import argparse
import os
import sqlite3
import unicodedata
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

from edinet_monitor.config.settings import (
    DB_PATH,
    MANIFEST_ROOT,
    TSE_LISTING_MASTER_CSV_PATH,
    ZIP_ROOT,
)
from edinet_monitor.services.collector.document_filter_service import filter_target_filings
from edinet_monitor.services.collector.document_list_service import (
    DocumentListResult,
    fetch_document_list,
)
from edinet_monitor.services.collector.document_row_mapper import normalize_security_code
from edinet_monitor.services.collector.issuer_master_csv_service import load_allowed_edinet_codes
from edinet_monitor.services.collector.target_date_service import resolve_target_dates
from edinet_monitor.services.storage.manifest_service import read_manifest_rows


DEFAULT_OUTPUT_DIR = Path("D:/\u4f5c\u696d\u7528")
DEFAULT_MANIFEST_PREFIX = "document_manifest"
DEFAULT_MAX_DAYS = 370
STATUS_PRIORITY = {
    "pending": 0,
    "error": 1,
    "downloaded": 2,
}
ISSUE_PRIORITY = [
    "EDINET_ERROR",
    "EDINET_COUNT_MISMATCH",
    "MANIFEST_READ_ERROR",
    "MANIFEST_MISSING",
    "MANIFEST_INCOMPLETE",
    "ZIP_INVALID",
    "ZIP_MISSING",
    "FILING_MISSING",
    "MANIFEST_EXTRA",
]


@dataclass(frozen=True)
class CoverageOptions:
    target_dates: list[date]
    api_key: str
    output_dir: Path
    db_path: Path
    manifest_root: Path
    zip_root: Path
    master_csv_path: Path
    manifest_prefix: str
    scan_all_manifests: bool
    skip_edinet: bool
    validate_zip: bool
    max_sample_docs: int


def display_width(text: Any) -> int:
    width = 0
    for ch in str(text):
        width += 2 if unicodedata.east_asian_width(ch) in ("F", "W", "A") else 1
    return width


def pad_right(text: Any, width: int) -> str:
    text = str(text)
    return text + " " * max(0, width - display_width(text))


def pad_left(text: Any, width: int) -> str:
    text = str(text)
    return " " * max(0, width - display_width(text)) + text


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_from_env(env_name: str, default: int) -> int:
    value = os.getenv(env_name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _date_text(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) < 10:
        return ""
    candidate = text[:10]
    try:
        date.fromisoformat(candidate)
    except ValueError:
        return ""
    return candidate


def _manifest_row_date(row: dict[str, Any]) -> str:
    return _date_text(row.get("source_date")) or _date_text(row.get("submit_date"))


def _candidate_manifest_paths(manifest_root: Path, prefix: str, target_date: date) -> list[Path]:
    date_text = target_date.isoformat()
    month_text = date_text[:7]
    return [
        manifest_root / f"{prefix}_{date_text}.jsonl",
        manifest_root / f"{prefix}_{month_text}.jsonl",
    ]


def _status_priority(row: dict[str, Any]) -> tuple[int, str, str]:
    status = str(row.get("download_status") or "pending").strip() or "pending"
    return (
        STATUS_PRIORITY.get(status, 0),
        str(row.get("submit_date") or ""),
        str(row.get("source_date") or ""),
    )


def _merge_rows_by_doc_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        doc_id = str(row.get("doc_id") or "").strip()
        if not doc_id:
            continue
        existing = merged.get(doc_id)
        if existing is None or _status_priority(row) >= _status_priority(existing):
            merged[doc_id] = dict(row)
    return sorted(merged.values(), key=lambda row: (str(row.get("submit_date") or ""), str(row.get("doc_id") or "")))


def load_manifest_rows_by_date(
    *,
    target_dates: list[date],
    manifest_root: Path,
    manifest_prefix: str,
    scan_all_manifests: bool,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, set[Path]], dict[str, set[Path]], dict[Path, str]]:
    target_texts = {target_date.isoformat() for target_date in target_dates}
    rows_by_date: dict[str, list[dict[str, Any]]] = {target_text: [] for target_text in target_texts}
    files_with_rows_by_date: dict[str, set[Path]] = {target_text: set() for target_text in target_texts}
    candidate_files_by_date: dict[str, set[Path]] = {target_text: set() for target_text in target_texts}
    read_errors: dict[Path, str] = {}

    candidate_paths: set[Path] = set()
    for target_date in target_dates:
        target_text = target_date.isoformat()
        for path in _candidate_manifest_paths(manifest_root, manifest_prefix, target_date):
            if path.exists():
                candidate_files_by_date[target_text].add(path)
                candidate_paths.add(path)

    if scan_all_manifests:
        paths = sorted(Path(manifest_root).glob(f"{manifest_prefix}*.jsonl"))
    else:
        paths = sorted(candidate_paths)

    for path in paths:
        try:
            rows = read_manifest_rows(path)
        except Exception as exc:  # pragma: no cover - exact JSON/path errors vary by runtime.
            read_errors[path] = repr(exc)
            continue

        for row in rows:
            row_date = _manifest_row_date(row)
            if row_date not in target_texts:
                continue
            row_with_path = dict(row)
            row_with_path["_manifest_path"] = str(path)
            rows_by_date[row_date].append(row_with_path)
            files_with_rows_by_date[row_date].add(path)

    for target_text, rows in rows_by_date.items():
        rows_by_date[target_text] = _merge_rows_by_doc_id(rows)

    return rows_by_date, files_with_rows_by_date, candidate_files_by_date, read_errors


def _build_zip_path(zip_root: Path, submit_date: str, doc_id: str) -> str:
    date_part = _date_text(submit_date) or "unknown_date"
    return str(zip_root / date_part / f"{doc_id}.zip")


def _expected_record_from_edinet_row(row: dict[str, Any], zip_root: Path) -> dict[str, Any]:
    doc_id = str(row.get("docID") or "").strip()
    submit_date = str(row.get("submitDateTime") or "").strip()
    return {
        "doc_id": doc_id,
        "edinet_code": str(row.get("edinetCode") or "").strip(),
        "security_code": normalize_security_code(row.get("secCode")),
        "company_name": str(row.get("filerName") or "").strip(),
        "doc_description": str(row.get("docDescription") or "").strip(),
        "form_code": str(row.get("formCode") or "").strip(),
        "doc_type_code": str(row.get("docTypeCode") or "").strip(),
        "ordinance_code": str(row.get("ordinanceCode") or "").strip(),
        "period_end": str(row.get("periodEnd") or "").strip(),
        "submit_date": submit_date,
        "source_date": "",
        "zip_path": _build_zip_path(zip_root, submit_date, doc_id),
    }


def fetch_expected_records_for_date(
    target_date: date,
    *,
    api_key: str,
    allowed_edinet_codes: set[str],
    zip_root: Path,
    fetcher: Callable[..., DocumentListResult] = fetch_document_list,
) -> dict[str, Any]:
    result = fetcher(target_date=target_date, api_key=api_key, list_type=2)
    filtered_rows = filter_target_filings(result.results)
    issuer_rows = [
        row
        for row in filtered_rows
        if str(row.get("edinetCode") or "").strip() in allowed_edinet_codes
    ]
    expected_records = [
        _expected_record_from_edinet_row(row, zip_root)
        for row in issuer_rows
        if str(row.get("docID") or "").strip()
    ]
    resultset_count = _to_int(result.metadata.get("resultset_count"))
    return {
        "metadata": result.metadata,
        "resultset_count": resultset_count,
        "all_results": len(result.results),
        "target_results": len(filtered_rows),
        "issuer_target_results": len(expected_records),
        "expected_records": expected_records,
    }


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_filings_by_doc_id(conn: sqlite3.Connection, doc_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not doc_ids:
        return {}

    out: dict[str, dict[str, Any]] = {}
    for start in range(0, len(doc_ids), 500):
        chunk = doc_ids[start:start + 500]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT
                doc_id,
                COALESCE(download_status, '') AS download_status,
                COALESCE(parse_status, '') AS parse_status,
                COALESCE(zip_path, '') AS zip_path
            FROM filings
            WHERE doc_id IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        out.update({str(row["doc_id"]): dict(row) for row in rows})
    return out


def _zip_path_for_check(record: dict[str, Any], zip_root: Path) -> str:
    zip_path = str(record.get("zip_path") or "").strip()
    if zip_path:
        return zip_path
    return _build_zip_path(
        zip_root,
        str(record.get("submit_date") or ""),
        str(record.get("doc_id") or ""),
    )


def _check_zip_counts(records: list[dict[str, Any]], *, zip_root: Path, validate_zip: bool) -> dict[str, Any]:
    ok = 0
    missing = 0
    invalid = 0
    missing_doc_ids: list[str] = []
    invalid_doc_ids: list[str] = []

    for record in records:
        doc_id = str(record.get("doc_id") or "").strip()
        path = Path(_zip_path_for_check(record, zip_root))
        if not path.is_file():
            missing += 1
            if doc_id:
                missing_doc_ids.append(doc_id)
            continue

        if validate_zip and not zipfile.is_zipfile(path):
            invalid += 1
            if doc_id:
                invalid_doc_ids.append(doc_id)
            continue

        ok += 1

    return {
        "zip_ok": ok,
        "zip_missing": missing,
        "zip_invalid": invalid,
        "zip_missing_doc_ids": missing_doc_ids,
        "zip_invalid_doc_ids": invalid_doc_ids,
    }


def _manifest_file_names(paths: set[Path]) -> str:
    if not paths:
        return ""
    names = [path.name for path in sorted(paths)]
    if len(names) <= 3:
        return ",".join(names)
    return ",".join(names[:3]) + f"...(+{len(names) - 3})"


def _choose_status(issues: list[str], expected_count: int, *, skip_edinet: bool) -> str:
    if not issues and expected_count == 0:
        if skip_edinet:
            return "NO_LOCAL_ROWS"
        return "NO_TARGET_DOCS"
    if not issues:
        return "OK"
    for issue in ISSUE_PRIORITY:
        if issue in issues:
            return issue
    return issues[0]


def _unique_sample_doc_ids(*groups: list[str], limit: int) -> str:
    seen: set[str] = set()
    out: list[str] = []
    for group in groups:
        for doc_id in group:
            text = str(doc_id or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
            if len(out) >= limit:
                return ",".join(out)
    return ",".join(out)


def build_daily_coverage_rows(
    options: CoverageOptions,
    *,
    allowed_edinet_codes: set[str],
    fetcher: Callable[..., DocumentListResult] = fetch_document_list,
) -> list[dict[str, Any]]:
    rows_by_date, files_by_date, candidate_files_by_date, manifest_read_errors = load_manifest_rows_by_date(
        target_dates=options.target_dates,
        manifest_root=options.manifest_root,
        manifest_prefix=options.manifest_prefix,
        scan_all_manifests=options.scan_all_manifests,
    )

    conn = _connect_readonly(options.db_path)
    try:
        out_rows: list[dict[str, Any]] = []
        for target_date in options.target_dates:
            target_text = target_date.isoformat()
            manifest_rows = rows_by_date.get(target_text, [])
            manifest_by_doc_id = {str(row.get("doc_id") or ""): row for row in manifest_rows}
            api_status = ""
            api_message = ""
            api_count: int | None = None
            all_results = 0
            target_results = 0
            expected_records: list[dict[str, Any]] = []
            issues: list[str] = []
            edinet_error = ""

            if options.skip_edinet:
                expected_records = manifest_rows
                api_status = "SKIP"
            else:
                try:
                    expected = fetch_expected_records_for_date(
                        target_date,
                        api_key=options.api_key,
                        allowed_edinet_codes=allowed_edinet_codes,
                        zip_root=options.zip_root,
                        fetcher=fetcher,
                    )
                    metadata = expected["metadata"]
                    api_status = str(metadata.get("status") or "")
                    api_message = str(metadata.get("message") or "")
                    api_count = expected["resultset_count"]
                    all_results = int(expected["all_results"])
                    target_results = int(expected["target_results"])
                    expected_records = list(expected["expected_records"])
                    if api_status and api_status != "200":
                        issues.append("EDINET_ERROR")
                    if api_count is not None and api_count != all_results:
                        issues.append("EDINET_COUNT_MISMATCH")
                except Exception as exc:
                    issues.append("EDINET_ERROR")
                    edinet_error = repr(exc)
                    expected_records = manifest_rows

            expected_by_doc_id = {
                str(row.get("doc_id") or "").strip(): row
                for row in expected_records
                if str(row.get("doc_id") or "").strip()
            }
            expected_doc_ids = sorted(expected_by_doc_id)
            manifest_doc_ids = set(manifest_by_doc_id)
            missing_manifest_doc_ids = sorted(set(expected_doc_ids) - manifest_doc_ids)
            extra_manifest_doc_ids = sorted(manifest_doc_ids - set(expected_doc_ids)) if not options.skip_edinet else []

            date_candidate_files = candidate_files_by_date.get(target_text, set())
            date_files_with_rows = files_by_date.get(target_text, set())
            date_read_errors = [
                path
                for path in date_candidate_files
                if path in manifest_read_errors
            ]

            if date_read_errors:
                issues.append("MANIFEST_READ_ERROR")
            if expected_doc_ids:
                if not date_candidate_files and not date_files_with_rows:
                    issues.append("MANIFEST_MISSING")
                elif missing_manifest_doc_ids:
                    issues.append("MANIFEST_INCOMPLETE")
            if extra_manifest_doc_ids:
                issues.append("MANIFEST_EXTRA")

            zip_counts = _check_zip_counts(
                list(expected_by_doc_id.values()),
                zip_root=options.zip_root,
                validate_zip=options.validate_zip,
            )
            if zip_counts["zip_invalid"]:
                issues.append("ZIP_INVALID")
            if zip_counts["zip_missing"]:
                issues.append("ZIP_MISSING")

            filing_by_doc_id = fetch_filings_by_doc_id(conn, expected_doc_ids)
            filing_missing_doc_ids = sorted(set(expected_doc_ids) - set(filing_by_doc_id))
            if filing_missing_doc_ids:
                issues.append("FILING_MISSING")

            filing_downloaded = sum(
                1
                for row in filing_by_doc_id.values()
                if str(row.get("download_status") or "") == "downloaded"
            )

            sample_doc_ids = _unique_sample_doc_ids(
                missing_manifest_doc_ids,
                zip_counts["zip_invalid_doc_ids"],
                zip_counts["zip_missing_doc_ids"],
                filing_missing_doc_ids,
                extra_manifest_doc_ids,
                limit=options.max_sample_docs,
            )

            unique_issues = list(dict.fromkeys(issues))
            out_rows.append(
                {
                    "date": target_text,
                    "status": _choose_status(
                        unique_issues,
                        len(expected_doc_ids),
                        skip_edinet=options.skip_edinet,
                    ),
                    "issues": ",".join(unique_issues),
                    "api_status": api_status,
                    "api_count": "" if api_count is None else api_count,
                    "api_all": all_results,
                    "api_target": target_results,
                    "edinet_target": len(expected_doc_ids),
                    "manifest_rows": len(manifest_rows),
                    "manifest_missing": len(missing_manifest_doc_ids),
                    "manifest_extra": len(extra_manifest_doc_ids),
                    "zip_ok": zip_counts["zip_ok"],
                    "zip_missing": zip_counts["zip_missing"],
                    "zip_invalid": zip_counts["zip_invalid"],
                    "filings": len(filing_by_doc_id),
                    "filings_downloaded": filing_downloaded,
                    "filing_missing": len(filing_missing_doc_ids),
                    "manifest_files": _manifest_file_names(date_files_with_rows or date_candidate_files),
                    "sample_doc_ids": sample_doc_ids,
                    "api_message": api_message,
                    "edinet_error": edinet_error,
                }
            )
    finally:
        conn.close()

    return out_rows


def render_report(rows: list[dict[str, Any]], options: CoverageOptions) -> str:
    generated_at = datetime.now().isoformat(timespec="seconds")
    counts = Counter(str(row.get("status") or "") for row in rows)
    lines = [
        f"generated_at: {generated_at}",
        f"db_path: {options.db_path}",
        f"manifest_root: {options.manifest_root}",
        f"zip_root: {options.zip_root}",
        f"manifest_prefix: {options.manifest_prefix}",
        f"date_from: {options.target_dates[0].isoformat() if options.target_dates else ''}",
        f"date_to: {options.target_dates[-1].isoformat() if options.target_dates else ''}",
        f"days: {len(options.target_dates)}",
        f"scan_all_manifests: {'on' if options.scan_all_manifests else 'off'}",
        f"skip_edinet: {'on' if options.skip_edinet else 'off'}",
        f"validate_zip: {'on' if options.validate_zip else 'off'}",
        f"rows: {len(rows)}",
        "status_summary: " + (" | ".join(f"{key}={counts[key]}" for key in sorted(counts)) if counts else "none"),
        "",
    ]

    columns = [
        ("date", "日付", "left"),
        ("status", "状態", "left"),
        ("api_status", "API", "left"),
        ("api_count", "API件数", "right"),
        ("edinet_target", "EDINET対象", "right"),
        ("manifest_rows", "manifest", "right"),
        ("manifest_missing", "manifest不足", "right"),
        ("zip_ok", "ZIP有", "right"),
        ("zip_missing", "ZIP無", "right"),
        ("zip_invalid", "ZIP不正", "right"),
        ("filings", "DB反映", "right"),
        ("filing_missing", "DB不足", "right"),
        ("issues", "issues", "left"),
        ("manifest_files", "manifestファイル", "left"),
        ("sample_doc_ids", "sample_doc_ids", "left"),
    ]

    widths: dict[str, int] = {}
    for key, label, _ in columns:
        widths[key] = max([display_width(label)] + [display_width(row.get(key, "")) for row in rows])

    header_parts: list[str] = []
    separator_parts: list[str] = []
    for key, label, align in columns:
        header_parts.append(pad_left(label, widths[key]) if align == "right" else pad_right(label, widths[key]))
        separator_parts.append("-" * widths[key])

    lines.append(" | ".join(header_parts))
    lines.append("-+-".join(separator_parts))

    for row in rows:
        parts: list[str] = []
        for key, _, align in columns:
            value = row.get(key, "")
            parts.append(pad_left(value, widths[key]) if align == "right" else pad_right(value, widths[key]))
        lines.append(" | ".join(parts).rstrip())

    error_rows = [
        row
        for row in rows
        if row.get("edinet_error") or row.get("api_message")
    ][:10]
    if error_rows:
        lines.append("")
        lines.append("API messages / errors:")
        for row in error_rows:
            message = row.get("edinet_error") or row.get("api_message")
            lines.append(f"{row.get('date')}: {message}")

    lines.append("")
    return "\n".join(lines)


def _sanitize_filename_part(text: str) -> str:
    safe = []
    for ch in str(text or ""):
        if ch.isalnum() or ch in {"-", "_"}:
            safe.append(ch)
        elif ch in {",", " "}:
            safe.append("_")
    return "".join(safe).strip("_") or "all"


def build_output_path(options: CoverageOptions) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not options.target_dates:
        date_part = "none"
    elif len(options.target_dates) == 1:
        date_part = options.target_dates[0].isoformat()
    else:
        date_part = f"{options.target_dates[0].isoformat()}_to_{options.target_dates[-1].isoformat()}"
    filename = f"download_coverage_{_sanitize_filename_part(date_part)}_{timestamp}.txt"
    return options.output_dir / filename


def generate_report(
    options: CoverageOptions,
    *,
    allowed_edinet_codes: set[str] | None = None,
    fetcher: Callable[..., DocumentListResult] = fetch_document_list,
) -> tuple[Path, list[dict[str, Any]]]:
    allowed_codes = allowed_edinet_codes
    if allowed_codes is None:
        allowed_codes = load_allowed_edinet_codes(options.master_csv_path)

    rows = build_daily_coverage_rows(options, allowed_edinet_codes=allowed_codes, fetcher=fetcher)
    report_text = render_report(rows, options)
    output_path = build_output_path(options)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_text, encoding="utf-8-sig")
    return output_path, rows


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-date",
        default=os.getenv("EDINET_TARGET_DATE", "").strip(),
        help="Single EDINET file date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--date-from",
        default=os.getenv("EDINET_DATE_FROM", "").strip(),
        help="Start EDINET file date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--date-to",
        default=os.getenv("EDINET_DATE_TO", "").strip(),
        help="End EDINET file date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--manifest-prefix",
        default=os.getenv("EDINET_MANIFEST_PREFIX", DEFAULT_MANIFEST_PREFIX).strip() or DEFAULT_MANIFEST_PREFIX,
    )
    parser.add_argument(
        "--manifest-root",
        default=os.getenv("EDINET_MANIFEST_ROOT", str(MANIFEST_ROOT)),
    )
    parser.add_argument(
        "--zip-root",
        default=os.getenv("EDINET_ZIP_ROOT", str(ZIP_ROOT)),
    )
    parser.add_argument(
        "--master-csv-path",
        default=os.getenv("EDINET_TSE_MASTER_CSV", str(TSE_LISTING_MASTER_CSV_PATH)),
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", str(DB_PATH)),
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)),
    )
    parser.add_argument(
        "--scan-all-manifests",
        action="store_true",
        help="Scan all matching manifest JSONL files. Default checks only day/month candidate files.",
    )
    parser.add_argument(
        "--skip-edinet",
        action="store_true",
        help="Do local-only checks from manifest rows without calling EDINET API.",
    )
    parser.add_argument(
        "--validate-zip",
        action="store_true",
        help="Also validate existing files with zipfile.is_zipfile().",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=_int_from_env("DOWNLOAD_COVERAGE_MAX_DAYS", DEFAULT_MAX_DAYS),
        help="Safety limit for API calls unless --allow-large-range is set.",
    )
    parser.add_argument("--allow-large-range", action="store_true")
    parser.add_argument("--max-sample-docs", type=int, default=5)
    return parser


def parse_options(args: argparse.Namespace) -> CoverageOptions:
    target_dates = resolve_target_dates(
        target_date_text=args.target_date,
        date_from_text=args.date_from,
        date_to_text=args.date_to,
    )
    if len(target_dates) > int(args.max_days) and not args.allow_large_range:
        raise ValueError(
            f"Date range has {len(target_dates)} days. "
            f"Use --allow-large-range or raise --max-days if this is intentional."
        )

    api_key = os.getenv("EDINET_API_KEY", "").strip()
    if not args.skip_edinet and not api_key:
        raise RuntimeError("Set EDINET_API_KEY or use --skip-edinet for local-only checks.")

    return CoverageOptions(
        target_dates=target_dates,
        api_key=api_key,
        output_dir=Path(str(args.output_dir)),
        db_path=Path(str(args.db_path)),
        manifest_root=Path(str(args.manifest_root)),
        zip_root=Path(str(args.zip_root)),
        master_csv_path=Path(str(args.master_csv_path)),
        manifest_prefix=str(args.manifest_prefix or DEFAULT_MANIFEST_PREFIX).strip() or DEFAULT_MANIFEST_PREFIX,
        scan_all_manifests=bool(args.scan_all_manifests),
        skip_edinet=bool(args.skip_edinet),
        validate_zip=bool(args.validate_zip),
        max_sample_docs=max(int(args.max_sample_docs), 1),
    )


def main() -> None:
    options = parse_options(build_arg_parser().parse_args())
    output_path, rows = generate_report(options)
    counts = Counter(str(row.get("status") or "") for row in rows)
    print(f"saved: {output_path}")
    print(f"rows: {len(rows)}")
    print("status_summary: " + (" | ".join(f"{key}={counts[key]}" for key in sorted(counts)) if counts else "none"))


if __name__ == "__main__":
    main()
