from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.metric_audit_service import (
    build_calculation_consistency_report,
    build_metric_audit_report,
    build_metric_audit_rows,
    build_unit_validation_report,
    check_calculation_consistency,
    fetch_raw_fact_audit_rows,
    now_stamp,
    validate_unit_decimals,
    write_text_report,
)
from edinet_monitor.services.normalization_impact_service import (
    build_impact_text_report,
    build_normalization_impact_preview,
    fetch_preview_scope_filings,
    write_impact_tsv,
)
from edinet_pipeline.domain.metric_labels import split_metric_key


DEFAULT_OUTPUT_DIR = str(Path("D:/") / "\u4f5c\u696d\u7528" / "normalization_change_audit")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run read-only audit reports for a small normalization-change scope. "
            "No DB updates are performed."
        )
    )
    parser.add_argument("--doc-id", action="append", dest="doc_ids", default=[])
    parser.add_argument("--security-code", action="append", dest="security_codes", default=[])
    parser.add_argument("--industry-33", action="append", dest="industry_33_list", default=[])
    parser.add_argument("--latest-only", action="store_true")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--allow-all", action="store_true")
    parser.add_argument("--metric-base", action="append", dest="metric_bases", default=[])
    parser.add_argument("--max-metric-bases", type=int, default=20)
    parser.add_argument("--enable-period-fallback", action="store_true")
    parser.add_argument("--enforce-candidate-validation", action="store_true")
    parser.add_argument("--include-unchanged", action="store_true")
    parser.add_argument("--all-periods", action="store_true")
    parser.add_argument("--skip-unit-validation", action="store_true")
    parser.add_argument("--skip-calculation-consistency", action="store_true")
    parser.add_argument("--tolerance-ratio", type=float, default=0.01)
    parser.add_argument("--tolerance-abs", type=float, default=1.0)
    parser.add_argument("--calculation-limit", type=int, default=100)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--db-path", default=str(DB_PATH))
    return parser


def _clean_list(values: list[str]) -> list[str]:
    return [str(value).strip() for value in values if str(value).strip()]


def validate_args(args: argparse.Namespace) -> None:
    has_filter = bool(args.doc_ids or args.security_codes or args.industry_33_list)
    if has_filter:
        return
    if not args.allow_all:
        raise SystemExit("Specify --doc-id, --security-code, or --industry-33. Use --allow-all only with a small --limit.")
    if args.limit <= 0:
        raise SystemExit("--allow-all requires --limit > 0")


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def _scope_description(args: argparse.Namespace) -> str:
    parts = []
    if args.doc_ids:
        parts.append(f"doc_id={','.join(args.doc_ids)}")
    if args.security_codes:
        parts.append(f"security_code={','.join(args.security_codes)}")
    if args.industry_33_list:
        parts.append(f"industry_33={','.join(args.industry_33_list)}")
    if args.latest_only:
        parts.append("latest_only=1")
    parts.append(f"limit={args.limit}")
    return " | ".join(parts)


def metric_bases_from_preview_rows(rows: list[dict[str, Any]]) -> list[str]:
    bases: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if str(row.get("metric_source") or "") != "normalized_metrics":
            continue
        if str(row.get("change_type") or "") == "unchanged":
            continue
        metric_key = str(row.get("metric_key") or "")
        if not metric_key:
            continue
        metric_base = split_metric_key(metric_key)[0]
        if metric_base and metric_base not in seen:
            seen.add(metric_base)
            bases.append(metric_base)
    return bases


def _safe_name(value: Any) -> str:
    text = str(value or "").strip()
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in text) or "unknown"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    args = build_arg_parser().parse_args()
    args.doc_ids = _clean_list(args.doc_ids)
    args.security_codes = _clean_list(args.security_codes)
    args.industry_33_list = _clean_list(args.industry_33_list)
    args.metric_bases = _clean_list(args.metric_bases)
    validate_args(args)

    timestamp = now_stamp()
    run_dir = Path(args.output_dir) / f"normalization_change_audit_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    preview_txt_path = run_dir / f"normalization_impact_{timestamp}.txt"
    preview_tsv_path = run_dir / f"normalization_impact_{timestamp}.tsv"
    manifest_path = run_dir / "audit_manifest.json"

    conn = _connect_readonly(Path(args.db_path))
    try:
        filings = fetch_preview_scope_filings(
            conn,
            doc_ids=args.doc_ids,
            industry_33_list=args.industry_33_list,
            security_codes=args.security_codes,
            latest_only=args.latest_only,
            limit=args.limit,
        )
        diff_rows, summary = build_normalization_impact_preview(
            conn,
            filings=filings,
            enable_period_fallback=args.enable_period_fallback,
            enforce_candidate_validation=args.enforce_candidate_validation,
            include_unchanged=args.include_unchanged,
            include_derived=True,
        )
        metric_bases = args.metric_bases or metric_bases_from_preview_rows(diff_rows)
        metric_bases = metric_bases[: max(args.max_metric_bases, 0)]

        write_impact_tsv(preview_tsv_path, diff_rows)
        preview_lines = build_impact_text_report(
            rows=diff_rows,
            summary=summary,
            enable_period_fallback=args.enable_period_fallback,
            enforce_candidate_validation=args.enforce_candidate_validation,
            include_unchanged=args.include_unchanged,
            include_derived=True,
            scope_description=_scope_description(args),
            tsv_path=preview_tsv_path,
        )
        preview_txt_path.write_text("\n".join(preview_lines) + "\n", encoding="utf-8-sig")

        audit_entries: list[dict[str, Any]] = []
        for filing in filings:
            doc_id = str(filing.get("doc_id") or "")
            doc_dir = run_dir / f"{_safe_name(filing.get('security_code'))}_{_safe_name(doc_id)}"
            raw_rows = fetch_raw_fact_audit_rows(conn, doc_id)
            doc_entry: dict[str, Any] = {
                "doc_id": doc_id,
                "security_code": filing.get("security_code", ""),
                "company_name": filing.get("company_name", ""),
                "period_end": filing.get("period_end", ""),
                "raw_fact_rows": len(raw_rows),
                "metric_audits": [],
            }

            for metric_base in metric_bases:
                candidates, selected = build_metric_audit_rows(
                    filing=filing,
                    raw_rows=raw_rows,
                    metric_base=metric_base,
                    enable_period_fallback=args.enable_period_fallback,
                    enforce_candidate_validation=args.enforce_candidate_validation,
                )
                lines = build_metric_audit_report(
                    filing=filing,
                    candidates=candidates,
                    selected=selected,
                    metric_base=metric_base,
                    all_periods=args.all_periods,
                )
                output_path = doc_dir / f"metric_selection_audit_{_safe_name(metric_base)}.txt"
                write_text_report(output_path, lines)
                doc_entry["metric_audits"].append(
                    {
                        "metric_base": metric_base,
                        "path": str(output_path),
                        "candidate_count": len(candidates),
                        "selected_count": len(selected),
                    }
                )

            if not args.skip_unit_validation:
                unit_rows = validate_unit_decimals(
                    filing=filing,
                    raw_rows=raw_rows,
                    all_periods=args.all_periods,
                )
                unit_path = doc_dir / "unit_decimals_validation.txt"
                write_text_report(
                    unit_path,
                    build_unit_validation_report(filing=filing, rows=unit_rows),
                )
                doc_entry["unit_validation"] = {
                    "path": str(unit_path),
                    "rows": len(unit_rows),
                    "warnings": sum(1 for row in unit_rows if row.get("status") == "WARNING"),
                    "infos": sum(1 for row in unit_rows if row.get("status") == "INFO"),
                }

            if not args.skip_calculation_consistency:
                calc_rows = check_calculation_consistency(
                    filing=filing,
                    raw_rows=raw_rows,
                    tolerance_ratio=args.tolerance_ratio,
                    tolerance_abs=args.tolerance_abs,
                    limit=args.calculation_limit,
                )
                calc_path = doc_dir / "calculation_consistency.txt"
                write_text_report(
                    calc_path,
                    build_calculation_consistency_report(
                        filing=filing,
                        rows=calc_rows,
                        tolerance_ratio=args.tolerance_ratio,
                        tolerance_abs=args.tolerance_abs,
                    ),
                )
                doc_entry["calculation_consistency"] = {
                    "path": str(calc_path),
                    "rows": len(calc_rows),
                    "warnings": sum(1 for row in calc_rows if row.get("status") == "WARNING"),
                    "skipped": sum(1 for row in calc_rows if row.get("status") == "SKIPPED"),
                }

            audit_entries.append(doc_entry)
    finally:
        conn.close()

    manifest = {
        "timestamp": timestamp,
        "db_path": str(args.db_path),
        "scope": {
            "doc_ids": args.doc_ids,
            "security_codes": args.security_codes,
            "industry_33_list": args.industry_33_list,
            "latest_only": args.latest_only,
            "limit": args.limit,
            "allow_all": args.allow_all,
        },
        "options": {
            "enable_period_fallback": args.enable_period_fallback,
            "enforce_candidate_validation": args.enforce_candidate_validation,
            "include_unchanged": args.include_unchanged,
            "all_periods": args.all_periods,
            "metric_bases": metric_bases,
        },
        "preview": {
            "txt_path": str(preview_txt_path),
            "tsv_path": str(preview_tsv_path),
            "summary": summary,
        },
        "filings": audit_entries,
    }
    _write_json(manifest_path, manifest)

    unit_warnings = sum(
        int(entry.get("unit_validation", {}).get("warnings", 0))
        for entry in audit_entries
    )
    calc_warnings = sum(
        int(entry.get("calculation_consistency", {}).get("warnings", 0))
        for entry in audit_entries
    )
    print(f"run_dir={run_dir}")
    print(f"manifest={manifest_path}")
    print(f"target_docs={summary.get('target_docs', 0)}")
    print(f"preview_changed={summary.get('changed', 0)}")
    print(f"preview_added={summary.get('added', 0)}")
    print(f"preview_removed={summary.get('removed', 0)}")
    print(f"metric_bases={','.join(metric_bases)}")
    print(f"unit_warnings={unit_warnings}")
    print(f"calculation_warnings={calc_warnings}")


if __name__ == "__main__":
    main()
