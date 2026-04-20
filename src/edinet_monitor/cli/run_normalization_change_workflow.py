from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from edinet_monitor.cli.rebuild_metrics_for_scope import rebuild_metrics_for_scope
from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.metric_snapshot_service import (
    compare_metric_snapshots,
    export_metric_snapshot,
)
from edinet_monitor.services.normalization_impact_service import (
    build_impact_text_report,
    build_normalization_impact_preview,
    fetch_preview_scope_filings,
    now_stamp,
    write_impact_tsv,
)


DEFAULT_OUTPUT_DIR = str(Path("D:/") / "\u4f5c\u696d\u7528" / "normalization_change_runs")
DEFAULT_SNAPSHOT_OUTPUT_DIR = str(Path("D:/") / "\u4f5c\u696d\u7528" / "metric_snapshots")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a guarded normalization-change workflow: preview impact first, "
            "optionally rebuild scoped metrics, export after snapshot, and compare snapshots."
        )
    )
    parser.add_argument("--doc-id", action="append", dest="doc_ids", default=[])
    parser.add_argument("--security-code", action="append", dest="security_codes", default=[])
    parser.add_argument("--industry-33", action="append", dest="industry_33_list", default=[])
    parser.add_argument("--latest-only", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--allow-all", action="store_true")
    parser.add_argument("--enable-period-fallback", action="store_true")
    parser.add_argument("--enforce-candidate-validation", action="store_true")
    parser.add_argument("--include-unchanged", action="store_true")
    parser.add_argument("--normalized-only", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Update DB for the scoped filings.")
    parser.add_argument(
        "--before-snapshot",
        default="",
        help="Required with --apply. Directory exported by export_metric_snapshot before the DB update.",
    )
    parser.add_argument("--after-label", default="after_taxonomy_change")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--snapshot-output-dir", default=DEFAULT_SNAPSHOT_OUTPUT_DIR)
    parser.add_argument("--db-path", default=str(DB_PATH))
    return parser


def _clean_list(values: list[str]) -> list[str]:
    return [str(value).strip() for value in values if str(value).strip()]


def validate_args(args: argparse.Namespace) -> None:
    has_filter = bool(args.doc_ids or args.security_codes or args.industry_33_list)
    if not has_filter:
        if not args.allow_all:
            raise SystemExit("Specify --doc-id, --security-code, or --industry-33. Use --allow-all only with a small --limit.")
        if args.limit <= 0:
            raise SystemExit("--allow-all requires --limit > 0")

    if args.apply and not str(args.before_snapshot or "").strip():
        raise SystemExit("--before-snapshot is required with --apply")


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


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    args = build_arg_parser().parse_args()
    args.doc_ids = _clean_list(args.doc_ids)
    args.security_codes = _clean_list(args.security_codes)
    args.industry_33_list = _clean_list(args.industry_33_list)
    validate_args(args)

    timestamp = now_stamp()
    run_dir = Path(args.output_dir) / f"normalization_change_run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    preview_txt_path = run_dir / f"normalization_impact_{timestamp}.txt"
    preview_tsv_path = run_dir / f"normalization_impact_{timestamp}.tsv"
    manifest_path = run_dir / "workflow_manifest.json"

    db_path = Path(args.db_path)
    conn = _connect_readonly(db_path)
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
            include_derived=not args.normalized_only,
        )
    finally:
        conn.close()

    write_impact_tsv(preview_tsv_path, diff_rows)
    preview_lines = build_impact_text_report(
        rows=diff_rows,
        summary=summary,
        enable_period_fallback=args.enable_period_fallback,
        enforce_candidate_validation=args.enforce_candidate_validation,
        include_unchanged=args.include_unchanged,
        include_derived=not args.normalized_only,
        scope_description=_scope_description(args),
        tsv_path=preview_tsv_path,
    )
    preview_txt_path.write_text("\n".join(preview_lines) + "\n", encoding="utf-8-sig")

    manifest: dict[str, Any] = {
        "timestamp": timestamp,
        "mode": "apply" if args.apply else "preview",
        "db_path": str(db_path),
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
            "include_derived": not args.normalized_only,
        },
        "preview": {
            "txt_path": str(preview_txt_path),
            "tsv_path": str(preview_tsv_path),
            "summary": summary,
        },
    }

    if args.apply:
        rebuild_summary = rebuild_metrics_for_scope(
            doc_ids=args.doc_ids,
            industry_33_list=args.industry_33_list,
            security_codes=args.security_codes,
            latest_only=args.latest_only,
            limit=args.limit,
            enable_period_fallback=args.enable_period_fallback,
            enforce_candidate_validation=args.enforce_candidate_validation,
        )
        after_snapshot = export_metric_snapshot(
            label=args.after_label,
            output_dir=Path(args.snapshot_output_dir),
            db_path=db_path,
            timestamp=timestamp,
        )
        comparison = compare_metric_snapshots(
            before_dir=Path(args.before_snapshot),
            after_dir=after_snapshot.snapshot_dir,
            output_dir=run_dir,
            timestamp=timestamp,
        )
        manifest["rebuild"] = rebuild_summary
        manifest["after_snapshot"] = {
            "snapshot_dir": str(after_snapshot.snapshot_dir),
            "manifest_path": str(after_snapshot.manifest_path),
            "normalized_rows": after_snapshot.normalized_rows,
            "derived_rows": after_snapshot.derived_rows,
        }
        manifest["comparison"] = {
            "comparison_dir": str(comparison.comparison_dir),
            "added_count": comparison.added_count,
            "removed_count": comparison.removed_count,
            "value_changed_count": comparison.value_changed_count,
            "full_changed_same_value_count": comparison.full_changed_same_value_count,
        }

    _write_manifest(manifest_path, manifest)

    print(f"run_dir={run_dir}")
    print(f"manifest={manifest_path}")
    print(f"preview_txt={preview_txt_path}")
    print(f"preview_tsv={preview_tsv_path}")
    print(f"target_docs={summary.get('target_docs', 0)}")
    print(f"added={summary.get('added', 0)}")
    print(f"removed={summary.get('removed', 0)}")
    print(f"changed={summary.get('changed', 0)}")
    if args.apply:
        comparison_summary = manifest["comparison"]
        print(f"after_snapshot={manifest['after_snapshot']['snapshot_dir']}")
        print(f"comparison_dir={comparison_summary['comparison_dir']}")
        print(f"value_changed_count={comparison_summary['value_changed_count']}")
    else:
        print("apply=0")


if __name__ == "__main__":
    main()
