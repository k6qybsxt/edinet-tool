from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

from edinet_monitor.config.settings import MANIFEST_ROOT


def sanitize_manifest_name(manifest_name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(manifest_name or "").strip())
    cleaned = cleaned.strip("._-")
    if not cleaned:
        return "document_manifest"
    return cleaned


def build_manifest_path(manifest_name: str) -> Path:
    return MANIFEST_ROOT / f"{sanitize_manifest_name(manifest_name)}.jsonl"


def read_manifest_rows(manifest_path: Path) -> list[dict[str, Any]]:
    target_path = Path(manifest_path)
    if not target_path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with target_path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def merge_manifest_rows(
    existing_rows: Iterable[dict[str, Any]],
    incoming_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_doc_id: dict[str, dict[str, Any]] = {}

    for row in existing_rows:
        doc_id = str(row.get("doc_id") or "").strip()
        if doc_id:
            by_doc_id[doc_id] = dict(row)

    for row in incoming_rows:
        doc_id = str(row.get("doc_id") or "").strip()
        if doc_id:
            by_doc_id[doc_id] = dict(row)

    return sorted(
        by_doc_id.values(),
        key=lambda row: (
            str(row.get("submit_date") or ""),
            str(row.get("doc_id") or ""),
        ),
    )


def write_manifest_rows(manifest_path: Path, rows: Iterable[dict[str, Any]]) -> int:
    target_path = Path(manifest_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with target_path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")
            count += 1

    return count


def summarize_manifest_rows(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    totals = {
        "manifest_rows": 0,
        "pending_rows": 0,
        "downloaded_rows": 0,
        "error_rows": 0,
        "other_rows": 0,
    }
    sample_errors: list[dict[str, Any]] = []

    for row in rows:
        totals["manifest_rows"] += 1
        status = str(row.get("download_status") or "pending").strip() or "pending"

        if status == "pending":
            totals["pending_rows"] += 1
        elif status == "downloaded":
            totals["downloaded_rows"] += 1
        elif status == "error":
            totals["error_rows"] += 1
            if len(sample_errors) < 5:
                sample_errors.append(
                    {
                        "doc_id": row.get("doc_id"),
                        "company_name": row.get("company_name"),
                        "submit_date": row.get("submit_date"),
                        "download_error": row.get("download_error"),
                    }
                )
        else:
            totals["other_rows"] += 1

    return {
        **totals,
        "sample_errors": sample_errors,
    }
