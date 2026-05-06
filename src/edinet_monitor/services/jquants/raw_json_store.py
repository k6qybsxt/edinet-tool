from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import JQUANTS_STORAGE_ROOT


def normalize_jquants_date(value: Any) -> str:
    text = str(value or "").strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def fins_summary_record_key(row: dict[str, Any]) -> str:
    disc_no = str(row.get("DiscNo") or "").strip()
    if disc_no:
        return disc_no
    parts = [
        str(row.get("Code") or ""),
        normalize_jquants_date(row.get("DiscDate")),
        str(row.get("CurPerType") or ""),
        normalize_jquants_date(row.get("CurPerEn")),
        str(row.get("DocType") or ""),
    ]
    return "generated:" + "|".join(parts)


def fins_summary_raw_path(date_text: str, *, storage_root: str | Path | None = None) -> Path:
    normalized = normalize_jquants_date(date_text)
    year = normalized[:4]
    root = Path(storage_root) if storage_root is not None else JQUANTS_STORAGE_ROOT
    return root / "raw_json" / "fins_summary" / year / f"{normalized}.jsonl"


def write_fins_summary_raw_jsonl(
    rows: list[dict[str, Any]],
    *,
    storage_root: str | Path | None = None,
) -> dict[str, int]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        date_text = normalize_jquants_date(row.get("DiscDate"))
        if date_text:
            grouped[date_text].append(row)

    written: dict[str, int] = {}
    for date_text, day_rows in grouped.items():
        path = fins_summary_raw_path(date_text, storage_root=storage_root)
        existing = _read_existing_by_key(path)
        for row in day_rows:
            existing[fins_summary_record_key(row)] = row
        path.parent.mkdir(parents=True, exist_ok=True)
        ordered = [existing[key] for key in sorted(existing)]
        path.write_text(
            "".join(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n" for row in ordered),
            encoding="utf-8",
        )
        written[date_text] = len(ordered)
    return written


def _read_existing_by_key(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    result: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if isinstance(row, dict):
            result[fins_summary_record_key(row)] = row
    return result
