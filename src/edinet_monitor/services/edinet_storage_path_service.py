from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Any

from edinet_monitor.config.settings import MONITOR_STORAGE_ROOT, XBRL_ROOT, ZIP_ROOT


OLD_MONITOR_STORAGE_ROOTS = (
    Path(r"D:\EDINET_Data\edinet_monitor"),
)


@dataclass(frozen=True)
class ResolvedStoragePaths:
    zip_path: Path | None
    xbrl_path: Path | None
    expected_zip_path: Path
    expected_xbrl_path: Path


@dataclass(frozen=True)
class StoragePathRepairAction:
    doc_id: str
    old_zip_path: str
    new_zip_path: str
    old_xbrl_path: str
    new_xbrl_path: str
    zip_resolved: bool
    xbrl_resolved: bool


def _submit_date_part(value: Any) -> str:
    text = str(value or "")[:10]
    return text if text else "unknown_date"


def expected_zip_path(*, submit_date: Any, doc_id: str) -> Path:
    return ZIP_ROOT / _submit_date_part(submit_date) / f"{doc_id}.zip"


def expected_xbrl_path(*, submit_date: Any, doc_id: str) -> Path:
    return XBRL_ROOT / _submit_date_part(submit_date) / f"{doc_id}.xbrl"


def _replace_old_storage_root(path: Path) -> Path | None:
    path_text = str(path)
    for old_root in OLD_MONITOR_STORAGE_ROOTS:
        old_text = str(old_root)
        if path_text.lower().startswith(old_text.lower()):
            suffix = path_text[len(old_text):].lstrip("\\/")
            return MONITOR_STORAGE_ROOT / suffix
    return None


def resolve_existing_path(
    *,
    current_path: Any,
    expected_path: Path,
) -> Path | None:
    candidates: list[Path] = []
    text = str(current_path or "").strip()
    if text:
        current = Path(text)
        candidates.append(current)
        replaced = _replace_old_storage_root(current)
        if replaced is not None:
            candidates.append(replaced)
    candidates.append(expected_path)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).lower()
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate
    return None


def resolve_storage_paths(filing: dict[str, Any]) -> ResolvedStoragePaths:
    doc_id = str(filing.get("doc_id") or "")
    submit_date = filing.get("submit_date") or ""
    zip_expected = expected_zip_path(submit_date=submit_date, doc_id=doc_id)
    xbrl_expected = expected_xbrl_path(submit_date=submit_date, doc_id=doc_id)
    return ResolvedStoragePaths(
        zip_path=resolve_existing_path(current_path=filing.get("zip_path"), expected_path=zip_expected),
        xbrl_path=resolve_existing_path(current_path=filing.get("xbrl_path"), expected_path=xbrl_expected),
        expected_zip_path=zip_expected,
        expected_xbrl_path=xbrl_expected,
    )


def repair_storage_paths(
    conn: sqlite3.Connection,
    filings: list[dict[str, Any]],
    *,
    apply: bool,
) -> list[StoragePathRepairAction]:
    actions: list[StoragePathRepairAction] = []
    for filing in filings:
        resolved = resolve_storage_paths(filing)
        old_zip = str(filing.get("zip_path") or "")
        old_xbrl = str(filing.get("xbrl_path") or "")
        new_zip = str(resolved.zip_path or old_zip)
        new_xbrl = str(resolved.xbrl_path or old_xbrl)
        action = StoragePathRepairAction(
            doc_id=str(filing.get("doc_id") or ""),
            old_zip_path=old_zip,
            new_zip_path=new_zip,
            old_xbrl_path=old_xbrl,
            new_xbrl_path=new_xbrl,
            zip_resolved=resolved.zip_path is not None,
            xbrl_resolved=resolved.xbrl_path is not None,
        )
        actions.append(action)
        if apply and (new_zip != old_zip or new_xbrl != old_xbrl):
            conn.execute(
                """
                UPDATE filings
                SET zip_path = ?,
                    xbrl_path = ?
                WHERE doc_id = ?
                """,
                (new_zip, new_xbrl, action.doc_id),
            )
    if apply:
        conn.commit()
    return actions
