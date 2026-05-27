from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import re
import sqlite3
from typing import Any

from edinet_monitor.db.migrations import apply_schema_migrations


ALLOWED_CATEGORIES = {"schema", "recalculation", "data_backfill", "validation", "other"}
SECTION_MARKER = "[DB\u53cd\u6620\u5f85\u3061]"
STATUS_PREFIX = "\u72b6\u614b:"
PENDING_STATUS = "\u672a\u53cd\u6620"
ADDED_DATE_PREFIX = "\u8ffd\u52a0\u65e5:"


@dataclass(frozen=True)
class DbReflectionItem:
    item_id: int
    title: str
    category: str
    description: str
    required_commands: list[str]
    verification_sql: list[str]
    related_migration_ids: list[str]
    source_path: str
    source_key: str
    notes: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ImportDbReflectionItemsResult:
    imported_count: int
    skipped_count: int
    item_ids: list[int]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_list(text: Any) -> list[str]:
    if not text:
        return []
    try:
        value = json.loads(str(text))
    except Exception:
        return []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_ready(conn: sqlite3.Connection) -> None:
    apply_schema_migrations(conn)
    conn.commit()


def _validate_category(category: str) -> str:
    normalized = str(category or "other").strip() or "other"
    if normalized not in ALLOWED_CATEGORIES:
        raise ValueError(f"category must be one of {', '.join(sorted(ALLOWED_CATEGORIES))}")
    return normalized


def _row_to_item(row: sqlite3.Row) -> DbReflectionItem:
    return DbReflectionItem(
        item_id=int(row["item_id"]),
        title=str(row["title"] or ""),
        category=str(row["category"] or ""),
        description=str(row["description"] or ""),
        required_commands=_json_list(row["required_commands_json"]),
        verification_sql=_json_list(row["verification_sql_json"]),
        related_migration_ids=_json_list(row["related_migration_ids_json"]),
        source_path=str(row["source_path"] or ""),
        source_key=str(row["source_key"] or ""),
        notes=str(row["notes"] or ""),
        created_at=str(row["created_at"] or ""),
        updated_at=str(row["updated_at"] or ""),
    )


def add_db_reflection_item(
    conn: sqlite3.Connection,
    *,
    title: str,
    category: str = "other",
    description: str = "",
    required_commands: list[str] | tuple[str, ...] | None = None,
    verification_sql: list[str] | tuple[str, ...] | None = None,
    related_migration_ids: list[str] | tuple[str, ...] | None = None,
    source_path: str = "",
    source_key: str = "",
    notes: str = "",
) -> DbReflectionItem:
    _ensure_ready(conn)
    clean_title = str(title or "").strip()
    if not clean_title:
        raise ValueError("title is required")
    clean_category = _validate_category(category)
    clean_source_key = str(source_key or "").strip()
    if clean_source_key:
        existing = get_db_reflection_item_by_source_key(conn, clean_source_key)
        if existing is not None:
            return existing
    timestamp = _now()
    cursor = conn.execute(
        """
        INSERT INTO db_reflection_items (
            title, category, description, required_commands_json,
            verification_sql_json, related_migration_ids_json, source_path,
            source_key, notes, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            clean_title,
            clean_category,
            str(description or "").strip(),
            _json_dumps([str(item) for item in (required_commands or [])]),
            _json_dumps([str(item) for item in (verification_sql or [])]),
            _json_dumps([str(item) for item in (related_migration_ids or [])]),
            str(source_path or "").strip(),
            clean_source_key or None,
            str(notes or "").strip(),
            timestamp,
            timestamp,
        ),
    )
    conn.commit()
    item = get_db_reflection_item(conn, int(cursor.lastrowid))
    if item is None:
        raise RuntimeError("failed to load inserted db_reflection_items row")
    return item


def list_db_reflection_items(conn: sqlite3.Connection) -> list[DbReflectionItem]:
    _ensure_ready(conn)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT *
        FROM db_reflection_items
        ORDER BY created_at, item_id
        """
    ).fetchall()
    return [_row_to_item(row) for row in rows]


def get_db_reflection_item(conn: sqlite3.Connection, item_id: int) -> DbReflectionItem | None:
    _ensure_ready(conn)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT *
        FROM db_reflection_items
        WHERE item_id = ?
        """,
        (item_id,),
    ).fetchone()
    return _row_to_item(row) if row else None


def get_db_reflection_item_by_source_key(
    conn: sqlite3.Connection,
    source_key: str,
) -> DbReflectionItem | None:
    _ensure_ready(conn)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT *
        FROM db_reflection_items
        WHERE source_key = ?
        """,
        (source_key,),
    ).fetchone()
    return _row_to_item(row) if row else None


def complete_db_reflection_item(conn: sqlite3.Connection, item_id: int) -> bool:
    _ensure_ready(conn)
    cursor = conn.execute(
        """
        DELETE FROM db_reflection_items
        WHERE item_id = ?
        """,
        (item_id,),
    )
    conn.commit()
    return cursor.rowcount > 0


def _read_text(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="replace")


def _split_blocks(text: str) -> list[str]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if len(stripped) >= 10 and set(stripped) == {"="}:
            if current:
                blocks.append(current)
                current = []
            continue
        current.append(line)
    if current:
        blocks.append(current)
    return ["\n".join(block).strip() for block in blocks if "\n".join(block).strip()]


def _extract_block_title(block: str) -> str:
    for line in block.splitlines():
        if line.strip().startswith(SECTION_MARKER):
            return line.split("]", 1)[-1].strip() or "DB reflection item"
    return "DB reflection item"


def _extract_added_date(block: str) -> str:
    for line in block.splitlines():
        stripped = line.strip()
        if stripped.startswith(ADDED_DATE_PREFIX):
            return stripped.split(":", 1)[-1].strip()
    return ""


def _is_pending_block(block: str) -> bool:
    if SECTION_MARKER not in block:
        return False
    for line in block.splitlines():
        stripped = line.strip()
        if stripped.startswith(STATUS_PREFIX):
            return PENDING_STATUS in stripped
    return False


def _related_migration_ids(block: str) -> list[str]:
    return sorted(set(re.findall(r"\b\d{3}_[A-Za-z0-9_]+\b", block)))


def import_db_reflection_items_from_txt(
    conn: sqlite3.Connection,
    *,
    path: str | Path,
) -> ImportDbReflectionItemsResult:
    source = Path(path)
    text = _read_text(source)
    imported: list[int] = []
    skipped_count = 0
    for block in _split_blocks(text):
        if not _is_pending_block(block):
            continue
        title = _extract_block_title(block)
        added_date = _extract_added_date(block)
        source_key = f"{source.resolve()}::{title}::{added_date}"
        before = get_db_reflection_item_by_source_key(conn, source_key)
        item = add_db_reflection_item(
            conn,
            title=title,
            category="other",
            description=block,
            related_migration_ids=_related_migration_ids(block),
            source_path=str(source),
            source_key=source_key,
        )
        if before is None:
            imported.append(item.item_id)
        else:
            skipped_count += 1
    return ImportDbReflectionItemsResult(
        imported_count=len(imported),
        skipped_count=skipped_count,
        item_ids=imported,
    )
