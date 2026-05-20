from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil


@dataclass(frozen=True)
class DbBackupResult:
    source_path: Path
    backup_path: Path
    source_size_bytes: int
    backup_size_bytes: int


def _safe_label(label: str | None) -> str:
    text = str(label or "").strip()
    if not text:
        return ""
    allowed = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_"}:
            allowed.append(ch)
        else:
            allowed.append("_")
    return "".join(allowed).strip("_")


def build_backup_path(
    *,
    source_path: Path,
    output_dir: Path,
    label: str | None = None,
    timestamp: datetime | None = None,
) -> Path:
    stamp = (timestamp or datetime.now()).strftime("%Y%m%d_%H%M%S")
    suffix = f"_{_safe_label(label)}" if _safe_label(label) else ""
    return output_dir / f"{source_path.stem}{suffix}_{stamp}{source_path.suffix}"


def backup_sqlite_db(
    *,
    source_path: Path,
    output_dir: Path,
    label: str | None = None,
) -> DbBackupResult:
    source_path = Path(source_path)
    output_dir = Path(output_dir)

    if not source_path.exists():
        raise FileNotFoundError(f"DB file not found: {source_path}")
    if not source_path.is_file():
        raise ValueError(f"DB path is not a file: {source_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    backup_path = build_backup_path(source_path=source_path, output_dir=output_dir, label=label)
    shutil.copy2(source_path, backup_path)

    return DbBackupResult(
        source_path=source_path,
        backup_path=backup_path,
        source_size_bytes=source_path.stat().st_size,
        backup_size_bytes=backup_path.stat().st_size,
    )
