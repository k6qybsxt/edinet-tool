from __future__ import annotations

import re
import shutil
from datetime import date
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import MANIFEST_ROOT, RAW_SAVE_YEARS, XBRL_ROOT, ZIP_ROOT


DATE_DIR_PATTERN = re.compile(r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})$")
MANIFEST_NAME_PATTERN = re.compile(r"(?P<year>\d{4})-(?P<month>\d{2})(?:-(?P<day>\d{2}))?$")


def month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def shift_month(target_month: date, offset_months: int) -> date:
    zero_based_month = (target_month.year * 12) + (target_month.month - 1) + offset_months
    year, month_index = divmod(zero_based_month, 12)
    return date(year, month_index + 1, 1)


def parse_month_from_date_dir_name(name: str) -> date | None:
    match = DATE_DIR_PATTERN.fullmatch(str(name or "").strip())
    if not match:
        return None
    return month_start(int(match.group("year")), int(match.group("month")))


def parse_month_from_manifest_name(name: str) -> date | None:
    stem = Path(str(name or "")).stem
    match = MANIFEST_NAME_PATTERN.search(stem)
    if not match:
        return None
    return month_start(int(match.group("year")), int(match.group("month")))


def detect_latest_raw_month(
    *,
    zip_root: Path = ZIP_ROOT,
    xbrl_root: Path = XBRL_ROOT,
    manifest_root: Path = MANIFEST_ROOT,
) -> date | None:
    latest: date | None = None

    for root in (Path(zip_root), Path(xbrl_root)):
        if not root.exists():
            continue
        for child in root.iterdir():
            if not child.is_dir():
                continue
            target_month = parse_month_from_date_dir_name(child.name)
            if target_month is None:
                continue
            if latest is None or target_month > latest:
                latest = target_month

    manifest_dir = Path(manifest_root)
    if manifest_dir.exists():
        for child in manifest_dir.iterdir():
            if not child.is_file():
                continue
            target_month = parse_month_from_manifest_name(child.name)
            if target_month is None:
                continue
            if latest is None or target_month > latest:
                latest = target_month

    return latest


def resolve_keep_from_month(*, latest_month: date, keep_years: int = RAW_SAVE_YEARS) -> date:
    if keep_years <= 0:
        raise ValueError("keep_years must be greater than 0.")
    keep_months = keep_years * 12
    return shift_month(latest_month, -(keep_months - 1))


def delete_old_date_dirs(root: Path, *, keep_from_month: date) -> tuple[int, list[str]]:
    deleted_paths: list[str] = []
    target_root = Path(root)
    if not target_root.exists():
        return 0, deleted_paths

    deleted_count = 0
    for child in sorted(target_root.iterdir(), key=lambda path: path.name):
        if not child.is_dir():
            continue
        target_month = parse_month_from_date_dir_name(child.name)
        if target_month is None or target_month >= keep_from_month:
            continue
        shutil.rmtree(child)
        deleted_count += 1
        deleted_paths.append(str(child))

    return deleted_count, deleted_paths


def delete_old_manifest_files(manifest_root: Path, *, keep_from_month: date) -> tuple[int, list[str]]:
    deleted_paths: list[str] = []
    target_root = Path(manifest_root)
    if not target_root.exists():
        return 0, deleted_paths

    deleted_count = 0
    for child in sorted(target_root.iterdir(), key=lambda path: path.name):
        if not child.is_file():
            continue
        target_month = parse_month_from_manifest_name(child.name)
        if target_month is None or target_month >= keep_from_month:
            continue
        child.unlink(missing_ok=True)
        deleted_count += 1
        deleted_paths.append(str(child))

    return deleted_count, deleted_paths


def cleanup_old_raw_storage(
    *,
    latest_month: date | None = None,
    keep_years: int = RAW_SAVE_YEARS,
    zip_root: Path = ZIP_ROOT,
    xbrl_root: Path = XBRL_ROOT,
    manifest_root: Path = MANIFEST_ROOT,
) -> dict[str, Any]:
    reference_month = latest_month or detect_latest_raw_month(
        zip_root=zip_root,
        xbrl_root=xbrl_root,
        manifest_root=manifest_root,
    )
    if reference_month is None:
        return {
            "status": "skipped",
            "reason": "no_reference_month",
            "reference_month": "",
            "keep_from_month": "",
            "deleted_zip_dirs": 0,
            "deleted_xbrl_dirs": 0,
            "deleted_manifest_files": 0,
            "deleted_total": 0,
            "sample_deleted_paths": [],
            "error": "",
        }

    keep_from_month = resolve_keep_from_month(latest_month=reference_month, keep_years=keep_years)
    deleted_zip_dirs, deleted_zip_paths = delete_old_date_dirs(Path(zip_root), keep_from_month=keep_from_month)
    deleted_xbrl_dirs, deleted_xbrl_paths = delete_old_date_dirs(Path(xbrl_root), keep_from_month=keep_from_month)
    deleted_manifest_files, deleted_manifest_paths = delete_old_manifest_files(
        Path(manifest_root),
        keep_from_month=keep_from_month,
    )
    sample_deleted_paths = (deleted_zip_paths + deleted_xbrl_paths + deleted_manifest_paths)[:10]

    return {
        "status": "completed",
        "reason": "",
        "reference_month": reference_month.strftime("%Y-%m"),
        "keep_from_month": keep_from_month.strftime("%Y-%m"),
        "deleted_zip_dirs": deleted_zip_dirs,
        "deleted_xbrl_dirs": deleted_xbrl_dirs,
        "deleted_manifest_files": deleted_manifest_files,
        "deleted_total": deleted_zip_dirs + deleted_xbrl_dirs + deleted_manifest_files,
        "sample_deleted_paths": sample_deleted_paths,
        "error": "",
    }
