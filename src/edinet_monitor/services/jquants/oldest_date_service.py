from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import json
from pathlib import Path

from edinet_monitor.config.settings import JQUANTS_STORAGE_ROOT
from edinet_monitor.services.jquants.client import JQuantsClient


@dataclass(frozen=True)
class JQuantsOldestDateResult:
    target: str
    date_from: str
    date_to: str
    oldest_date: str | None
    oldest_month: str | None
    checked_days: int
    hit_count: int
    errors: list[str] = field(default_factory=list)
    output_path: Path | None = None
    manifest_path: Path | None = None


def discover_oldest_fins_summary_date(
    *,
    client: JQuantsClient,
    date_from: str,
    date_to: str,
    output_dir: str | Path | None = None,
    storage_root: str | Path | None = None,
    seed_codes: list[str] | None = None,
    lookback_months: int = 3,
) -> JQuantsOldestDateResult:
    started_at = datetime.now().isoformat(timespec="seconds")
    errors: list[str] = []
    checked_days = 0
    hit_count = 0
    oldest_date: str | None = None
    seed_dates, seed_errors = _seed_oldest_dates(client, seed_codes or [])
    errors.extend(seed_errors)
    scan_start = _scan_start_from_seed(seed_dates, date_from=date_from, lookback_months=lookback_months)

    if seed_codes and not seed_dates and seed_errors:
        errors.append("daily_scan_skipped_because_all_seed_code_requests_failed")
    else:
        for current in _iter_dates(scan_start, date_to):
            api_date = current.isoformat()
            checked_days += 1
            try:
                page = client.get_fin_summary_page(date=api_date)
                count = len(page.items)
            except Exception as exc:  # pragma: no cover - CLI safety path
                errors.append(f"{api_date}: {type(exc).__name__}: {exc}")
                continue
            if count > 0:
                hit_count = count
                oldest_date = api_date
                break

    oldest_month = oldest_date[:7] if oldest_date else None
    finished_at = datetime.now().isoformat(timespec="seconds")
    result_payload = {
        "target": "fins_summary",
        "date_from": date_from,
        "date_to": date_to,
        "oldest_date": oldest_date,
        "oldest_month": oldest_month,
        "checked_days": checked_days,
        "hit_count": hit_count,
        "errors": errors,
        "seed_codes": seed_codes or [],
        "seed_oldest_dates": seed_dates,
        "scan_start": scan_start,
        "lookback_months": lookback_months,
        "started_at": started_at,
        "finished_at": finished_at,
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = None
    if output_dir is not None:
        output_path = Path(output_dir) / f"jquants_oldest_fins_summary_{timestamp}.txt"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(_format_report(result_payload), encoding="utf-8-sig")

    root = Path(storage_root) if storage_root is not None else JQUANTS_STORAGE_ROOT
    manifest_path = root / "manifests" / f"oldest_fins_summary_{timestamp}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(result_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return JQuantsOldestDateResult(
        target="fins_summary",
        date_from=date_from,
        date_to=date_to,
        oldest_date=oldest_date,
        oldest_month=oldest_month,
        checked_days=checked_days,
        hit_count=hit_count,
        errors=errors,
        output_path=output_path,
        manifest_path=manifest_path,
    )


def _iter_dates(date_from: str, date_to: str):
    current = date.fromisoformat(date_from.replace("/", "-"))
    end = date.fromisoformat(date_to.replace("/", "-"))
    while current <= end:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def _seed_oldest_dates(client: JQuantsClient, seed_codes: list[str]) -> tuple[dict[str, str], list[str]]:
    result: dict[str, str] = {}
    errors: list[str] = []
    for code in seed_codes:
        normalized_code = str(code or "").strip()
        if not normalized_code:
            continue
        dates: list[str] = []
        try:
            for row in client.iter_fin_summary(code=normalized_code):
                date_text = _normalize_date(row.get("DiscDate"))
                if date_text:
                    dates.append(date_text)
        except Exception as exc:  # pragma: no cover - CLI safety path
            errors.append(f"seed_code={normalized_code}: {type(exc).__name__}: {exc}")
            continue
        if dates:
            result[normalized_code] = min(dates)
    return result, errors


def _scan_start_from_seed(seed_dates: dict[str, str], *, date_from: str, lookback_months: int) -> str:
    floor = date.fromisoformat(date_from.replace("/", "-"))
    if not seed_dates:
        return floor.isoformat()
    oldest = min(date.fromisoformat(value) for value in seed_dates.values())
    scan_month = _add_months(date(oldest.year, oldest.month, 1), -max(0, lookback_months))
    return max(floor, scan_month).isoformat()


def _add_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 + months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def _normalize_date(value) -> str:
    text = str(value or "").strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _format_report(payload: dict) -> str:
    lines = [
        f"generated_at: {payload['finished_at']}",
        "target: fins_summary",
        f"date_from: {payload['date_from']}",
        f"date_to: {payload['date_to']}",
        f"scan_start: {payload['scan_start']}",
        f"seed_codes: {','.join(payload['seed_codes'])}",
        f"seed_oldest_dates: {json.dumps(payload['seed_oldest_dates'], ensure_ascii=False, sort_keys=True)}",
        f"oldest_date: {payload['oldest_date'] or ''}",
        f"oldest_month: {payload['oldest_month'] or ''}",
        f"checked_days: {payload['checked_days']}",
        f"hit_count: {payload['hit_count']}",
        f"errors: {len(payload['errors'])}",
        "",
    ]
    for error in payload["errors"][:50]:
        lines.append(f"error: {error}")
    return "\n".join(lines) + "\n"
