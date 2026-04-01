from __future__ import annotations

from datetime import date, timedelta


DEFAULT_TARGET_LAG_DAYS = 2


def _expand_date_range(start_date: date, end_date: date) -> list[date]:
    days = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(days + 1)]


def resolve_target_dates(
    *,
    target_date_text: str = "",
    date_from_text: str = "",
    date_to_text: str = "",
    default_lag_days: int = DEFAULT_TARGET_LAG_DAYS,
) -> list[date]:
    target_date_text = str(target_date_text or "").strip()
    date_from_text = str(date_from_text or "").strip()
    date_to_text = str(date_to_text or "").strip()

    if target_date_text and (date_from_text or date_to_text):
        raise ValueError("Use either target_date or date_from/date_to.")

    if date_from_text or date_to_text:
        if not date_from_text or not date_to_text:
            raise ValueError("Both date_from and date_to are required.")

        start_date = date.fromisoformat(date_from_text)
        end_date = date.fromisoformat(date_to_text)

        if start_date > end_date:
            raise ValueError("date_from must be earlier than or equal to date_to.")

        return _expand_date_range(start_date, end_date)

    if target_date_text:
        return [date.fromisoformat(target_date_text)]

    return [date.today() - timedelta(days=default_lag_days)]
