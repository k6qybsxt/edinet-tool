from __future__ import annotations

from datetime import datetime
from typing import Any


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_context_map(parsed: dict) -> dict[str, dict]:
    return parsed.get("contexts", {}) or {}


def to_raw_fact_rows(doc_id: str, parsed: dict) -> list[dict[str, Any]]:
    facts = parsed.get("facts", []) or []
    contexts = build_context_map(parsed)

    rows: list[dict[str, Any]] = []
    created_at = now_text()

    for fact in facts:
        context_ref = fact.get("contextRef") or ""
        ctx = contexts.get(context_ref, {}) if context_ref else {}

        period_type = ""
        period_start = None
        period_end = None
        instant_date = None

        if ctx.get("instant"):
            period_type = "instant"
            instant_date = ctx.get("instant")
        elif ctx.get("start") or ctx.get("end"):
            period_type = "duration"
            period_start = ctx.get("start")
            period_end = ctx.get("end")

        rows.append(
            {
                "doc_id": doc_id,
                "tag_name": str(fact.get("local") or ""),
                "context_ref": context_ref,
                "unit_ref": fact.get("unitRef"),
                "period_type": period_type,
                "period_start": period_start,
                "period_end": period_end,
                "instant_date": instant_date,
                "consolidation": ctx.get("dim"),
                "value_text": fact.get("text"),
                "created_at": created_at,
            }
        )

    return rows