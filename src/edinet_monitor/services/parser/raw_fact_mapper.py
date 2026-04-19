from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_context_map(parsed: dict) -> dict[str, dict]:
    return parsed.get("contexts", {}) or {}


def build_unit_map(parsed: dict) -> dict[str, dict]:
    return parsed.get("units", {}) or {}


def _json_text(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)


def to_raw_fact_rows(
    doc_id: str,
    parsed: dict,
    *,
    xbrl_member_name: str | None = None,
) -> list[dict[str, Any]]:
    facts = parsed.get("facts", []) or []
    contexts = build_context_map(parsed)
    units = build_unit_map(parsed)

    rows: list[dict[str, Any]] = []
    created_at = now_text()

    for fact in facts:
        context_ref = fact.get("contextRef") or ""
        ctx = contexts.get(context_ref, {}) if context_ref else {}
        unit_ref = fact.get("unitRef")
        unit = units.get(unit_ref, {}) if unit_ref else {}

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
                "tag_qname": str(fact.get("qname") or fact.get("tag") or ""),
                "namespace_uri": str(fact.get("namespace_uri") or ""),
                "namespace_prefix": str(fact.get("namespace_prefix") or ""),
                "taxonomy_kind": str(fact.get("taxonomy_kind") or ""),
                "context_ref": context_ref,
                "unit_ref": unit_ref,
                "decimals": fact.get("decimals"),
                "period_type": period_type,
                "period_start": period_start,
                "period_end": period_end,
                "instant_date": instant_date,
                "consolidation": ctx.get("dim"),
                "is_nil": 1 if fact.get("is_nil") else 0,
                "context_dimensions_json": _json_text(ctx.get("dimensions")),
                "unit_measures_json": _json_text(unit),
                "xbrl_member_name": xbrl_member_name or "",
                "value_text": fact.get("text"),
                "created_at": created_at,
            }
        )

    return rows
