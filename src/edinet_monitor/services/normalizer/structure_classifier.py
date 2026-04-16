from __future__ import annotations

from typing import Any


def _structure_text(tag_name: str, structure_info: dict[str, Any] | None) -> str:
    if not structure_info:
        return tag_name
    parts = [
        tag_name,
        str(structure_info.get("label") or ""),
        *[str(item) for item in structure_info.get("presentation_parent_labels", []) or []],
    ]
    return " ".join(part for part in parts if part)


def classify_structure(
    *,
    metric_base: str,
    tag_name: str,
    structure_info: dict[str, Any] | None,
) -> dict[str, Any]:
    text = _structure_text(tag_name, structure_info)
    is_total = bool(structure_info and structure_info.get("is_total"))
    calculation_children_count = int(structure_info.get("calculation_children_count") or 0) if structure_info else 0
    parent_labels = [str(item) for item in structure_info.get("presentation_parent_labels", []) or []] if structure_info else []

    role = "unknown"
    confidence = "low"

    if any(keyword in text for keyword in ("売上総利益", "粗利益", "資金利益", "純収益")):
        role = "profit"
        confidence = "high"
    elif "控除後" in text and "収益" in text:
        role = "profit"
        confidence = "high"
    elif "利益" in text:
        role = "profit"
        confidence = "medium"
    elif any(keyword in text for keyword in ("売上原価", "原価", "金融費用", "資金調達費用")):
        role = "cost"
        confidence = "high" if is_total or parent_labels else "medium"
    elif any(keyword in text for keyword in ("販売費及び一般管理費", "販管費", "営業経費", "一般管理費")):
        role = "expense"
        confidence = "high" if parent_labels else "medium"
    elif any(keyword in text for keyword in ("費用合計", "経常費用", "営業費用", "事業費用", "業務費")):
        role = "combined_expense" if is_total or calculation_children_count > 0 else "expense"
        confidence = "high" if is_total or calculation_children_count > 0 else "medium"
    elif any(keyword in text for keyword in ("収益", "売上高", "営業収益", "事業収益")):
        role = "revenue"
        confidence = "medium"

    if metric_base == "CostOfSales" and role == "cost":
        confidence = "high"
    if metric_base == "SellingExpenses" and role == "expense":
        confidence = "high"
    if metric_base == "CostOfSalesAndSellingGeneralAndAdministrativeExpenses" and role == "combined_expense":
        confidence = "high"
    if metric_base == "GrossProfit" and role == "profit":
        confidence = "high"

    return {
        "role": role,
        "confidence": confidence,
        "is_total": is_total,
        "text": text,
        "presentation_parent_labels": parent_labels,
        "calculation_children_count": calculation_children_count,
    }
