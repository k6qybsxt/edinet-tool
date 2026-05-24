from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any

from edinet_monitor.services.jquants.mapper import normalize_security_code


@dataclass(frozen=True)
class JQuantsListedInfoRaw:
    listing_date: str
    local_code: str
    security_code: str
    company_name: str
    company_name_en: str
    sector_17_code: str
    sector_17_name: str
    sector_33_code: str
    sector_33_name: str
    scale_category: str
    market_code: str
    market_name: str
    margin_code: str
    margin_name: str
    raw_json: str


@dataclass(frozen=True)
class JQuantsFsDetailsRaw:
    disclosure_number: str
    disclosed_date: str
    disclosed_time: str
    local_code: str
    security_code: str
    type_of_document: str
    raw_json: str


@dataclass(frozen=True)
class JQuantsFsDetailItem:
    disclosure_number: str
    local_code: str
    security_code: str
    disclosed_date: str
    item_key: str
    metric_hint: str
    detail_label: str
    value_num: float | None
    value_text: str
    source_path: str


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _first(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return ""


def _normalize_date(value: Any) -> str:
    text = str(value or "").strip().replace("/", "-")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text.replace(",", ""))
    except InvalidOperation:
        return None


def _to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def listed_info_from_row(row: dict[str, Any]) -> JQuantsListedInfoRaw:
    local_code = str(_first(row, "Code", "LocalCode", "code")).strip()
    return JQuantsListedInfoRaw(
        listing_date=_normalize_date(_first(row, "Date", "date")),
        local_code=local_code,
        security_code=normalize_security_code(local_code),
        company_name=str(_first(row, "CompanyName", "CoName", "Name", "company_name")).strip(),
        company_name_en=str(_first(row, "CompanyNameEnglish", "CoNameEn", "NameEn", "company_name_en")).strip(),
        sector_17_code=str(_first(row, "Sector17Code", "S17", "sector_17_code")).strip(),
        sector_17_name=str(_first(row, "Sector17CodeName", "S17Nm", "sector_17_name")).strip(),
        sector_33_code=str(_first(row, "Sector33Code", "S33", "sector_33_code")).strip(),
        sector_33_name=str(_first(row, "Sector33CodeName", "S33Nm", "sector_33_name")).strip(),
        scale_category=str(_first(row, "ScaleCategory", "ScaleCat", "scale_category")).strip(),
        market_code=str(_first(row, "MarketCode", "Mkt", "market_code")).strip(),
        market_name=str(_first(row, "MarketCodeName", "MktNm", "market_name")).strip(),
        margin_code=str(_first(row, "MarginCode", "Mrgn", "margin_code")).strip(),
        margin_name=str(_first(row, "MarginCodeName", "MrgnNm", "margin_name")).strip(),
        raw_json=_json_dumps(row),
    )


def fs_details_from_row(row: dict[str, Any]) -> tuple[JQuantsFsDetailsRaw, list[JQuantsFsDetailItem]]:
    local_code = str(_first(row, "Code", "LocalCode", "code")).strip()
    disclosure_number = str(_first(row, "DiscNo", "DisclosureNumber", "disclosure_number")).strip()
    disclosed_date = _normalize_date(_first(row, "DiscDate", "DisclosedDate", "disclosed_date"))
    raw = JQuantsFsDetailsRaw(
        disclosure_number=disclosure_number,
        disclosed_date=disclosed_date,
        disclosed_time=str(_first(row, "DiscTime", "DisclosedTime", "disclosed_time")).strip(),
        local_code=local_code,
        security_code=normalize_security_code(local_code),
        type_of_document=str(_first(row, "DocType", "TypeOfDocument", "type_of_document")).strip(),
        raw_json=_json_dumps(row),
    )
    fs = row.get("FS") or row.get("FinancialStatement") or row.get("financial_statement") or {}
    items = [
        JQuantsFsDetailItem(
            disclosure_number=raw.disclosure_number,
            local_code=raw.local_code,
            security_code=raw.security_code,
            disclosed_date=raw.disclosed_date,
            item_key=_item_key(path, label),
            metric_hint=hint,
            detail_label=label,
            value_num=_to_float(_parse_decimal(value_text)),
            value_text=value_text,
            source_path=path,
        )
        for path, label, value_text, hint in _iter_major_fs_items(fs)
    ]
    return raw, items


def _item_key(path: str, label: str) -> str:
    digest = hashlib.sha1(f"{path}|{label}".encode("utf-8")).hexdigest()[:16]
    return digest


def _iter_major_fs_items(value: Any, *, path: str = "FS"):
    for item_path, label, value_text in _iter_fs_leaf_values(value, path=path):
        hint = _metric_hint_for_label(f"{label} {item_path}")
        if hint:
            yield item_path, label, value_text, hint


def _iter_fs_leaf_values(value: Any, *, path: str):
    if isinstance(value, dict):
        label = str(_first(value, "label", "Label", "name", "Name")).strip()
        scalar = _first(value, "value", "Value", "amount", "Amount")
        if scalar not in (None, ""):
            yield path, label or path.rsplit(".", 1)[-1], str(scalar)
        for key, child in value.items():
            if key in {"label", "Label", "name", "Name", "value", "Value", "amount", "Amount"}:
                continue
            child_label = str(key)
            if not isinstance(child, (dict, list)):
                yield f"{path}.{child_label}", child_label, str(child)
            else:
                yield from _iter_fs_leaf_values(child, path=f"{path}.{child_label}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_fs_leaf_values(child, path=f"{path}[{index}]")
        return
    if value not in (None, ""):
        yield path, path.rsplit(".", 1)[-1], str(value)


def _metric_hint_for_label(text: str) -> str:
    normalized = text.lower()
    if any(token in text for token in ("非支配株主",)) or "non-controlling" in normalized:
        return "non_controlling_interests"
    if any(token in text for token in ("親会社株主", "所有者に帰属", "親会社の所有者")) or "owners of parent" in normalized:
        return "profit_attributable_to_owners"
    if any(token in text for token in ("有利子負債", "借入金", "社債", "コマーシャル・ペーパー")) or any(
        token in normalized for token in ("borrowings", "bonds payable", "commercial papers", "interest-bearing")
    ):
        return "interest_bearing_debt_candidate"
    if any(token in text for token in ("営業活動によるキャッシュ", "営業キャッシュ")) or "operating activities" in normalized:
        return "operating_cash"
    if any(token in text for token in ("投資活動によるキャッシュ", "投資キャッシュ")) or "investing activities" in normalized:
        return "investment_cash"
    if any(token in text for token in ("財務活動によるキャッシュ", "財務キャッシュ")) or "financing activities" in normalized:
        return "financing_cash"
    if any(token in text for token in ("現金及び現金同等物", "現金及び預金")) or "cash and cash equivalents" in normalized:
        return "cash_and_cash_equivalents"
    if any(token in text for token in ("税引前", "経常利益")) or "profit before" in normalized or "ordinary income" in normalized:
        return "ordinary_income_or_profit_before_tax"
    if any(token in text for token in ("営業利益",)) or "operating profit" in normalized or "operating income" in normalized:
        return "operating_income"
    if any(token in text for token in ("売上高", "営業収益", "売上収益")) or "revenue" in normalized or "sales" in normalized:
        return "net_sales"
    if any(token in text for token in ("総資産",)) or "total assets" in normalized:
        return "total_assets"
    if any(token in text for token in ("純資産", "資本合計")) or "net assets" in normalized or "total equity" in normalized:
        return "net_assets"
    if any(token in text for token in ("当期純利益", "四半期純利益")) or "profit" in normalized:
        return "profit_loss"
    return ""
