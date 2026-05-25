from __future__ import annotations

from dataclasses import dataclass
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
