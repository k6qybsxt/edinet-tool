from __future__ import annotations

import gc
import zipfile
import re
from pathlib import Path
from collections import defaultdict

from edinet_tool.services.zip_loader import list_xbrl_members_in_zip, extract_selected_xbrl
from lxml import etree

_HALF_TOKEN = "jpcrp040300"
_ANNUAL_TOKEN = "jpcrp030000-asr"

def _extract_end_date(path: str) -> str:
    name = Path(path).name
    m = re.search(r"_(\d{4}-\d{2}-\d{2})_\d{2}_\d{4}-\d{2}-\d{2}\.xbrl$", name, re.IGNORECASE)
    return m.group(1) if m else ""


def _is_half_xbrl(path: str) -> bool:
    return _HALF_TOKEN in Path(path).name.lower()


def _is_annual_xbrl(path: str) -> bool:
    return _ANNUAL_TOKEN in Path(path).name.lower()


def _detect_doc_type(path: str) -> str | None:
    name = Path(path).name.lower()
    if _is_half_xbrl(name):
        return "half"
    if _is_annual_xbrl(name):
        return "annual"
    return None


def _fallback_company_key(zip_path: str, xbrl_name: str) -> str:
    zip_stem = Path(zip_path).stem
    m = re.match(r"^([A-Za-z0-9_-]+?)__", Path(xbrl_name).name)
    if m:
        return f"UNKNOWN::{m.group(1)}"
    return f"UNKNOWN::{zip_stem}"

def _get_company_name_from_probe(probe: dict) -> str | None:
    candidate_keys = [
        "FilerNameInJapaneseDEI",
        "CompanyNameInJapaneseDEI",
        "CompanyNameDEI",
        "FilerNameDEI",
    ]
    for key in candidate_keys:
        value = probe.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return None

def _local_tag(tag) -> str:
    return str(tag).split("}")[-1].split(":")[-1] if tag else ""


def _probe_xbrl_identity_from_zip(zip_path: str, member_name: str) -> dict:
    found = {
        "SecurityCodeDEI": None,
        "FilerNameInJapaneseDEI": None,
        "CompanyNameInJapaneseDEI": None,
        "CompanyNameDEI": None,
        "FilerNameDEI": None,
    }

    needed = set(found.keys())

    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(member_name, "r") as src:
            for _event, elem in etree.iterparse(src, events=("end",), recover=True, huge_tree=True):
                local = _local_tag(elem.tag)

                if local in needed:
                    text = elem.text.strip() if elem.text else ""
                    if text and found[local] in (None, ""):
                        found[local] = text

                    if local == "SecurityCodeDEI" and text:
                        found[local] = text[:-1] if len(text) >= 5 else text

                    if found["SecurityCodeDEI"] and (
                        found["FilerNameInJapaneseDEI"]
                        or found["CompanyNameInJapaneseDEI"]
                        or found["CompanyNameDEI"]
                        or found["FilerNameDEI"]
                    ):
                        break

                elem.clear()

    return found

def collect_zip_items(zip_dir: Path, extract_root: str | None = None) -> list[dict]:
    zip_dir = Path(zip_dir)
    extract_dir = Path(extract_root) if extract_root else (zip_dir.parent / "_zip_extracted")
    extract_dir.mkdir(parents=True, exist_ok=True)

    members = list_xbrl_members_in_zip(str(zip_dir))
    items: list[dict] = []

    by_zip: dict[str, list[dict]] = defaultdict(list)
    for row in members:
        by_zip[row["zip_path"]].append(row)

    for zip_path, rows in by_zip.items():
        for row in rows:
            member_name = row["member_name"]
            xbrl_name = row["xbrl_name"]

            doc_type = _detect_doc_type(xbrl_name)
            if doc_type is None:
                continue

            company_code = ""
            company_name = None

            try:
                probe = _probe_xbrl_identity_from_zip(zip_path, member_name)

                security_code = probe.get("SecurityCodeDEI")
                if security_code not in (None, ""):
                    company_code = str(security_code).strip()

                company_name = _get_company_name_from_probe(probe)

            except Exception:
                company_code = ""

            if not company_code:
                company_code = _fallback_company_key(zip_path, xbrl_name)

            items.append(
                {
                    "company_code": company_code,
                    "company_name": company_name,
                    "doc_type": doc_type,
                    "period_end": _extract_end_date(xbrl_name),
                    "zip_path": zip_path,
                    "member_name": member_name,
                    "xbrl_name": xbrl_name,
                    "xbrl_path": "",
                }
            )

    gc.collect()

    items.sort(
        key=lambda x: (
            x["company_code"],
            x["doc_type"],
            x["period_end"],
            x["xbrl_name"],
        ),
        reverse=True,
    )

    return items

def group_zip_items_by_company(zip_items: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)

    for item in zip_items:
        company_code = item.get("company_code") or _fallback_company_key(
            item.get("zip_path", ""),
            item.get("xbrl_name", ""),
        )
        grouped[company_code].append(item)

    return dict(grouped)

def build_company_job(company_code: str, items: list[dict], extract_root: str | None = None) -> dict | None:
    if not items:
        return None

    half_items = [x for x in items if x.get("doc_type") == "half"]
    annual_items = [x for x in items if x.get("doc_type") == "annual"]

    half_items.sort(key=lambda x: (x.get("period_end", ""), x.get("xbrl_name", "")), reverse=True)
    annual_items.sort(key=lambda x: (x.get("period_end", ""), x.get("xbrl_name", "")), reverse=True)

    company_name = None
    for item in items:
        if item.get("company_name"):
            company_name = item["company_name"]
            break

    has_half = len(half_items) >= 1

    if has_half:
        if len(annual_items) < 2:
            return None
        selected = [half_items[0], annual_items[0], annual_items[1]]
    else:
        if len(annual_items) < 3:
            return None
        selected = [annual_items[0], annual_items[1], annual_items[2]]

    extract_base = Path(extract_root) if extract_root else (Path(selected[0]["zip_path"]).parent.parent / "_zip_extracted")
    extract_base.mkdir(parents=True, exist_ok=True)

    extracted_paths = []
    for item in selected:
        out_path = extract_base / item["xbrl_name"]
        extract_selected_xbrl(item["zip_path"], item["member_name"], str(out_path))
        extracted_paths.append(str(out_path))

    source_zips = sorted({x["zip_path"] for x in items if x.get("zip_path")})

    return {
        "company_code": company_code,
        "company_name": company_name,
        "has_half": has_half,
        "file1": extracted_paths[0],
        "file2": extracted_paths[1],
        "file3": extracted_paths[2],
        "source_zips": source_zips,
    }

def build_all_company_jobs(zip_dir: Path, extract_root: str | None = None) -> list[dict]:
    zip_items = collect_zip_items(Path(zip_dir), extract_root=extract_root)
    grouped = group_zip_items_by_company(zip_items)

    jobs: list[dict] = []

    for company_code in sorted(grouped.keys()):
        job = build_company_job(company_code, grouped[company_code], extract_root=extract_root)
        if job is not None:
            jobs.append(job)

    return jobs