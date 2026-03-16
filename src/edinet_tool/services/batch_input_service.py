from __future__ import annotations

import gc
import re
from pathlib import Path
from collections import defaultdict

from edinet_tool.services.zip_loader import collect_xbrl_from_zip
from edinet_tool.services.xbrl_parser import parse_xbrl_file

_HALF_TOKEN = "jpcrp040300"
_ANNUAL_TOKEN = "jpcrp030000-asr"


class _SilentLogger:

    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


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

def _get_company_name_from_out(out: dict) -> str | None:
    candidate_keys = [
        "FilerNameInJapaneseDEI",
        "CompanyNameInJapaneseDEI",
        "CompanyNameDEI",
        "FilerNameDEI",
    ]
    for key in candidate_keys:
        value = out.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return None


def collect_zip_items(zip_dir: Path, extract_root: str | None = None) -> list[dict]:
    zip_dir = Path(zip_dir)

    extract_dir = Path(extract_root) if extract_root else (zip_dir.parent / "_zip_extracted")
    xbrl_paths = collect_xbrl_from_zip(str(zip_dir), str(extract_dir))

    extracted = []
    for xbrl_path in xbrl_paths:
        extracted.append(
            {
                "zip_path": "",
                "xbrl_path": str(xbrl_path),
                "xbrl_name": Path(xbrl_path).name,
            }
        )

    items: list[dict] = []

    silent_logger = _SilentLogger()

    for row in extracted:
        xbrl_path = row["xbrl_path"]
        xbrl_name = row["xbrl_name"]
        zip_path = row["zip_path"]

        doc_type = _detect_doc_type(xbrl_name)
        if doc_type is None:
            continue

        mode = "half" if doc_type == "half" else "full"

        company_code = ""
        company_name = None

        try:
            out, security_code, _out_meta = parse_xbrl_file(
                xbrl_path,
                mode=mode,
                logger=silent_logger,
            )

            if security_code not in (None, ""):
                company_code = str(security_code).strip()

            if isinstance(out, dict):
                company_name = _get_company_name_from_out(out)

        except Exception:
            continue

        if not company_code:
            company_code = _fallback_company_key(zip_path, xbrl_name)

        items.append(
            {
                "company_code": company_code,
                "company_name": company_name,
                "doc_type": doc_type,
                "period_end": _extract_end_date(xbrl_name),
                "xbrl_path": xbrl_path,
                "zip_path": zip_path,
                "xbrl_name": xbrl_name,
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

def build_company_job(company_code: str, items: list[dict]) -> dict | None:
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
        
        file1 = half_items[0]["xbrl_path"]
        file2 = annual_items[0]["xbrl_path"]
        file3 = annual_items[1]["xbrl_path"]
    else:
        if len(annual_items) < 3:
            return None
        
        file1 = annual_items[0]["xbrl_path"]
        file2 = annual_items[1]["xbrl_path"]
        file3 = annual_items[2]["xbrl_path"]

    source_zips = sorted({x["zip_path"] for x in items if x.get("zip_path")})

    return {
        "company_code": company_code,
        "company_name": company_name,
        "has_half": has_half,
        "file1": file1,
        "file2": file2,
        "file3": file3,
        "source_zips": source_zips,
    }


def build_all_company_jobs(zip_dir: Path, extract_root: str | None = None) -> list[dict]:
    zip_items = collect_zip_items(Path(zip_dir), extract_root=extract_root)
    grouped = group_zip_items_by_company(zip_items)

    jobs: list[dict] = []

    for company_code in sorted(grouped.keys()):
        job = build_company_job(company_code, grouped[company_code])
        if job is not None:
            jobs.append(job)

    return jobs