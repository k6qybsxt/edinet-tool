from __future__ import annotations
import gc

import re
import zipfile
from pathlib import Path
from collections import defaultdict

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


def _extract_target_xbrl_from_zip(zip_dir: Path, extract_root: str | None = None) -> list[dict]:
    extract_dir = Path(extract_root) if extract_root else (zip_dir.parent / "_zip_extracted")
    extract_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []

    zip_files = sorted(zip_dir.glob("*.zip"))
    print(f"[batch debug] zip_dir={zip_dir}")
    print(f"[batch debug] zip_files={len(zip_files)}")

    for zip_path in zip_files:
        picked = 0
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                names = z.namelist()
                print(f"[batch debug] zip={zip_path.name} entries={len(names)}")

                for name in names:
                    lower = name.lower()

                    if not lower.endswith(".xbrl"):
                        continue

                    if "/auditdoc/" in lower or "\\auditdoc\\" in lower:
                        continue

                    if _HALF_TOKEN not in lower and _ANNUAL_TOKEN not in lower:
                        continue

                    inner_name = Path(name).name
                    safe_zip_stem = re.sub(r'[\\/:*?"<>|]', "_", zip_path.stem)
                    out_name = f"{safe_zip_stem}__{inner_name}"
                    out_path = extract_dir / out_name

                    data = z.read(name)
                    with open(out_path, "wb") as dst:
                        dst.write(data)

                    rows.append(
                        {
                            "zip_path": str(zip_path),
                            "xbrl_path": str(out_path),
                            "xbrl_name": inner_name,
                        }
                    )
                    picked += 1

            print(f"[batch debug] zip={zip_path.name} picked={picked}")

        except Exception as e:
            print(f"[batch debug] zip_error={zip_path.name} error={e}")
            continue

    print(f"[batch debug] extracted_rows={len(rows)}")
    return rows

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

    extracted = _extract_target_xbrl_from_zip(zip_dir, extract_root=extract_root)
    items: list[dict] = []

    silent_logger = _SilentLogger()

    for row in extracted:
        xbrl_path = row["xbrl_path"]
        xbrl_name = row["xbrl_name"]
        zip_path = row["zip_path"]

        doc_type = _detect_doc_type(xbrl_name)
        if doc_type is None:
            print(f"[batch debug] skip_doc_type_none xbrl={xbrl_name}")
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

        except Exception as e:
            print(f"[batch debug] parse_fail xbrl={xbrl_name} mode={mode} error={e}")

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

    print(f"[batch debug] zip_items={len(items)}")

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

    print(f"[batch debug] grouped_companies={len(grouped)}")
    for company_code, items in sorted(grouped.items()):
        half_n = sum(1 for x in items if x.get("doc_type") == "half")
        annual_n = sum(1 for x in items if x.get("doc_type") == "annual")
        print(f"[batch debug] group code={company_code} half={half_n} annual={annual_n}")

    return dict(grouped)


def build_company_job(company_code: str, items: list[dict]) -> dict | None:
    if not items:
        print(f"[batch debug] build_job_empty code={company_code}")
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
            print(f"[batch debug] skip_job code={company_code} reason=half_mode_annual_short annual={len(annual_items)}")
            return None
        file1 = half_items[0]["xbrl_path"]
        file2 = annual_items[0]["xbrl_path"]
        file3 = annual_items[1]["xbrl_path"]
    else:
        if len(annual_items) < 3:
            print(f"[batch debug] skip_job code={company_code} reason=annual_only_short annual={len(annual_items)}")
            return None
        file1 = annual_items[0]["xbrl_path"]
        file2 = annual_items[1]["xbrl_path"]
        file3 = annual_items[2]["xbrl_path"]

    source_zips = sorted({x["zip_path"] for x in items if x.get("zip_path")})

    print(f"[batch debug] job_ready code={company_code} has_half={has_half}")

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

    print(f"[batch debug] jobs={len(jobs)}")
    return jobs