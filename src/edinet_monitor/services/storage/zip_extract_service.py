from __future__ import annotations

from dataclasses import dataclass
import zipfile
from pathlib import Path


@dataclass(frozen=True)
class XbrlExtractionResult:
    output_path: Path
    member_name: str


def find_xbrl_member_names(zip_path: Path) -> list[str]:
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()

    return [
        name for name in names
        if name.lower().endswith(".xbrl")
    ]


def _normalize_member_name(member_name: str) -> str:
    return str(member_name or "").replace("\\", "/")


def _is_public_doc_member(member_name: str) -> bool:
    normalized = _normalize_member_name(member_name)
    return "/XBRL/PublicDoc/" in f"/{normalized}"


def _is_annual_securities_report_member(member_name: str) -> bool:
    filename = Path(_normalize_member_name(member_name)).name.lower()
    return filename.startswith("jpcrp030000-asr") and filename.endswith(".xbrl")


def choose_preferred_xbrl_member(member_names: list[str]) -> str:
    if not member_names:
        raise RuntimeError("xbrl not found in zip")

    def sort_key(member_name: str) -> tuple[int, str]:
        public_doc = _is_public_doc_member(member_name)
        annual_report = _is_annual_securities_report_member(member_name)
        if public_doc and annual_report:
            priority = 0
        elif annual_report:
            priority = 1
        elif public_doc:
            priority = 2
        else:
            priority = 3
        return priority, _normalize_member_name(member_name).lower()

    return sorted(member_names, key=sort_key)[0]


def extract_preferred_xbrl(zip_path: Path, output_path: Path) -> XbrlExtractionResult:
    member_names = find_xbrl_member_names(zip_path)

    if not member_names:
        raise RuntimeError(f"xbrl not found in zip: {zip_path}")

    member_name = choose_preferred_xbrl_member(member_names)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(member_name) as src, open(output_path, "wb") as dst:
            dst.write(src.read())

    return XbrlExtractionResult(output_path=output_path, member_name=member_name)


def extract_first_xbrl(zip_path: Path, output_path: Path) -> Path:
    result = extract_preferred_xbrl(zip_path, output_path)
    return output_path
