from __future__ import annotations

import zipfile
from pathlib import Path


def find_xbrl_member_names(zip_path: Path) -> list[str]:
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()

    return [
        name for name in names
        if name.lower().endswith(".xbrl")
    ]


def extract_first_xbrl(zip_path: Path, output_path: Path) -> Path:
    member_names = find_xbrl_member_names(zip_path)

    if not member_names:
        raise RuntimeError(f"xbrl not found in zip: {zip_path}")

    member_name = member_names[0]

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(member_name) as src, open(output_path, "wb") as dst:
            dst.write(src.read())

    return output_path