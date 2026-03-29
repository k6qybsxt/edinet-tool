from __future__ import annotations

from pathlib import Path

from edinet_pipeline.services.xbrl_parser import parse_xbrl_file_raw


def parse_xbrl_to_raw(path: Path) -> dict:
    parsed = parse_xbrl_file_raw(path=str(path), mode="full", logger=None)
    return parsed