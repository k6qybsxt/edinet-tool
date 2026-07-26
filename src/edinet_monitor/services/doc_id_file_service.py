from __future__ import annotations

from pathlib import Path


def normalize_doc_ids(*values: str | list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value is None:
            continue
        raw_values = value.split(",") if isinstance(value, str) else value
        for raw_value in raw_values:
            doc_id = str(raw_value or "").strip()
            if not doc_id or doc_id in seen:
                continue
            seen.add(doc_id)
            out.append(doc_id)
    return tuple(out)


def load_doc_ids_file(path: str | Path) -> tuple[str, ...]:
    source = Path(path)
    lines = source.read_text(encoding="utf-8-sig").splitlines()
    return normalize_doc_ids(
        [line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#")]
    )
