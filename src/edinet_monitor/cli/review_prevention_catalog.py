from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.services.prevention_catalog_service import (
    ACTIVE_PREVENTION_STATUSES,
    ALLOWED_PREVENTION_STATUSES,
    DEFAULT_PREVENTION_CATALOG_PATH,
    DEFAULT_PREVENTION_CATALOG_REVIEW_OUTPUT_DIR,
    PreventionCatalogReviewOptions,
    review_prevention_catalog,
)


def _split_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in str(value or "").split(",") if part.strip())


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review relevant prevention catalog items before implementation or DB reflection."
    )
    parser.add_argument("--catalog-path", default=str(DEFAULT_PREVENTION_CATALOG_PATH))
    parser.add_argument("--areas", default="")
    parser.add_argument("--triggers", default="")
    parser.add_argument("--statuses", default="")
    parser.add_argument("--include-retired", action="store_true")
    parser.add_argument("--output-dir", default=str(DEFAULT_PREVENTION_CATALOG_REVIEW_OUTPUT_DIR))
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


def _selected_statuses(statuses_text: str, include_retired: bool) -> tuple[str, ...]:
    explicit = _split_csv(statuses_text)
    if explicit:
        return explicit
    if include_retired:
        return tuple(sorted(ALLOWED_PREVENTION_STATUSES))
    return ACTIVE_PREVENTION_STATUSES


def main() -> None:
    args = build_arg_parser().parse_args()
    result = review_prevention_catalog(
        PreventionCatalogReviewOptions(
            catalog_path=Path(args.catalog_path),
            areas=_split_csv(args.areas),
            triggers=_split_csv(args.triggers),
            statuses=_selected_statuses(args.statuses, bool(args.include_retired)),
            output_dir=Path(args.output_dir),
        )
    )

    print(f"review_id={result.review_id}")
    print(f"status={result.status}")
    print(f"matched_count={len(result.matched_items)}")
    print(f"critical={result.counts_by_severity.get('critical', 0)}")
    print(f"warning={result.counts_by_severity.get('warning', 0)}")
    print(f"info={result.counts_by_severity.get('info', 0)}")
    print(f"json_path={result.json_path}")
    print(f"excel_path={result.excel_path}")
    preview_limit = max(int(args.limit_preview), 0)
    for item in result.matched_items[:preview_limit]:
        print(
            "item="
            f"{item.severity}|{item.status}|{item.item_id}|{item.title}|"
            f"{','.join(item.areas)}|{','.join(item.triggers)}"
        )


if __name__ == "__main__":
    main()
