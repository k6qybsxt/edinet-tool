from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.services.db_reflection_preflight_service import (
    DEFAULT_DB_REFLECTION_PREFLIGHT_OUTPUT_DIR,
    DbReflectionPreflightOptions,
    DbReflectionPreflightResult,
    build_db_reflection_preflight,
)
from edinet_monitor.services.prevention_catalog_service import (
    DEFAULT_PREVENTION_CATALOG_PATH,
    PreventionCatalogStatusUpdateResult,
    update_prevention_catalog_statuses,
)


TRIGGERABLE_PREVENTION_STATUSES = ("active", "monitoring")


@dataclass(frozen=True)
class DbReflectionPreflightGuardResult:
    preflight: DbReflectionPreflightResult
    trigger_update: PreventionCatalogStatusUpdateResult

    @property
    def blocked(self) -> bool:
        return bool(self.preflight.summary.get("db_reflection_blocked", False))

    @property
    def triggered_item_ids(self) -> tuple[str, ...]:
        return self.trigger_update.updated_ids


def _get_read_only_connection(db_path: Path) -> sqlite3.Connection:
    uri = db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _print_guard_summary(result: DbReflectionPreflightGuardResult) -> None:
    preflight = result.preflight
    print(f"preflight_id={preflight.preflight_id}")
    print(f"pipeline_failure_policy={preflight.summary.get('pipeline_failure_policy', '')}")
    print(f"db_reflection_blocked={preflight.summary.get('db_reflection_blocked', False)}")
    print(f"critical={preflight.counts_by_severity.get('critical', 0)}")
    print(f"warning={preflight.counts_by_severity.get('warning', 0)}")
    print(f"json_path={preflight.json_path}")
    print(f"excel_path={preflight.excel_path}")
    if preflight.counts_by_severity.get("warning", 0) > 0:
        print("preflight_warning=1")
    if result.blocked:
        print("preflight_blocked=critical")


def run_db_reflection_preflight_guard(
    *,
    cli_name: str,
    db_path: str | Path = DB_PATH,
    catalog_path: str | Path = DEFAULT_PREVENTION_CATALOG_PATH,
    output_dir: str | Path = DEFAULT_DB_REFLECTION_PREFLIGHT_OUTPUT_DIR,
) -> DbReflectionPreflightGuardResult:
    resolved_db_path = Path(db_path)
    resolved_catalog_path = Path(catalog_path)
    conn = _get_read_only_connection(resolved_db_path)
    try:
        preflight = build_db_reflection_preflight(
            conn,
            DbReflectionPreflightOptions(
                db_path=resolved_db_path,
                catalog_path=resolved_catalog_path,
                output_dir=Path(output_dir),
                pipeline_failure_policy="block_on_critical",
                guard_cli_name=cli_name,
            ),
        )
    finally:
        conn.close()

    trigger_update = update_prevention_catalog_statuses(
        resolved_catalog_path,
        item_ids=tuple(item.item_id for item in preflight.catalog_items),
        from_statuses=TRIGGERABLE_PREVENTION_STATUSES,
        to_status="triggered",
    )
    result = DbReflectionPreflightGuardResult(
        preflight=preflight,
        trigger_update=trigger_update,
    )
    _print_guard_summary(result)
    if result.blocked:
        raise SystemExit(2)
    return result


def mark_db_reflection_preflight_guard_success(
    result: DbReflectionPreflightGuardResult | None,
    *,
    catalog_path: str | Path = DEFAULT_PREVENTION_CATALOG_PATH,
) -> PreventionCatalogStatusUpdateResult | None:
    if result is None or not result.triggered_item_ids:
        return None
    return update_prevention_catalog_statuses(
        catalog_path,
        item_ids=result.triggered_item_ids,
        from_statuses=("triggered",),
        to_status="monitoring",
    )
