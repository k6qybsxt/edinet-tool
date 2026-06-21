from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.db_reflection_preflight_service import (
    DEFAULT_DB_REFLECTION_PREFLIGHT_OUTPUT_DIR,
    DbReflectionPreflightOptions,
    DbReflectionPreflightResult,
    build_db_reflection_preflight,
)
from edinet_monitor.services.preflight_history_service import (
    PreflightHistorySaveResult,
    mark_preflight_history_completed,
    save_preflight_history,
)
from edinet_monitor.services.prevention_catalog_service import (
    DEFAULT_PREVENTION_CATALOG_PATH,
    PreventionCatalogStatusUpdateResult,
    update_prevention_catalog_statuses,
)


TRIGGERABLE_PREVENTION_STATUSES = ("active", "monitoring")
DEFAULT_PREVENTION_CATALOG_TRIGGERS = ("pre_db_reflection",)
CLI_PREVENTION_CATALOG_AREAS: dict[str, tuple[str, ...]] = {
    "save_raw_facts": ("raw_facts", "xbrl_parse", "db_reflection"),
    "save_normalized_metrics": ("normalization", "normalized_metrics", "db_reflection"),
    "save_derived_metrics": ("derived_metrics", "db_reflection"),
    "save_jquants_statements": ("jquants", "db_reflection"),
    "save_jquants_daily_quotes": ("jquants", "market_data", "db_reflection"),
    "save_jquants_listed_info": ("jquants", "issuer_master", "db_reflection"),
    "import_manifest_filings_to_db": ("edinet_download", "filings", "issuer_master", "db_reflection"),
    "collect_document_list_to_db": ("edinet_download", "filings", "db_reflection"),
    "import_tse_listing_master": ("issuer_master", "db_reflection"),
    "run_jquants_backfill": ("jquants", "market_data", "db_reflection"),
    "run_zip_backfill": ("edinet_download", "db_reflection"),
    "run_screening": ("screening", "db_reflection"),
    "run_xbrl_retention_cleanup": ("storage_retention", "db_reflection"),
}


@dataclass(frozen=True)
class DbReflectionPreflightGuardResult:
    preflight: DbReflectionPreflightResult
    trigger_update: PreventionCatalogStatusUpdateResult
    history_save: PreflightHistorySaveResult

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
    print(f"guard_cli_name={preflight.summary.get('guard_cli_name', '')}")
    print(f"command_names={','.join(preflight.summary.get('command_names', ()))}")
    print(f"pending_count={preflight.summary.get('pending_count', 0)}")
    print(f"matched_pending_count={preflight.summary.get('matched_pending_count', 0)}")
    print(f"db_size_gb={preflight.summary.get('db_size_gb', '')}")
    print(f"heavy_db_size_warning_threshold_gb={preflight.summary.get('heavy_db_size_warning_threshold_gb', '')}")
    print(f"long_date_range_warning_days={preflight.summary.get('long_date_range_warning_days', '')}")
    print(f"db_reflection_blocked={preflight.summary.get('db_reflection_blocked', False)}")
    print(f"critical={preflight.counts_by_severity.get('critical', 0)}")
    print(f"warning={preflight.counts_by_severity.get('warning', 0)}")
    print(f"json_path={preflight.json_path}")
    print(f"excel_path={preflight.excel_path}")
    print(f"history_saved={result.history_save.history_saved}")
    print(f"history_status={result.history_save.status}")
    print(f"history_preflight_id={result.history_save.preflight_id}")
    if preflight.counts_by_severity.get("warning", 0) > 0:
        print("preflight_warning=1")
    if result.blocked:
        print("preflight_blocked=critical")


def run_db_reflection_preflight_guard(
    *,
    cli_name: str,
    command_names: tuple[str, ...] | list[str] | None = None,
    catalog_areas: tuple[str, ...] | list[str] | None = None,
    catalog_triggers: tuple[str, ...] | list[str] | None = None,
    db_path: str | Path = DB_PATH,
    catalog_path: str | Path = DEFAULT_PREVENTION_CATALOG_PATH,
    output_dir: str | Path = DEFAULT_DB_REFLECTION_PREFLIGHT_OUTPUT_DIR,
) -> DbReflectionPreflightGuardResult:
    resolved_db_path = Path(db_path)
    resolved_catalog_path = Path(catalog_path)
    resolved_command_names = tuple((cli_name,) if command_names is None else command_names)
    resolved_catalog_areas = tuple(
        CLI_PREVENTION_CATALOG_AREAS.get(cli_name, ("db_reflection",))
        if catalog_areas is None
        else catalog_areas
    )
    resolved_catalog_triggers = tuple(
        DEFAULT_PREVENTION_CATALOG_TRIGGERS
        if catalog_triggers is None
        else catalog_triggers
    )
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
                command_names=resolved_command_names,
                catalog_areas=resolved_catalog_areas,
                catalog_triggers=resolved_catalog_triggers,
            ),
        )
    finally:
        conn.close()

    history_conn = get_connection(resolved_db_path)
    try:
        history_save = save_preflight_history(history_conn, preflight)
    finally:
        history_conn.close()

    trigger_update = update_prevention_catalog_statuses(
        resolved_catalog_path,
        item_ids=tuple(item.item_id for item in preflight.catalog_items),
        from_statuses=TRIGGERABLE_PREVENTION_STATUSES,
        to_status="triggered",
    )
    result = DbReflectionPreflightGuardResult(
        preflight=preflight,
        trigger_update=trigger_update,
        history_save=history_save,
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
    if result is None:
        return None
    db_path = Path(result.preflight.summary.get("db_path") or DB_PATH)
    history_conn = get_connection(db_path)
    try:
        mark_preflight_history_completed(
            history_conn,
            preflight_id=result.preflight.preflight_id,
        )
    finally:
        history_conn.close()
    if not result.triggered_item_ids:
        return None
    return update_prevention_catalog_statuses(
        catalog_path,
        item_ids=result.triggered_item_ids,
        from_statuses=("triggered",),
        to_status="monitoring",
    )
