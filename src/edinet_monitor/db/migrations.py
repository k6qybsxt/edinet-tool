from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import sqlite3


@dataclass(frozen=True)
class SchemaMigration:
    migration_id: str
    description: str
    statements: tuple[str, ...] = ()


@dataclass(frozen=True)
class SchemaMigrationStatus:
    migration_id: str
    description: str
    applied: bool
    applied_at: str = ""


SCHEMA_MIGRATIONS: tuple[SchemaMigration, ...] = (
    SchemaMigration(
        migration_id="001_baseline_current_schema",
        description="Record the current schema.py managed SQLite schema as the migration baseline.",
    ),
    SchemaMigration(
        migration_id="002_add_data_quality_report_tables",
        description="Add persisted data quality report run and item tables.",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS data_quality_report_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL UNIQUE,
                generated_at TEXT NOT NULL,
                date_from TEXT NOT NULL,
                date_to TEXT NOT NULL,
                condition_key TEXT NOT NULL,
                codes_json TEXT NOT NULL,
                industry_33_json TEXT NOT NULL,
                output_path TEXT,
                previous_run_id TEXT,
                total_items INTEGER NOT NULL DEFAULT 0,
                issue_count INTEGER NOT NULL DEFAULT 0,
                critical_count INTEGER NOT NULL DEFAULT 0,
                warning_count INTEGER NOT NULL DEFAULT 0,
                info_count INTEGER NOT NULL DEFAULT 0,
                summary_json TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS data_quality_report_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                item_key TEXT NOT NULL,
                category TEXT NOT NULL,
                severity TEXT NOT NULL,
                check_name TEXT NOT NULL,
                subject TEXT NOT NULL,
                current_value REAL,
                previous_value REAL,
                delta_value REAL,
                value_unit TEXT,
                message TEXT NOT NULL,
                detail_json TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(run_id, item_key)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_data_quality_report_runs_condition
            ON data_quality_report_runs(condition_key, generated_at)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_data_quality_report_items_run
            ON data_quality_report_items(run_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_data_quality_report_items_severity
            ON data_quality_report_items(severity, category, check_name)
            """,
        ),
    ),
    SchemaMigration(
        migration_id="003_add_db_reflection_items",
        description="Add DB reflection pending item queue table.",
        statements=(
            """
            CREATE TABLE IF NOT EXISTS db_reflection_items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                category TEXT NOT NULL CHECK (
                    category IN ('schema', 'recalculation', 'data_backfill', 'validation', 'other')
                ),
                description TEXT NOT NULL,
                required_commands_json TEXT NOT NULL,
                verification_sql_json TEXT NOT NULL,
                related_migration_ids_json TEXT NOT NULL,
                source_path TEXT,
                source_key TEXT UNIQUE,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_db_reflection_items_category
            ON db_reflection_items(category, created_at)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_db_reflection_items_source_key
            ON db_reflection_items(source_key)
            """,
        ),
    ),
)


def ensure_schema_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            migration_id TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )


def _schema_migrations_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'schema_migrations'
        """
    ).fetchone()
    return row is not None


def get_applied_migration_ids(conn: sqlite3.Connection) -> set[str]:
    if not _schema_migrations_table_exists(conn):
        return set()
    rows = conn.execute("SELECT migration_id FROM schema_migrations").fetchall()
    return {str(row[0]) for row in rows}


def get_applied_migration_map(conn: sqlite3.Connection) -> dict[str, str]:
    if not _schema_migrations_table_exists(conn):
        return {}
    rows = conn.execute(
        """
        SELECT migration_id, applied_at
        FROM schema_migrations
        """
    ).fetchall()
    return {str(row[0]): str(row[1] or "") for row in rows}


def get_schema_migration_statuses(conn: sqlite3.Connection) -> list[SchemaMigrationStatus]:
    applied = get_applied_migration_map(conn)
    return [
        SchemaMigrationStatus(
            migration_id=migration.migration_id,
            description=migration.description,
            applied=migration.migration_id in applied,
            applied_at=applied.get(migration.migration_id, ""),
        )
        for migration in SCHEMA_MIGRATIONS
    ]


def apply_schema_migrations(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
) -> list[SchemaMigrationStatus]:
    statuses = get_schema_migration_statuses(conn)
    if dry_run:
        return statuses

    ensure_schema_migrations_table(conn)
    applied = get_applied_migration_ids(conn)
    now = datetime.now().isoformat(timespec="seconds")
    for migration in SCHEMA_MIGRATIONS:
        if migration.migration_id in applied:
            continue
        for statement in migration.statements:
            conn.execute(statement)
        conn.execute(
            """
            INSERT INTO schema_migrations (migration_id, description, applied_at)
            VALUES (?, ?, ?)
            """,
            (migration.migration_id, migration.description, now),
        )
        applied.add(migration.migration_id)
    return get_schema_migration_statuses(conn)
