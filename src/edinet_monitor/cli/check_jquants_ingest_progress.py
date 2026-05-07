from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from edinet_monitor.db.schema import create_tables, get_connection


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export J-Quants ingest progress by date/code.")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--status", choices=["all", "running", "completed", "error"], default="all")
    parser.add_argument("--target-kind", choices=["all", "date", "code"], default="date")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--output-dir", default="D:\\\u4f5c\u696d\u7528")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        rows = _fetch_progress(
            conn,
            date_from=args.date_from,
            date_to=args.date_to,
            status=args.status,
            target_kind=args.target_kind,
            limit=args.limit,
        )
        output_path = _write_report(
            rows,
            date_from=args.date_from,
            date_to=args.date_to,
            status=args.status,
            target_kind=args.target_kind,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"output_path={output_path}")
    print(f"rows={len(rows)}")
    print(f"running={sum(1 for row in rows if row['status'] == 'running')}")
    print(f"error={sum(1 for row in rows if row['status'] == 'error')}")


def _fetch_progress(conn, *, date_from: str, date_to: str, status: str, target_kind: str, limit: int):
    where = ["target_value BETWEEN ? AND ?"]
    params: list[object] = [date_from, date_to]
    if status != "all":
        where.append("status = ?")
        params.append(status)
    if target_kind != "all":
        where.append("target_kind = ?")
        params.append(target_kind)
    params.append(limit)
    return conn.execute(
        f"""
        SELECT run_id, run_type, target_kind, target_value, status,
               fetched_count, saved_count, skipped_count, error_message,
               started_at, finished_at, updated_at
        FROM jquants_ingest_progress
        WHERE {' AND '.join(where)}
        ORDER BY target_value, run_type, started_at
        LIMIT ?
        """,
        params,
    ).fetchall()


def _write_report(rows, *, date_from: str, date_to: str, status: str, target_kind: str, output_dir: str) -> Path:
    generated_at = datetime.now()
    path = Path(output_dir) / f"jquants_ingest_progress_{date_from}_to_{date_to}_{generated_at:%Y%m%d_%H%M%S}.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"generated_at: {generated_at.isoformat(timespec='seconds')}",
        f"date_from: {date_from}",
        f"date_to: {date_to}",
        f"status_filter: {status}",
        f"target_kind_filter: {target_kind}",
        f"rows: {len(rows)}",
        f"running: {sum(1 for row in rows if row['status'] == 'running')}",
        f"error: {sum(1 for row in rows if row['status'] == 'error')}",
        "",
        "run_type | target | status | fetched | saved | skipped | started_at | finished_at | updated_at | run_id | error",
        "---------+--------+--------+---------+-------+---------+------------+-------------+------------+--------+------",
    ]
    for row in rows:
        lines.append(
            f"{row['run_type']} | {row['target_kind']}:{row['target_value']} | {row['status']} | "
            f"{row['fetched_count']} | {row['saved_count']} | {row['skipped_count']} | "
            f"{row['started_at'] or ''} | {row['finished_at'] or ''} | {row['updated_at'] or ''} | "
            f"{row['run_id']} | {row['error_message'] or ''}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


if __name__ == "__main__":
    main()
