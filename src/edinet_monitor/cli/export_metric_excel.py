from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.metric_excel_export_service import (
    MetricExcelExportResult,
    build_metric_excel_rows,
    read_metric_excel_condition,
    write_metric_excel,
)
from edinet_monitor.services.performance_log_service import PerformanceLog


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export selected metrics from the monitor DB to an Excel workbook."
    )
    parser.add_argument("--condition-xlsx", required=True)
    parser.add_argument("--output-dir", default=r"D:\作業用")
    parser.add_argument("--output-name", default="")
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


def _build_output_path(output_dir: str, output_name: str) -> Path:
    directory = Path(output_dir)
    if output_name:
        return directory / output_name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return directory / f"metric_export_{timestamp}.xlsx"


def main() -> None:
    args = build_arg_parser().parse_args()
    output_path = _build_output_path(args.output_dir, args.output_name)

    create_tables()
    conn = get_connection()
    perf_log = PerformanceLog(
        command_name="export_metric_excel",
        workers=1,
        parameters={
            "condition_xlsx": str(args.condition_xlsx),
            "output_path": str(output_path),
            "limit_preview": args.limit_preview,
        },
    )
    result: MetricExcelExportResult | None = None
    errors: list[str] = []
    warnings: list[str] = []
    target_companies = 0
    output_rows = 0
    writer_summary: dict[str, object] = {}
    unhandled_error: Exception | None = None

    def record_writer_span(
        category: str,
        name: str,
        elapsed: float,
        count: int,
        detail: dict[str, object],
    ) -> None:
        perf_log.add_span(
            category,
            name,
            elapsed_seconds=elapsed,
            count_total=count,
            detail=detail,
        )
        if name == "write_metric_sheets":
            writer_summary.update(detail)

    try:
        with perf_log.measure("file_io", "read_metric_excel_condition"):
            condition = read_metric_excel_condition(args.condition_xlsx)
        with perf_log.measure("compute", "build_metric_excel_rows"):
            rows, errors, warnings, preview_rows, target_companies = build_metric_excel_rows(
                conn,
                condition,
                preview_limit=args.limit_preview,
            )
        output_rows = len(rows)
        path = write_metric_excel(
            rows=rows,
            condition=condition,
            output_path=output_path,
            db_path=DB_PATH,
            errors=errors,
            warnings=warnings,
            target_companies=target_companies,
            span_recorder=record_writer_span,
        )
        result = MetricExcelExportResult(
            output_path=path,
            target_companies=target_companies,
            output_rows=output_rows,
            errors=errors,
            warnings=warnings,
            preview_rows=preview_rows,
        )
    except Exception as e:
        unhandled_error = e
        raise
    finally:
        status = "error" if unhandled_error else ("completed_with_errors" if errors else "success")
        perf_log.finish(
            conn,
            status=status,
            target_total=target_companies,
            success_total=1 if result is not None else 0,
            error_total=len(errors) + (1 if unhandled_error else 0),
            output_rows_total=output_rows,
            error_summary={"unhandled_error": repr(unhandled_error)} if unhandled_error else {"errors": errors},
            summary={
                "target_companies": target_companies,
                "output_rows": output_rows,
                "error_count": len(errors),
                "warning_count": len(warnings),
                "output_path": str(result.output_path) if result is not None else "",
                "writer_mode": "openpyxl_write_only",
                "file_size_bytes": result.output_path.stat().st_size if result is not None else 0,
                "sheet_row_counts": writer_summary.get("sheet_row_counts", {}),
            },
        )
        conn.close()

    if result is None:
        raise RuntimeError("metric excel export did not produce a result")

    print(f"output_path={result.output_path}")
    print(f"target_companies={result.target_companies}")
    print(f"output_rows={result.output_rows}")
    print(f"errors={len(result.errors)}")
    print(f"warnings={len(result.warnings)}")
    for error in result.errors[: args.limit_preview]:
        print(f"error={error}")
    for warning in result.warnings[: args.limit_preview]:
        print(f"warning={warning}")

    print(f"preview_rows={len(result.preview_rows)}")
    for idx, row in enumerate(result.preview_rows, start=1):
        print(f"preview_{idx}={row}")


if __name__ == "__main__":
    main()
