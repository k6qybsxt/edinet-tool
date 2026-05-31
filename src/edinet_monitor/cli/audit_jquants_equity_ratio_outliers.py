from __future__ import annotations

import argparse

from edinet_monitor.config.settings import DB_PATH, OPERATION_LOG_ROOT
from edinet_monitor.services.jquants_equity_ratio_outlier_service import (
    EquityRatioOutlierAuditResult,
    benchmark_jquants_equity_ratio_outlier_audit,
    run_jquants_equity_ratio_outlier_audit,
    write_jquants_equity_ratio_outlier_tsv,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit J-Quants EquityRatio outliers with one read-only bulk JOIN."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--limit-preview", type=int, default=10)
    parser.add_argument("--verify-serial-equivalence", action="store_true")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def _print_result(result: EquityRatioOutlierAuditResult, *, prefix: str = "") -> None:
    print(f"{prefix}run_id={result.run_id}")
    print(f"{prefix}workers={result.workers}")
    print(f"{prefix}checked_total={result.checked_total}")
    print(f"{prefix}partition_total={result.partition_total}")
    print(f"{prefix}anomaly_total={result.anomaly_total}")
    print(f"{prefix}negative_total={result.negative_total}")
    print(f"{prefix}over_150_percent_total={result.over_150_percent_total}")
    print(f"{prefix}elapsed_seconds={round(result.elapsed_seconds, 3)}")
    print(f"{prefix}db_read_elapsed_seconds={round(result.db_read_elapsed_seconds, 3)}")
    print(f"{prefix}compute_elapsed_seconds={round(result.compute_elapsed_seconds, 3)}")


def _print_preview(result: EquityRatioOutlierAuditResult, *, limit_preview: int) -> None:
    print(
        "company_name\tsecurity_code\tfiscal_year\tperiod_key\tquarter_type\t"
        "period_end\tdisclosed_date\tvalue_num\tvalue_percent\tclassification\t"
        "disclosure_number"
    )
    for row in result.outliers[: max(int(limit_preview), 0)]:
        print(
            "\t".join(
                [
                    row.company_name,
                    row.security_code,
                    str(row.fiscal_year),
                    row.period_key,
                    row.quarter_type,
                    row.period_end,
                    row.disclosed_date,
                    str(row.value_num),
                    f"{row.value_num * 100:.2f}%",
                    row.classification,
                    row.disclosure_number,
                ]
            )
        )


def main() -> None:
    args = build_parser().parse_args()
    if args.verify_serial_equivalence:
        benchmark = benchmark_jquants_equity_ratio_outlier_audit(
            db_path=args.db_path,
            date_from=args.date_from,
            date_to=args.date_to,
            parallel_workers=args.workers,
        )
        print(f"serial_parallel_equivalent={int(benchmark.equivalent)}")
        _print_result(benchmark.serial, prefix="serial_")
        _print_result(benchmark.parallel, prefix="parallel_")
        result = benchmark.parallel
    else:
        result = run_jquants_equity_ratio_outlier_audit(
            db_path=args.db_path,
            date_from=args.date_from,
            date_to=args.date_to,
            workers=args.workers,
        )
        _print_result(result)

    output_path = write_jquants_equity_ratio_outlier_tsv(
        result=result,
        output_dir=args.output_dir,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    print(f"output_path={output_path}")
    _print_preview(result, limit_preview=args.limit_preview)


if __name__ == "__main__":
    main()
