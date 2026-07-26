from __future__ import annotations

import contextlib
import io
import sys
import types
import unittest
from unittest import mock

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.cli import (  # noqa: E402
    backfill_2q_outstanding_shares,
    collect_document_list_to_db,
    cleanup_obsolete_half_metrics,
    cleanup_obsolete_quarter_metrics,
    import_manifest_filings_to_db,
    import_tse_listing_master,
    migrate_2q_forecast_metadata,
    rebuild_jquants_metrics_from_raw,
    run_jquants_backfill,
    run_zip_backfill,
    save_derived_metrics,
    save_jquants_daily_quotes,
    save_jquants_listed_info,
    save_jquants_statements,
    save_market_derived_metrics,
    save_normalized_metrics,
    save_raw_facts,
    save_segment_metrics,
)


class _FakeConnection:
    row_factory = None

    def close(self) -> None:
        return None


def _market_result():
    return types.SimpleNamespace(rows=[], missing_quotes=0, warnings=[], output_path="out.txt")


def _segment_result():
    return types.SimpleNamespace(rows=[], candidates=[], saved_rows=0, warnings=[], output_path="out.txt")


def _rebuild_result():
    return types.SimpleNamespace(
        apply=True,
        raw_rows=1,
        metrics_built=1,
        metrics_saved=1,
        skipped_rows=0,
        error_rows=0,
        output_path="out.txt",
    )


def _migration_result():
    return types.SimpleNamespace(
        apply=True,
        annual_derived_candidates=0,
        q2_derived_candidates=0,
        forecast_stage_candidates=0,
        obsolete_forecast_candidates=0,
        annual_derived_updated=0,
        q2_derived_updated=0,
        forecast_stage_updated=0,
        obsolete_forecast_deleted=0,
        output_path="out.txt",
    )


def _jquants_result():
    return types.SimpleNamespace(
        run_id="run",
        fetched_total=1,
        saved_total=1,
        skipped_total=0,
        error_total=0,
        warnings=[],
        output_path="out.txt",
    )


class DbReflectionPreflightGuardCliIntegrationTest(unittest.TestCase):
    def _run_main(self, module, argv: list[str]) -> str:
        stdout = io.StringIO()
        with mock.patch.object(sys, "argv", argv), contextlib.redirect_stdout(stdout):
            module.main()
        return stdout.getvalue()

    def test_save_derived_metrics_always_runs_guard_before_body_and_marks_success(self) -> None:
        order: list[str] = []
        guard_result = object()
        with (
            mock.patch.object(sys, "argv", ["save_derived_metrics"]),
            mock.patch(
                "edinet_monitor.cli.save_derived_metrics.run_db_reflection_preflight_guard",
                side_effect=lambda **_: order.append("guard") or guard_result,
            ) as guard,
            mock.patch(
                "edinet_monitor.cli.save_derived_metrics.run_save_derived_metrics",
                side_effect=lambda **_: order.append("body") or {},
            ) as body,
            mock.patch(
                "edinet_monitor.cli.save_derived_metrics.mark_db_reflection_preflight_guard_success",
                side_effect=lambda result: order.append("mark"),
            ) as mark,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            save_derived_metrics.main()

        guard.assert_called_once()
        body.assert_called_once()
        mark.assert_called_once_with(guard_result)
        self.assertEqual(order, ["guard", "body", "mark"])

    def test_apply_clis_run_guard_before_write_body_and_mark_success(self) -> None:
        cases = [
            (
                save_market_derived_metrics,
                "edinet_monitor.cli.save_market_derived_metrics",
                ["save_market_derived_metrics", "--apply"],
                "save_market_derived_metrics",
                _market_result(),
            ),
            (
                save_segment_metrics,
                "edinet_monitor.cli.save_segment_metrics",
                ["save_segment_metrics", "--apply"],
                "save_segment_metrics",
                _segment_result(),
            ),
            (
                rebuild_jquants_metrics_from_raw,
                "edinet_monitor.cli.rebuild_jquants_metrics_from_raw",
                [
                    "rebuild_jquants_metrics_from_raw",
                    "--date-from",
                    "2026-01-01",
                    "--date-to",
                    "2026-01-31",
                    "--apply",
                ],
                "rebuild_jquants_financial_metrics_from_raw",
                _rebuild_result(),
            ),
            (
                backfill_2q_outstanding_shares,
                "edinet_monitor.cli.backfill_2q_outstanding_shares",
                ["backfill_2q_outstanding_shares", "--apply"],
                "backfill_2q_outstanding_shares",
                {
                    "apply": True,
                    "target_docs": 1,
                    "candidate_actions": 1,
                    "ok_rows": 1,
                    "total_rows": 1,
                    "ok_rate": 1.0,
                    "report_path": "out.txt",
                },
            ),
            (
                migrate_2q_forecast_metadata,
                "edinet_monitor.cli.migrate_2q_forecast_metadata",
                ["migrate_2q_forecast_metadata", "--apply"],
                "migrate_quarter_forecast_metadata",
                _migration_result(),
            ),
        ]
        for module, module_path, argv, body_name, result in cases:
            with self.subTest(module=module_path):
                order: list[str] = []
                guard_result = object()
                with (
                    mock.patch(
                        f"{module_path}.run_db_reflection_preflight_guard",
                        side_effect=lambda **_: order.append("guard") or guard_result,
                    ) as guard,
                    mock.patch(f"{module_path}.create_tables", side_effect=lambda: order.append("create")),
                    mock.patch(f"{module_path}.get_connection", return_value=_FakeConnection()),
                    mock.patch(
                        f"{module_path}.{body_name}",
                        side_effect=lambda *args, **kwargs: order.append("body") or result,
                    ) as body,
                    mock.patch(
                        f"{module_path}.mark_db_reflection_preflight_guard_success",
                        side_effect=lambda guard_result: order.append("mark"),
                    ) as mark,
                ):
                    self._run_main(module, argv)

                guard.assert_called_once()
                body.assert_called_once()
                mark.assert_called_once_with(guard_result)
                self.assertEqual(order[0], "guard")
                self.assertIn("body", order)
                self.assertEqual(order[-1], "mark")

    def test_save_segment_metrics_does_not_mark_guard_when_target_has_no_saved_rows(self) -> None:
        guard_result = object()
        with (
            mock.patch.object(sys, "argv", ["save_segment_metrics", "--doc-id", "S100EMPTY", "--apply"]),
            mock.patch(
                "edinet_monitor.cli.save_segment_metrics.run_db_reflection_preflight_guard",
                return_value=guard_result,
            ),
            mock.patch("edinet_monitor.cli.save_segment_metrics.create_tables"),
            mock.patch("edinet_monitor.cli.save_segment_metrics.get_connection", return_value=_FakeConnection()),
            mock.patch(
                "edinet_monitor.cli.save_segment_metrics.save_segment_metrics",
                return_value=_segment_result(),
            ),
            mock.patch(
                "edinet_monitor.cli.save_segment_metrics.mark_db_reflection_preflight_guard_success"
            ) as mark,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            with self.assertRaisesRegex(RuntimeError, "No segment metrics were saved"):
                save_segment_metrics.main()

        mark.assert_not_called()

    def test_cleanup_apply_clis_run_guard_before_delete_and_mark_success(self) -> None:
        cases = [
            (
                cleanup_obsolete_quarter_metrics,
                "edinet_monitor.cli.cleanup_obsolete_quarter_metrics",
                ["cleanup_obsolete_quarter_metrics", "--db-path", "dummy.db", "--apply"],
                "delete_obsolete_quarter_metrics",
                "count_obsolete_quarter_metrics",
            ),
            (
                cleanup_obsolete_half_metrics,
                "edinet_monitor.cli.cleanup_obsolete_half_metrics",
                ["cleanup_obsolete_half_metrics", "--db-path", "dummy.db", "--apply"],
                "delete_obsolete_half_metrics",
                "count_obsolete_half_metrics",
            ),
        ]
        for module, module_path, argv, delete_name, count_name in cases:
            with self.subTest(module=module_path):
                order: list[str] = []
                guard_result = object()
                count_rows = [{"row_count": 1, "table_name": "t", "target_scope": "s", "metric_base": "m", "metric_source": "s", "metric_key": "m"}]
                with (
                    mock.patch(
                        f"{module_path}.run_db_reflection_preflight_guard",
                        side_effect=lambda **_: order.append("guard") or guard_result,
                    ) as guard,
                    mock.patch(f"{module_path}.get_connection", return_value=_FakeConnection()),
                    mock.patch(f"{module_path}.{count_name}", return_value=count_rows),
                    mock.patch(
                        f"{module_path}.{delete_name}",
                        side_effect=lambda conn: order.append("body") or 1,
                    ) as body,
                    mock.patch(
                        f"{module_path}.mark_db_reflection_preflight_guard_success",
                        side_effect=lambda guard_result: order.append("mark"),
                    ) as mark,
                ):
                    self._run_main(module, argv)

                guard.assert_called_once()
                body.assert_called_once()
                mark.assert_called_once_with(guard_result)
                self.assertEqual(order, ["guard", "body", "mark"])

    def test_normal_pipeline_save_clis_run_guard_before_body_and_mark_success(self) -> None:
        cases = [
            (
                save_raw_facts,
                "edinet_monitor.cli.save_raw_facts",
                ["save_raw_facts"],
                "run_save_raw_facts",
                {},
            ),
            (
                save_normalized_metrics,
                "edinet_monitor.cli.save_normalized_metrics",
                ["save_normalized_metrics"],
                "run_save_normalized_metrics",
                {},
            ),
            (
                import_manifest_filings_to_db,
                "edinet_monitor.cli.import_manifest_filings_to_db",
                ["import_manifest_filings_to_db", "--manifest-name", "m"],
                "run_import_manifest_filings_to_db",
                {},
            ),
        ]
        for module, module_path, argv, body_name, result in cases:
            with self.subTest(module=module_path):
                order: list[str] = []
                guard_result = object()
                with (
                    mock.patch(
                        f"{module_path}.run_db_reflection_preflight_guard",
                        side_effect=lambda **_: order.append("guard") or guard_result,
                    ) as guard,
                    mock.patch(
                        f"{module_path}.{body_name}",
                        side_effect=lambda *args, **kwargs: order.append("body") or result,
                    ) as body,
                    mock.patch(
                        f"{module_path}.mark_db_reflection_preflight_guard_success",
                        side_effect=lambda result: order.append("mark"),
                    ) as mark,
                ):
                    self._run_main(module, argv)

                guard.assert_called_once()
                body.assert_called_once()
                mark.assert_called_once_with(guard_result)
                self.assertEqual(order, ["guard", "body", "mark"])

    def test_jquants_and_master_clis_run_guard_before_db_write_and_mark_success(self) -> None:
        cases = [
            (
                save_jquants_statements,
                "edinet_monitor.cli.save_jquants_statements",
                ["save_jquants_statements", "--date-from", "2026-01-01", "--date-to", "2026-01-31"],
                "save_jquants_statements",
                _jquants_result(),
            ),
            (
                save_jquants_daily_quotes,
                "edinet_monitor.cli.save_jquants_daily_quotes",
                ["save_jquants_daily_quotes", "--date-from", "2026-01-01", "--date-to", "2026-01-31"],
                "save_jquants_daily_quotes",
                _jquants_result(),
            ),
            (
                save_jquants_listed_info,
                "edinet_monitor.cli.save_jquants_listed_info",
                ["save_jquants_listed_info", "--date", "2026-01-31"],
                "save_jquants_listed_info",
                _jquants_result(),
            ),
            (
                import_tse_listing_master,
                "edinet_monitor.cli.import_tse_listing_master",
                ["import_tse_listing_master"],
                "upsert_issuers",
                1,
            ),
        ]
        for module, module_path, argv, body_name, result in cases:
            with self.subTest(module=module_path):
                order: list[str] = []
                guard_result = object()
                with (
                    mock.patch(
                        f"{module_path}.run_db_reflection_preflight_guard",
                        side_effect=lambda **_: order.append("guard") or guard_result,
                    ) as guard,
                    mock.patch(f"{module_path}.create_tables", side_effect=lambda: order.append("create")),
                    mock.patch(f"{module_path}.get_connection", return_value=_FakeConnection()),
                    mock.patch(f"{module_path}.load_csv_rows", return_value=[{"edinet_code": "E1"}], create=True),
                    mock.patch(
                        f"{module_path}.{body_name}",
                        side_effect=lambda *args, **kwargs: order.append("body") or result,
                    ) as body,
                    mock.patch(
                        f"{module_path}.mark_db_reflection_preflight_guard_success",
                        side_effect=lambda result: order.append("mark"),
                    ) as mark,
                ):
                    self._run_main(module, argv)

                guard.assert_called_once()
                body.assert_called_once()
                mark.assert_called_once_with(guard_result)
                self.assertEqual(order[0], "guard")
                self.assertIn("body", order)
                self.assertEqual(order[-1], "mark")

    def test_collect_and_zip_backfill_clis_run_guard_before_body_and_mark_success(self) -> None:
        cases = [
            (
                collect_document_list_to_db,
                "edinet_monitor.cli.collect_document_list_to_db",
                ["collect_document_list_to_db", "--target-date", "2026-01-31"],
                "collect_document_list_for_dates",
            ),
            (
                run_zip_backfill,
                "edinet_monitor.cli.run_zip_backfill",
                ["run_zip_backfill", "--date-from", "2026-01-01", "--date-to", "2026-01-31"],
                "run_zip_backfill",
            ),
        ]
        for module, module_path, argv, body_name in cases:
            with self.subTest(module=module_path):
                order: list[str] = []
                guard_result = object()
                with (
                    mock.patch(f"{module_path}.validate_edinet_api_key", return_value="dummy"),
                    mock.patch(f"{module_path}.resolve_target_dates", return_value=[], create=True),
                    mock.patch(
                        f"{module_path}.run_db_reflection_preflight_guard",
                        side_effect=lambda **_: order.append("guard") or guard_result,
                    ) as guard,
                    mock.patch(
                        f"{module_path}.{body_name}",
                        side_effect=lambda *args, **kwargs: order.append("body") or {},
                    ) as body,
                    mock.patch(
                        f"{module_path}.mark_db_reflection_preflight_guard_success",
                        side_effect=lambda result: order.append("mark"),
                    ) as mark,
                ):
                    self._run_main(module, argv)

                guard.assert_called_once()
                body.assert_called_once()
                mark.assert_called_once_with(guard_result)
                self.assertEqual(order, ["guard", "body", "mark"])

    def test_run_jquants_backfill_guards_only_enabled_stages(self) -> None:
        order: list[str] = []
        with (
            mock.patch.object(
                sys,
                "argv",
                [
                    "run_jquants_backfill",
                    "--date-from",
                    "2026-01-01",
                    "--date-to",
                    "2026-01-31",
                    "--skip-statements",
                    "--include-listed-info",
                ],
            ),
            mock.patch(
                "edinet_monitor.cli.run_jquants_backfill.run_db_reflection_preflight_guard",
                side_effect=lambda **kwargs: order.append(f"guard:{','.join(kwargs.get('command_names', ()))}") or object(),
            ) as guard,
            mock.patch("edinet_monitor.cli.run_jquants_backfill.create_tables", side_effect=lambda: order.append("create")),
            mock.patch("edinet_monitor.cli.run_jquants_backfill.get_connection", return_value=_FakeConnection()),
            mock.patch("edinet_monitor.cli.run_jquants_backfill.save_jquants_daily_quotes", side_effect=lambda *args, **kwargs: order.append("quotes") or _jquants_result()),
            mock.patch("edinet_monitor.cli.run_jquants_backfill.save_jquants_listed_info", side_effect=lambda *args, **kwargs: order.append("listed") or _jquants_result()),
            mock.patch("edinet_monitor.cli.run_jquants_backfill.save_jquants_statements") as statements,
            mock.patch("edinet_monitor.cli.run_jquants_backfill.export_jquants_coverage", return_value=types.SimpleNamespace(output_path="coverage.txt")),
            mock.patch(
                "edinet_monitor.cli.run_jquants_backfill.mark_db_reflection_preflight_guard_success",
                side_effect=lambda result: order.append("mark"),
            ) as mark,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            run_jquants_backfill.main()

        self.assertEqual(guard.call_count, 2)
        statements.assert_not_called()
        self.assertEqual(mark.call_count, 2)
        self.assertIn("guard:run_jquants_backfill,save_jquants_daily_quotes", order)
        self.assertIn("guard:run_jquants_backfill,save_jquants_listed_info", order)
        self.assertNotIn("guard:run_jquants_backfill,save_jquants_statements", order)
        self.assertLess(order.index("guard:run_jquants_backfill,save_jquants_daily_quotes"), order.index("create"))

    def test_dry_run_apply_cli_does_not_run_guard(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["save_market_derived_metrics"]),
            mock.patch("edinet_monitor.cli.save_market_derived_metrics.run_db_reflection_preflight_guard") as guard,
            mock.patch("edinet_monitor.cli.save_market_derived_metrics.get_connection", return_value=_FakeConnection()),
            mock.patch("edinet_monitor.cli.save_market_derived_metrics.save_market_derived_metrics", return_value=_market_result()),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            save_market_derived_metrics.main()

        guard.assert_not_called()

    def test_critical_guard_exit_prevents_apply_body(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["save_market_derived_metrics", "--apply"]),
            mock.patch(
                "edinet_monitor.cli.save_market_derived_metrics.run_db_reflection_preflight_guard",
                side_effect=SystemExit(2),
            ),
            mock.patch("edinet_monitor.cli.save_market_derived_metrics.create_tables") as create_tables,
            mock.patch("edinet_monitor.cli.save_market_derived_metrics.save_market_derived_metrics") as body,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            with self.assertRaises(SystemExit) as context:
                save_market_derived_metrics.main()

        self.assertEqual(context.exception.code, 2)
        create_tables.assert_not_called()
        body.assert_not_called()


if __name__ == "__main__":
    unittest.main()
