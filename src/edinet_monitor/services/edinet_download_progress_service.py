from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Callable

from edinet_monitor.cli.run_zip_backfill import (
    build_chunk_manifest_name,
    iter_manifest_chunks,
)
from edinet_monitor.services.collector.document_filter_service import normalize_form_codes
from edinet_monitor.services.storage.manifest_service import (
    build_manifest_path,
    read_manifest_rows,
    resolve_manifest_prefix_for_form_codes,
    summarize_manifest_rows,
)


@dataclass(frozen=True)
class EdinetDownloadProgressChunk:
    chunk_key: str
    date_from: str
    date_to: str
    manifest_name: str
    manifest_path: Path
    manifest_exists: bool
    manifest_rows: int = 0
    pending_rows: int = 0
    downloaded_rows: int = 0
    error_rows: int = 0
    retryable_error_rows: int = 0
    other_rows: int = 0
    sample_errors: list[dict] = field(default_factory=list)

    @property
    def status(self) -> str:
        if not self.manifest_exists:
            return "MANIFEST_MISSING"
        if self.error_rows or self.pending_rows or self.other_rows:
            return "INCOMPLETE"
        return "COMPLETED"


@dataclass(frozen=True)
class EdinetDownloadProgressResult:
    date_from: str
    date_to: str
    manifest_prefix: str
    manifest_granularity: str
    form_codes: tuple[str, ...]
    chunks: list[EdinetDownloadProgressChunk]
    output_path: Path | None

    @property
    def missing_manifest_chunks(self) -> int:
        return sum(1 for chunk in self.chunks if not chunk.manifest_exists)

    @property
    def incomplete_chunks(self) -> int:
        return sum(1 for chunk in self.chunks if chunk.status != "COMPLETED")

    @property
    def manifest_rows(self) -> int:
        return sum(chunk.manifest_rows for chunk in self.chunks)

    @property
    def pending_rows(self) -> int:
        return sum(chunk.pending_rows for chunk in self.chunks)

    @property
    def downloaded_rows(self) -> int:
        return sum(chunk.downloaded_rows for chunk in self.chunks)

    @property
    def error_rows(self) -> int:
        return sum(chunk.error_rows for chunk in self.chunks)

    @property
    def retryable_error_rows(self) -> int:
        return sum(chunk.retryable_error_rows for chunk in self.chunks)


def export_edinet_download_progress(
    *,
    date_from: str,
    date_to: str,
    manifest_prefix: str = "document_manifest",
    manifest_granularity: str = "month",
    form_codes: tuple[str, ...] | list[str] | str | None = None,
    output_dir: str | Path | None = None,
    manifest_path_builder: Callable[[str], Path] = build_manifest_path,
) -> EdinetDownloadProgressResult:
    start_date = date.fromisoformat(str(date_from).replace("/", "-"))
    end_date = date.fromisoformat(str(date_to).replace("/", "-"))
    target_form_codes = normalize_form_codes(form_codes)
    resolved_prefix = resolve_manifest_prefix_for_form_codes(
        manifest_prefix,
        form_codes=target_form_codes,
    )
    chunks: list[EdinetDownloadProgressChunk] = []

    for chunk in iter_manifest_chunks(start_date, end_date, granularity=manifest_granularity):
        manifest_name = build_chunk_manifest_name(resolved_prefix, chunk.chunk_key)
        manifest_path = manifest_path_builder(manifest_name)
        manifest_exists = manifest_path.exists()
        if manifest_exists:
            summary = summarize_manifest_rows(read_manifest_rows(manifest_path))
            chunks.append(
                EdinetDownloadProgressChunk(
                    chunk_key=chunk.chunk_key,
                    date_from=chunk.start_date.isoformat(),
                    date_to=chunk.end_date.isoformat(),
                    manifest_name=manifest_name,
                    manifest_path=manifest_path,
                    manifest_exists=True,
                    manifest_rows=int(summary["manifest_rows"]),
                    pending_rows=int(summary["pending_rows"]),
                    downloaded_rows=int(summary["downloaded_rows"]),
                    error_rows=int(summary["error_rows"]),
                    retryable_error_rows=int(summary["retryable_error_rows"]),
                    other_rows=int(summary["other_rows"]),
                    sample_errors=list(summary["sample_errors"]),
                )
            )
        else:
            chunks.append(
                EdinetDownloadProgressChunk(
                    chunk_key=chunk.chunk_key,
                    date_from=chunk.start_date.isoformat(),
                    date_to=chunk.end_date.isoformat(),
                    manifest_name=manifest_name,
                    manifest_path=manifest_path,
                    manifest_exists=False,
                )
            )

    output_path = None
    result = EdinetDownloadProgressResult(
        date_from=start_date.isoformat(),
        date_to=end_date.isoformat(),
        manifest_prefix=resolved_prefix,
        manifest_granularity=manifest_granularity,
        form_codes=target_form_codes,
        chunks=chunks,
        output_path=None,
    )
    if output_dir is not None:
        output_path = Path(output_dir) / f"edinet_download_progress_{result.date_from}_to_{result.date_to}_{datetime.now():%Y%m%d_%H%M%S}.txt"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(_format_report(result), encoding="utf-8-sig")
        result = EdinetDownloadProgressResult(
            date_from=result.date_from,
            date_to=result.date_to,
            manifest_prefix=result.manifest_prefix,
            manifest_granularity=result.manifest_granularity,
            form_codes=result.form_codes,
            chunks=result.chunks,
            output_path=output_path,
        )
    return result


def _format_report(result: EdinetDownloadProgressResult) -> str:
    lines = [
        f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"date_from: {result.date_from}",
        f"date_to: {result.date_to}",
        f"manifest_prefix: {result.manifest_prefix}",
        f"manifest_granularity: {result.manifest_granularity}",
        f"form_codes: {','.join(result.form_codes)}",
        f"chunks: {len(result.chunks)}",
        f"missing_manifest_chunks: {result.missing_manifest_chunks}",
        f"incomplete_chunks: {result.incomplete_chunks}",
        f"manifest_rows: {result.manifest_rows}",
        f"downloaded_rows: {result.downloaded_rows}",
        f"pending_rows: {result.pending_rows}",
        f"error_rows: {result.error_rows}",
        f"retryable_error_rows: {result.retryable_error_rows}",
        "",
        "status | chunk | date_from | date_to | manifest | rows | downloaded | pending | error | retryable_error | other",
        "-------+-------+-----------+---------+----------+------+------------+---------+-------+-----------------+------",
    ]
    for chunk in result.chunks:
        lines.append(
            f"{chunk.status} | {chunk.chunk_key} | {chunk.date_from} | {chunk.date_to} | "
            f"{chunk.manifest_name} | {chunk.manifest_rows} | {chunk.downloaded_rows} | "
            f"{chunk.pending_rows} | {chunk.error_rows} | {chunk.retryable_error_rows} | {chunk.other_rows}"
        )
        for error in chunk.sample_errors[:3]:
            lines.append(
                "sample_error | "
                f"{chunk.chunk_key} | doc_id={error.get('doc_id')} | "
                f"company={error.get('company_name')} | type={error.get('download_error_type')} | "
                f"retryable={error.get('download_error_retryable')} | status={error.get('download_http_status')}"
            )
    lines.extend(
        [
            "",
            "resume_hint:",
            "  Re-run run_zip_backfill with the same date range and --download-run-all.",
            "  Add --download-retry-errors when retryable error rows remain.",
        ]
    )
    return "\n".join(lines) + "\n"
