from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, TypeVar


MAX_DOWNLOAD_WORKERS = 2
T = TypeVar("T")
R = TypeVar("R")


def validate_download_workers(workers: int) -> int:
    normalized = int(workers or 1)
    if normalized not in {1, MAX_DOWNLOAD_WORKERS}:
        raise ValueError(f"download workers must be 1 or {MAX_DOWNLOAD_WORKERS}: {workers}")
    return normalized


def iter_download_waves(items: list[T], *, workers: int) -> list[list[T]]:
    size = validate_download_workers(workers)
    return [items[index:index + size] for index in range(0, len(items), size)]


def run_download_wave(
    jobs: list[T],
    *,
    workers: int,
    job_func: Callable[[T], R],
    executor: ThreadPoolExecutor | None = None,
) -> list[R]:
    target_workers = validate_download_workers(workers)
    if not jobs:
        return []
    if target_workers == 1:
        return [job_func(job) for job in jobs]

    owned_executor = executor is None
    active_executor = executor or ThreadPoolExecutor(max_workers=target_workers)
    try:
        futures: list[Future[R]] = [
            active_executor.submit(job_func, job)
            for job in jobs
        ]
        return [future.result() for future in futures]
    finally:
        if owned_executor:
            active_executor.shutdown()
