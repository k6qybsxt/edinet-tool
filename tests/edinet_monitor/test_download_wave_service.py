from __future__ import annotations

import sys
import threading
import time
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_monitor.services.collector.download_wave_service import (  # noqa: E402
    iter_download_waves,
    run_download_wave,
    validate_download_workers,
)


class DownloadWaveServiceTest(unittest.TestCase):
    def test_validate_download_workers_rejects_more_than_two(self) -> None:
        self.assertEqual(validate_download_workers(1), 1)
        self.assertEqual(validate_download_workers(2), 2)

        with self.assertRaises(ValueError):
            validate_download_workers(3)

    def test_run_download_wave_workers_two_preserves_input_order_and_runs_in_parallel(self) -> None:
        lock = threading.Lock()
        active = 0
        max_active = 0

        def run_job(value: int) -> int:
            nonlocal active
            nonlocal max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.02)
            with lock:
                active -= 1
            return value * 10

        results = run_download_wave([2, 1], workers=2, job_func=run_job)

        self.assertEqual(results, [20, 10])
        self.assertEqual(max_active, 2)

    def test_iter_download_waves_limits_each_wave_to_workers(self) -> None:
        waves = iter_download_waves([1, 2, 3, 4, 5], workers=2)

        self.assertEqual(waves, [[1, 2], [3, 4], [5]])


if __name__ == "__main__":
    unittest.main()
