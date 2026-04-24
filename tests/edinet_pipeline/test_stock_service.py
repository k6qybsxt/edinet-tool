from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_pipeline.config.settings import CACHE_ROOT  # noqa: E402
from edinet_pipeline.services import stock_service  # noqa: E402


class StockServiceTest(unittest.TestCase):
    def test_stock_cache_dir_uses_pipeline_cache_root(self) -> None:
        self.assertEqual(
            stock_service._STOCK_CACHE_DIR,
            CACHE_ROOT / "stock",
        )


if __name__ == "__main__":
    unittest.main()
