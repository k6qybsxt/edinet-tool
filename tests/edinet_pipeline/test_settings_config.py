from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edinet_pipeline.config import settings as pipeline_settings  # noqa: E402


class SettingsConfigTest(unittest.TestCase):
    def test_default_config_file_exists(self) -> None:
        self.assertTrue(pipeline_settings.DEFAULT_CONFIG_PATH.exists())

    def test_load_pipeline_settings_supports_external_config_and_env_overrides(self) -> None:
        temp_config_path = ROOT_DIR / "tests" / "_tmp_edinet_pipeline_config.json"
        try:
            temp_config_path.write_text(
                json.dumps(
                    {
                        "log_mode": "NORMAL",
                        "data_root": "E:\\PipelineData",
                        "template_dir": "templates",
                        "template_workbook_name": "決算分析シート_1.xlsm"
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            loaded = pipeline_settings.load_pipeline_settings(
                env={
                    "EDINET_PIPELINE_CONFIG": str(temp_config_path),
                    "EDINET_PIPELINE_LOG_DIR": "logs/test-edinet-pipeline",
                },
                include_local_config=False,
            )

            self.assertEqual(loaded["log_mode"], "NORMAL")
            self.assertEqual(loaded["data_root"], Path(r"E:\PipelineData"))
            self.assertEqual(
                loaded["log_dir"],
                ROOT_DIR / "logs" / "test-edinet-pipeline",
            )
            self.assertEqual(
                loaded["template_dir"],
                ROOT_DIR / "templates",
            )
            self.assertEqual(
                loaded["template_workbook_name"],
                "決算分析シート_1.xlsm",
            )
            self.assertEqual(
                loaded["active_config_path"],
                str(temp_config_path),
            )
        finally:
            if temp_config_path.exists():
                temp_config_path.unlink()


if __name__ == "__main__":
    unittest.main()
