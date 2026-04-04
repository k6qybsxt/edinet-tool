from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Mapping


BASE_DIR = Path(__file__).resolve().parents[3]
CONFIG_DIR = BASE_DIR / "config"
DEFAULT_CONFIG_PATH = CONFIG_DIR / "edinet_pipeline.default.json"
LOCAL_CONFIG_PATH = CONFIG_DIR / "edinet_pipeline.local.json"
CONFIG_PATH_ENV_NAME = "EDINET_PIPELINE_CONFIG"

_ENV_OVERRIDE_MAP = {
    "EDINET_PIPELINE_LOG_MODE": "log_mode",
    "EDINET_PIPELINE_LOG_DIR": "log_dir",
    "EDINET_PIPELINE_DATA_ROOT": "data_root",
    "EDINET_PIPELINE_INPUT_ROOT": "input_root",
    "EDINET_PIPELINE_OUTPUT_ROOT": "output_root",
    "EDINET_PIPELINE_CACHE_ROOT": "cache_root",
    "EDINET_PIPELINE_ZIP_INPUT_DIR": "zip_input_dir",
    "EDINET_PIPELINE_COMPANY_SETS_DIR": "company_sets_dir",
    "EDINET_PIPELINE_MANUAL_XBRL_DIR": "manual_xbrl_dir",
    "EDINET_PIPELINE_TEMPLATE_DIR": "template_dir",
    "EDINET_PIPELINE_TEMPLATE_WORKBOOK_NAME": "template_workbook_name",
}


def load_config(file_path: str | Path) -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    if not isinstance(loaded, dict):
        raise ValueError(f"config file must contain a JSON object: {file_path}")

    return loaded


def _merge_config(base: dict, override: Mapping[str, object]) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if value is not None:
            merged[key] = value
    return merged


def _resolve_path(value: str | Path | None) -> Path | None:
    if value in (None, ""):
        return None

    path = Path(str(value))
    if path.is_absolute():
        return path
    return BASE_DIR / path


def _normalize_log_mode(value: object) -> str:
    log_mode = str(value or "DEBUG").strip().upper()
    if log_mode not in {"NORMAL", "DEBUG"}:
        raise ValueError(f"unsupported log_mode: {log_mode}")
    return log_mode


def _normalize_int(value: object, *, setting_name: str, minimum: int) -> int:
    normalized = int(value)
    if normalized < minimum:
        raise ValueError(f"{setting_name} must be >= {minimum}: {normalized}")
    return normalized


def _load_optional_config(config_path: str | Path | None) -> tuple[dict, Path | None]:
    if config_path in (None, ""):
        return {}, None

    resolved_path = Path(config_path)
    if not resolved_path.is_absolute():
        resolved_path = BASE_DIR / resolved_path

    if not resolved_path.exists():
        raise FileNotFoundError(f"config file was not found: {resolved_path}")

    return load_config(resolved_path), resolved_path


def _apply_env_overrides(config: dict, env: Mapping[str, str]) -> dict:
    overrides = {}
    for env_name, config_key in _ENV_OVERRIDE_MAP.items():
        value = env.get(env_name)
        if value not in (None, ""):
            overrides[config_key] = value
    return _merge_config(config, overrides)


def load_pipeline_settings(
    *,
    env: Mapping[str, str] | None = None,
    config_path: str | Path | None = None,
    include_local_config: bool = True,
) -> dict:
    effective_env = os.environ if env is None else env

    merged_config = load_config(DEFAULT_CONFIG_PATH)
    loaded_paths: list[Path] = [DEFAULT_CONFIG_PATH]

    if include_local_config and LOCAL_CONFIG_PATH.exists():
        local_config, local_path = _load_optional_config(LOCAL_CONFIG_PATH)
        merged_config = _merge_config(merged_config, local_config)
        if local_path is not None:
            loaded_paths.append(local_path)

    external_path = config_path or effective_env.get(CONFIG_PATH_ENV_NAME)
    if external_path not in (None, ""):
        external_config, resolved_external_path = _load_optional_config(external_path)
        merged_config = _merge_config(merged_config, external_config)
        if resolved_external_path is not None:
            loaded_paths.append(resolved_external_path)

    merged_config = _apply_env_overrides(merged_config, effective_env)

    log_mode = _normalize_log_mode(merged_config.get("log_mode"))
    log_dir = _resolve_path(merged_config.get("log_dir")) or (BASE_DIR / "logs")
    data_root = _resolve_path(merged_config.get("data_root")) or Path(r"D:\EDINET_Data")
    input_root = _resolve_path(merged_config.get("input_root")) or (data_root / "input")
    output_root = _resolve_path(merged_config.get("output_root")) or (data_root / "output")
    cache_root = _resolve_path(merged_config.get("cache_root")) or (data_root / "cache")
    zip_input_dir = _resolve_path(merged_config.get("zip_input_dir")) or (input_root / "zip")
    company_sets_dir = _resolve_path(merged_config.get("company_sets_dir")) or (input_root / "company_sets")
    manual_xbrl_dir = _resolve_path(merged_config.get("manual_xbrl_dir")) or (input_root / "xbrl_manual")
    template_dir = _resolve_path(merged_config.get("template_dir")) or (BASE_DIR / "templates")

    template_workbook_name = str(
        merged_config.get("template_workbook_name") or "決算分析シート_1.xlsm"
    ).strip()
    if not template_workbook_name:
        raise ValueError("template_workbook_name must not be empty")

    log_max_bytes = _normalize_int(
        merged_config.get("log_max_bytes", 2_000_000),
        setting_name="log_max_bytes",
        minimum=1,
    )
    log_backup_count = _normalize_int(
        merged_config.get("log_backup_count", 5),
        setting_name="log_backup_count",
        minimum=0,
    )

    if log_mode == "DEBUG":
        worker_log_level = "DEBUG"
        worker_enable_stream_handler = True
        worker_enable_file_handler = True
        worker_emit_initialized_log = True
    else:
        worker_log_level = "INFO"
        worker_enable_stream_handler = False
        worker_enable_file_handler = False
        worker_emit_initialized_log = False

    return {
        "log_mode": log_mode,
        "log_level": "DEBUG" if log_mode == "DEBUG" else "INFO",
        "worker_log_level": worker_log_level,
        "worker_enable_stream_handler": worker_enable_stream_handler,
        "worker_enable_file_handler": worker_enable_file_handler,
        "worker_emit_initialized_log": worker_emit_initialized_log,
        "log_dir": log_dir,
        "data_root": data_root,
        "input_root": input_root,
        "output_root": output_root,
        "cache_root": cache_root,
        "zip_input_dir": zip_input_dir,
        "company_sets_dir": company_sets_dir,
        "manual_xbrl_dir": manual_xbrl_dir,
        "template_dir": template_dir,
        "template_workbook_name": template_workbook_name,
        "log_max_bytes": log_max_bytes,
        "log_backup_count": log_backup_count,
        "loaded_config_paths": tuple(str(path) for path in loaded_paths),
        "active_config_path": str(loaded_paths[-1]),
    }


PIPELINE_SETTINGS = load_pipeline_settings()

LOG_MODE = PIPELINE_SETTINGS["log_mode"]
LOG_LEVEL = PIPELINE_SETTINGS["log_level"]
WORKER_LOG_LEVEL = PIPELINE_SETTINGS["worker_log_level"]
WORKER_ENABLE_STREAM_HANDLER = PIPELINE_SETTINGS["worker_enable_stream_handler"]
WORKER_ENABLE_FILE_HANDLER = PIPELINE_SETTINGS["worker_enable_file_handler"]
WORKER_EMIT_INITIALIZED_LOG = PIPELINE_SETTINGS["worker_emit_initialized_log"]

LOG_DIR = PIPELINE_SETTINGS["log_dir"]

DATA_ROOT = PIPELINE_SETTINGS["data_root"]
INPUT_ROOT = PIPELINE_SETTINGS["input_root"]
OUTPUT_ROOT = PIPELINE_SETTINGS["output_root"]
CACHE_ROOT = PIPELINE_SETTINGS["cache_root"]

ZIP_INPUT_DIR = PIPELINE_SETTINGS["zip_input_dir"]
COMPANY_SETS_DIR = PIPELINE_SETTINGS["company_sets_dir"]
MANUAL_XBRL_DIR = PIPELINE_SETTINGS["manual_xbrl_dir"]

TEMPLATE_DIR = PIPELINE_SETTINGS["template_dir"]
TEMPLATE_WORKBOOK_NAME = PIPELINE_SETTINGS["template_workbook_name"]

LOG_MAX_BYTES = PIPELINE_SETTINGS["log_max_bytes"]
LOG_BACKUP_COUNT = PIPELINE_SETTINGS["log_backup_count"]
LOADED_CONFIG_PATHS = PIPELINE_SETTINGS["loaded_config_paths"]
ACTIVE_CONFIG_PATH = PIPELINE_SETTINGS["active_config_path"]
