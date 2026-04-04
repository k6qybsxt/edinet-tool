from pathlib import Path

from edinet_pipeline.config.settings import (
    OUTPUT_ROOT,
    TEMPLATE_DIR,
    TEMPLATE_WORKBOOK_NAME,
    ZIP_INPUT_DIR,
)
from edinet_pipeline.services.parse_cache import XbrlParseCache
from edinet_pipeline.services.template_contract_service import ensure_template_contract


def get_main_zip_dir():
    return ZIP_INPUT_DIR


def get_main_template_dir():
    return TEMPLATE_DIR


def get_main_template_path(template_dir: str | Path | None = None) -> Path:
    base_dir = Path(template_dir) if template_dir is not None else get_main_template_dir()
    return base_dir / TEMPLATE_WORKBOOK_NAME


def build_main_output_root(timestamp):
    return OUTPUT_ROOT / timestamp


def build_main_extracted_root(output_root):
    return output_root / "_zip_extracted"


def prepare_main_paths(timestamp):
    output_root = build_main_output_root(timestamp)
    extracted_root = build_main_extracted_root(output_root)
    template_dir = get_main_template_dir()
    template_path = get_main_template_path(template_dir)

    output_root.mkdir(parents=True, exist_ok=True)
    extracted_root.mkdir(parents=True, exist_ok=True)
    template_dir.mkdir(parents=True, exist_ok=True)

    return {
        "output_root": output_root,
        "extracted_root": extracted_root,
        "template_dir": template_dir,
        "template_path": template_path,
    }


def create_main_parse_cache(logger, runtime):
    return XbrlParseCache(
        logger=logger,
        max_items=runtime.parse_cache_max_items,
    )


def validate_main_template_contract(template_dir, logger, *, include_stock_ranges: bool):
    template_path = get_main_template_path(template_dir)
    report = ensure_template_contract(
        template_path,
        include_stock_ranges=include_stock_ranges,
    )
    logger.info(
        "[template contract] ok template=%s sheets=%d defined_names=%d required_named_ranges=%d",
        template_path,
        report["sheet_count"],
        report["defined_name_count"],
        report["required_named_range_count"],
    )
    return report
