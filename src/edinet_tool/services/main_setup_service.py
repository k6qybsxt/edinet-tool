from edinet_tool.config.settings import BASE_DIR
from edinet_tool.services.parse_cache import XbrlParseCache


def get_main_zip_dir():
    return BASE_DIR / "data" / "input" / "zip"


def get_main_template_dir():
    return BASE_DIR / "templates"


def build_main_output_root(timestamp):
    return BASE_DIR / "data" / "output" / timestamp


def build_main_extracted_root(output_root):
    return output_root / "_zip_extracted"


def prepare_main_paths(timestamp):
    output_root = build_main_output_root(timestamp)
    extracted_root = build_main_extracted_root(output_root)
    template_dir = get_main_template_dir()

    output_root.mkdir(parents=True, exist_ok=True)
    extracted_root.mkdir(parents=True, exist_ok=True)

    return {
        "output_root": output_root,
        "extracted_root": extracted_root,
        "template_dir": template_dir,
    }


def create_main_parse_cache(logger):
    return XbrlParseCache(logger=logger, max_items=8)