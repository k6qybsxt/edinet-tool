from edinet_tool.config.settings import BASE_DIR
from edinet_tool.services.parse_cache import XbrlParseCache


def get_main_zip_dir():
    return BASE_DIR / "data" / "input" / "zip"

def prepare_main_paths(runtime, timestamp):
    output_root = BASE_DIR / "data" / "output" / timestamp
    extracted_root = output_root / "_zip_extracted"
    template_dir = BASE_DIR / "templates"

    output_root.mkdir(parents=True, exist_ok=True)
    extracted_root.mkdir(parents=True, exist_ok=True)

    return {
        "output_root": output_root,
        "extracted_root": extracted_root,
        "template_dir": template_dir,
    }


def create_main_parse_cache(logger):
    return XbrlParseCache(logger=logger, max_items=8)