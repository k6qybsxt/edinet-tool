from io import BytesIO
import zipfile
from lxml import etree


def parse_xbrl_bytes(xbrl_bytes: bytes):
    tree = etree.parse(BytesIO(xbrl_bytes))
    root = tree.getroot()
    return root


def read_xbrl_from_zip(zip_path: str, member_name: str):
    with zipfile.ZipFile(zip_path, "r") as z:
        return z.read(member_name)