from lxml import etree
from io import BytesIO


def parse_xbrl_bytes(xbrl_bytes: bytes):

    tree = etree.parse(BytesIO(xbrl_bytes))
    root = tree.getroot()

    return root