from lxml import etree
from io import BytesIO
import zipfile

def parse_xbrl_bytes(xbrl_bytes: bytes):

    tree = etree.parse(BytesIO(xbrl_bytes))
    root = tree.getroot()

    return root

def read_xbrl_from_zip(zip_path: str):
    """
    ZIPからXBRLをメモリ上で取得（disk展開しない）
    """
    with zipfile.ZipFile(zip_path, 'r') as z:
        for name in z.namelist():
            if name.lower().endswith(".xbrl"):
                return z.read(name)

    raise ValueError("XBRLファイルがZIP内に見つかりません")