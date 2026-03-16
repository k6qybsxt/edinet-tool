import zipfile
from pathlib import Path


def collect_xbrl_from_zip(zip_dir: str):
    """
    zipフォルダからXBRLを抽出して一覧を返す
    return:
        [
            {
                "zip_path": "...",
                "xbrl_name": "...",
                "xbrl_bytes": bytes
            }
        ]
    """

    results = []

    zip_dir = Path(zip_dir)

    for zip_path in zip_dir.glob("*.zip"):

        with zipfile.ZipFile(zip_path, "r") as z:

            for name in z.namelist():

                if not name.lower().endswith(".xbrl"):
                    continue

                data = z.read(name)

                results.append(
                    {
                        "zip_path": str(zip_path),
                        "xbrl_name": name,
                        "xbrl_bytes": data,
                    }
                )

    return results