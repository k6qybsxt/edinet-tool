import zipfile
from pathlib import Path


def collect_xbrl_from_zip(zip_dir: str, extract_dir: str):
    """
    zipフォルダから対象XBRLを抽出し、抽出後のファイルパス一覧を返す
    return:
        [
            "C:/.../data/input/_zip_extracted/xxx.xbrl",
            ...
        ]
    """

    zip_dir = Path(zip_dir)
    extract_dir = Path(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for zip_path in zip_dir.glob("*.zip"):
        with zipfile.ZipFile(zip_path, "r") as z:
            for name in z.namelist():
                lower = name.lower()

                if not lower.endswith(".xbrl"):
                    continue

                if "/auditdoc/" in lower or "\\auditdoc\\" in lower:
                    continue

                if "jpcrp030000-asr" not in lower and "jpcrp040300" not in lower:
                    continue

                out_name = Path(name).name
                out_path = extract_dir / out_name

                data = z.read(name)
                with open(out_path, "wb") as dst:
                    dst.write(data)
                    dst.flush()

                results.append(str(out_path))

    return sorted(results)