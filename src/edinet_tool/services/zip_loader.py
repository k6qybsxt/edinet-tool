from pathlib import Path
import zipfile


def list_xbrl_members_in_zip(zip_dir: str):
    """
    zipフォルダ内の対象XBRLを展開せずに列挙する
    return:
        [
            {
                "zip_path": ".../abc.zip",
                "member_name": "XBRL/PublicDoc/xxx.xbrl",
                "xbrl_name": "xxx.xbrl",
            },
            ...
        ]
    """
    zip_dir = Path(zip_dir)
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

                results.append(
                    {
                        "zip_path": str(zip_path),
                        "member_name": name,
                        "xbrl_name": Path(name).name,
                    }
                )

    return sorted(results, key=lambda x: (x["zip_path"], x["xbrl_name"]))


def extract_selected_xbrl(zip_path: str, member_name: str, out_path: str):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as z:
        data = z.read(member_name)

    with open(out_path, "wb") as dst:
        dst.write(data)
        dst.flush()

    return str(out_path)