from pathlib import Path

ROOT = Path(r"C:\Users\silve\OneDrive\PC\開発\test\Python")
EXCLUDE_DIRS = {".git", "venv", "__pycache__", ".mypy_cache", ".pytest_cache"}
EXCLUDE_FILES = set()

def is_excluded(path: Path) -> bool:
    return any(part in EXCLUDE_DIRS for part in path.parts) or path.name in EXCLUDE_FILES

def build_tree(path: Path, prefix=""):
    entries = [
        p for p in sorted(path.iterdir(), key=lambda x: (x.is_file(), x.name.lower()))
        if not is_excluded(p)
    ]

    lines = []
    for i, entry in enumerate(entries):
        is_last = i == len(entries) - 1
        connector = "└─ " if is_last else "├─ "
        lines.append(prefix + connector + entry.name + ("/" if entry.is_dir() else ""))

        if entry.is_dir():
            extension = "   " if is_last else "│  "
            lines.extend(build_tree(entry, prefix + extension))
    return lines

def main():
    lines = [ROOT.name + "/"]
    lines.extend(build_tree(ROOT))
    text = "\n".join(lines)

    output_file = ROOT / "folder_tree.txt"
    output_file.write_text(text, encoding="utf-8")
    print(text)
    print(f"\n保存先: {output_file}")

if __name__ == "__main__":
    main()