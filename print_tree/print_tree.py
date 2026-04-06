from __future__ import annotations

import sys
from pathlib import Path


DEFAULT_ROOT = Path(r"C:\Users\silve\EDINET_Pipeline")
EXCLUDE_DIRS = {".git", ".venv", "__pycache__", ".mypy_cache", ".pytest_cache", ".vscode"}
EXCLUDE_FILES = set()
PRUNE_PATHS = {
    Path(r"D:\EDINET_Data\edinet_monitor\raw\zip").resolve(),
    Path(r"D:\EDINET_Data\edinet_monitor\raw\xbrl").resolve(),
    Path(r"D:\EDINET_Data\edinet_monitor\raw\manifests").resolve(),
}


def is_excluded(path: Path) -> bool:
    return any(part in EXCLUDE_DIRS for part in path.parts) or path.name in EXCLUDE_FILES


def is_pruned(path: Path) -> bool:
    return path.resolve() in PRUNE_PATHS


def build_tree(path: Path, prefix: str = "") -> list[str]:
    if is_pruned(path):
        return [prefix + "[contents omitted]"]

    entries = [
        p for p in sorted(path.iterdir(), key=lambda x: (x.is_file(), x.name.lower()))
        if not is_excluded(p)
    ]

    lines: list[str] = []
    for i, entry in enumerate(entries):
        is_last = i == len(entries) - 1
        connector = "└─ " if is_last else "├─ "
        lines.append(prefix + connector + entry.name + ("/" if entry.is_dir() else ""))

        if entry.is_dir():
            extension = "   " if is_last else "│  "
            lines.extend(build_tree(entry, prefix + extension))
    return lines


def resolve_target_root() -> Path:
    if len(sys.argv) >= 2:
        return Path(sys.argv[1]).resolve()
    return DEFAULT_ROOT.resolve()


def resolve_output_file(target_root: Path) -> Path:
    if len(sys.argv) >= 3:
        return Path(sys.argv[2]).resolve()
    return target_root / "folder_tree.txt"


def main() -> None:
    target_root = resolve_target_root()
    output_file = resolve_output_file(target_root)

    if not target_root.exists():
        raise FileNotFoundError(f"対象フォルダが存在しません: {target_root}")

    lines = [target_root.name + "/"]
    lines.extend(build_tree(target_root))
    text = "\n".join(lines)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(text, encoding="utf-8")

    print(text)
    print(f"\n保存先: {output_file}")


if __name__ == "__main__":
    main()
