import os
from dataclasses import dataclass, field
from typing import Any, Callable
from collections import OrderedDict

@dataclass
class ParsedXbrlDocument:
    path: str
    cache_key: str
    contexts: dict = field(default_factory=dict)
    units: dict = field(default_factory=dict)
    nsmap: dict = field(default_factory=dict)
    dei_data: dict = field(default_factory=dict)
    meta: dict = field(default_factory=dict)
    facts: list = field(default_factory=list)
    out: dict = field(default_factory=dict)
    out_meta: dict = field(default_factory=dict)
    security_code: Any = None
    accounting_standard: str = "jpgaap"
    document_display_unit: Any = None


def make_xbrl_cache_key(path: str) -> str:
    abs_path = os.path.abspath(path)
    st = os.stat(abs_path)
    return f"{abs_path}|{st.st_size}|{st.st_mtime_ns}"


class XbrlParseCache:

    def stats(self) -> dict:
        return {
            "size": len(self._docs),
            "max_items": self.max_items,
            "keys": list(self._docs.keys())[-5:],
        }

    def __init__(self, logger=None, max_items=8):
        self._docs = OrderedDict()
        self.logger = logger
        self.max_items = max_items

    def get(self, cache_key: str) -> ParsedXbrlDocument | None:
        doc = self._docs.get(cache_key)
        if doc is not None:
            self._docs.move_to_end(cache_key)
        return doc

    def put(self, doc: ParsedXbrlDocument) -> None:

        if doc.cache_key in self._docs:
            return

        self._docs[doc.cache_key] = doc
        self._docs.move_to_end(doc.cache_key)

        if len(self._docs) > self.max_items:
            self._docs.popitem(last=False)

    def clear(self) -> None:
        self._docs.clear()

    def size(self) -> int:
        return len(self._docs)

    def get_or_create(
        self,
        path: str,
        parser_func,
    ) -> ParsedXbrlDocument:

        cache_key = make_xbrl_cache_key(path)

        cached = self.get(cache_key)
        if cached is not None:
            if self.logger:
                self.logger.debug(f"[xbrl cache hit] {os.path.basename(path)}")
            return cached

        if self.logger:
            self.logger.debug(f"[xbrl cache miss] {os.path.basename(path)}")

        parsed = parser_func(path)

        # ★ここ追加：不要データ削減（メモリ削減＆高速化）
        doc = ParsedXbrlDocument(
            path=path,
            cache_key=cache_key,
            contexts=parsed.get("contexts", {}),
            units=parsed.get("units", {}),
            nsmap=parsed.get("nsmap", {}),
            dei_data=parsed.get("dei_data", {}),
            meta=parsed.get("meta", {}),
            facts=parsed.get("facts", []),
            out=parsed.get("out", {}),
            out_meta=parsed.get("out_meta", {}),
            security_code=parsed.get("security_code"),
            accounting_standard=parsed.get("meta", {}).get("accounting_standard", "jpgaap"),
            document_display_unit=parsed.get("meta", {}).get("document_display_unit"),
        )

        self.put(doc)

        return doc