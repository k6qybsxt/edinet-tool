import os
from dataclasses import dataclass, field
from typing import Any, Callable

@dataclass
class ParsedXbrlDocument:
    path: str
    cache_key: str
    facts: list = field(default_factory=list)
    contexts: dict = field(default_factory=dict)
    units: dict = field(default_factory=dict)
    nsmap: dict = field(default_factory=dict)
    dei_data: dict = field(default_factory=dict)
    meta: dict = field(default_factory=dict)
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
    def __init__(self, logger=None):
        self._docs: dict[str, ParsedXbrlDocument] = {}
        self.logger = logger

    def get(self, cache_key: str) -> ParsedXbrlDocument | None:
        return self._docs.get(cache_key)

    def put(self, doc: ParsedXbrlDocument) -> None:
        self._docs[doc.cache_key] = doc

    def clear(self) -> None:
        self._docs.clear()

    def size(self) -> int:
        return len(self._docs)

    def get_or_create(
        self,
        path: str,
        parser_func: Callable[[str], dict],
    ) -> ParsedXbrlDocument:
        cache_key = make_xbrl_cache_key(path)
        cached = self.get(cache_key)

        if cached is not None:
            if self.logger is not None:
                self.logger.debug(f"[xbrl cache hit] {os.path.basename(path)}")
            return cached

        if self.logger is not None:
            self.logger.debug(f"[xbrl cache miss] {os.path.basename(path)}")

        parsed = parser_func(path)

        doc = ParsedXbrlDocument(
            path=path,
            cache_key=cache_key,
            facts=parsed.get("facts", []),
            contexts=parsed.get("contexts", {}),
            units=parsed.get("units", {}),
            nsmap=parsed.get("nsmap", {}),
            dei_data=parsed.get("dei_data", {}),
            meta=parsed.get("meta", {}),
            out=parsed.get("out", {}),
            out_meta=parsed.get("out_meta", {}),
            security_code=parsed.get("security_code"),
            accounting_standard=parsed.get("meta", {}).get("accounting_standard", "jpgaap"),
            document_display_unit=parsed.get("meta", {}).get("document_display_unit"),
        )

        self.put(doc)
        
        return doc