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
    local_cache: dict[str, Any] = field(default_factory=dict)


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
        )
        doc.local_cache["__legacy_result__"] = parsed.get("__legacy_result__")

        self.put(doc)
        
        return doc