from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

import requests

from edinet_monitor.config.settings import (
    JQUANTS_API_BASE_URL,
    JQUANTS_API_KEY_ENV,
    JQUANTS_CONNECT_TIMEOUT_SEC,
    JQUANTS_READ_TIMEOUT_SEC,
)


class JQuantsAuthError(RuntimeError):
    pass


@dataclass(frozen=True)
class JQuantsPage:
    items: list[dict[str, Any]]
    pagination_key: str | None = None


class JQuantsClient:
    """Small J-Quants API V2 client.

    The API key is read from the environment and used only as a request header.
    Do not log API key values from this class.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = JQUANTS_API_BASE_URL,
        session: requests.Session | None = None,
        timeout: tuple[int, int] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key if api_key is not None else os.getenv(JQUANTS_API_KEY_ENV, "")
        self.session = session or requests.Session()
        self.timeout = timeout or (JQUANTS_CONNECT_TIMEOUT_SEC, JQUANTS_READ_TIMEOUT_SEC)

    def _ensure_api_key(self) -> str:
        api_key = str(self.api_key or "").strip()
        if not api_key:
            raise JQuantsAuthError(f"{JQUANTS_API_KEY_ENV} is not set.")
        return api_key

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        api_key = self._ensure_api_key()
        response = self.session.get(
            f"{self.base_url}{path}",
            params={key: value for key, value in params.items() if value not in (None, "")},
            headers={"x-api-key": api_key},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def _page_from_payload(self, payload: dict[str, Any], keys: tuple[str, ...]) -> JQuantsPage:
        items: list[dict[str, Any]] = []
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                items = list(value)
                break
        pagination_key = payload.get("pagination_key") or payload.get("paginationKey")
        return JQuantsPage(items=items, pagination_key=pagination_key)

    @staticmethod
    def _api_date(value: str | None) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        return text.replace("-", "").replace("/", "")

    @staticmethod
    def _api_code(value: str | None) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        if len(text) == 4 and text.isdigit():
            return f"{text}0"
        return text

    def get_fin_summary_page(
        self,
        *,
        date: str | None = None,
        code: str | None = None,
        pagination_key: str | None = None,
    ) -> JQuantsPage:
        payload = self._get(
            "/fins/summary",
            {
                "date": self._api_date(date),
                "code": self._api_code(code),
                "pagination_key": pagination_key,
            },
        )
        return self._page_from_payload(payload, ("fin_summary", "summary", "data"))

    def iter_fin_summary(
        self,
        *,
        date: str | None = None,
        code: str | None = None,
    ):
        pagination_key: str | None = None
        while True:
            page = self.get_fin_summary_page(date=date, code=code, pagination_key=pagination_key)
            yield from page.items
            if not page.pagination_key:
                break
            pagination_key = page.pagination_key

    def get_statements_page(self, *, date: str | None = None, code: str | None = None, pagination_key: str | None = None) -> JQuantsPage:
        return self.get_fin_summary_page(date=date, code=code, pagination_key=pagination_key)

    def iter_statements(self, *, date: str | None = None, code: str | None = None):
        yield from self.iter_fin_summary(date=date, code=code)

    def get_eq_bars_daily_page(
        self,
        *,
        date: str | None = None,
        code: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        pagination_key: str | None = None,
    ) -> JQuantsPage:
        payload = self._get(
            "/equities/bars/daily",
            {
                "date": self._api_date(date),
                "code": self._api_code(code),
                "from": self._api_date(date_from),
                "to": self._api_date(date_to),
                "pagination_key": pagination_key,
            },
        )
        return self._page_from_payload(payload, ("eq_bars_daily", "bars", "data"))

    def iter_eq_bars_daily(
        self,
        *,
        date: str | None = None,
        code: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        pagination_key: str | None = None
        while True:
            page = self.get_eq_bars_daily_page(
                date=date,
                code=code,
                date_from=date_from,
                date_to=date_to,
                pagination_key=pagination_key,
            )
            yield from page.items
            if not page.pagination_key:
                break
            pagination_key = page.pagination_key

    def get_daily_quotes_page(
        self,
        *,
        date: str | None = None,
        code: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        pagination_key: str | None = None,
    ) -> JQuantsPage:
        return self.get_eq_bars_daily_page(
            date=date,
            code=code,
            date_from=date_from,
            date_to=date_to,
            pagination_key=pagination_key,
        )

    def iter_daily_quotes(
        self,
        *,
        date: str | None = None,
        code: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        yield from self.iter_eq_bars_daily(date=date, code=code, date_from=date_from, date_to=date_to)
