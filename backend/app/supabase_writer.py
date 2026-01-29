from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

import httpx


Json = Union[None, bool, int, float, str, List["Json"], Dict[str, "Json"]]


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    service_role_key: str
    write_enabled: bool


class SupabaseWriter:
    def __init__(self, config: SupabaseConfig, http_client: httpx.AsyncClient) -> None:
        self._config = config
        self._client = http_client
        self._base_url = config.url.rstrip("/")

    def enabled(self) -> bool:
        return bool(self._config.write_enabled and self._config.url and self._config.service_role_key)

    def _headers(self, prefer: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "apikey": self._config.service_role_key,
            "authorization": f"Bearer {self._config.service_role_key}",
            "content-type": "application/json",
            "accept": "application/json",
        }
        if prefer:
            headers["prefer"] = prefer
        return headers

    async def select(
        self,
        table: str,
        *,
        select: str,
        filters: Optional[Dict[str, str]] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if not self.enabled():
            return []

        params: Dict[str, str] = {"select": select}
        if filters:
            params.update(filters)
        if order:
            params["order"] = order
        if limit is not None:
            params["limit"] = str(limit)

        resp = await self._client.get(
            f"{self._base_url}/rest/v1/{table}",
            params=params,
            headers=self._headers(),
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, list) else []

    async def upsert(
        self,
        table: str,
        rows: Union[Dict[str, Any], Sequence[Dict[str, Any]]],
        *,
        on_conflict: str,
        select: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not self.enabled():
            return []

        params: Dict[str, str] = {"on_conflict": on_conflict}
        if select:
            params["select"] = select

        prefer = "resolution=merge-duplicates,return=representation"
        resp = await self._client.post(
            f"{self._base_url}/rest/v1/{table}",
            params=params,
            headers=self._headers(prefer),
            json=rows,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, list) else []

    async def insert(
        self,
        table: str,
        rows: Union[Dict[str, Any], Sequence[Dict[str, Any]]],
        *,
        select: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not self.enabled():
            return []

        params: Dict[str, str] = {}
        if select:
            params["select"] = select

        resp = await self._client.post(
            f"{self._base_url}/rest/v1/{table}",
            params=params,
            headers=self._headers("return=representation"),
            json=rows,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, list) else []
