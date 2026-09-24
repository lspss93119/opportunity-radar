from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


def _with_query(url: str, params: Mapping[str, object] | None) -> str:
    if not params:
        return url
    parts = urlsplit(url)
    query = urlencode(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _request_json_sync(
    url: str,
    *,
    method: str,
    params: Mapping[str, object] | None,
    json_body: Mapping[str, object] | None,
    headers: Mapping[str, str] | None,
    timeout: float,
) -> Any:
    request_url = _with_query(url, params)
    body = None if json_body is None else json.dumps(json_body).encode("utf-8")
    request_headers = {
        "Accept": "application/json",
        "User-Agent": "opportunity-radar/readonly",
    }
    if headers is not None:
        request_headers.update(headers)
    if body is not None:
        request_headers["Content-Type"] = "application/json"
    request = Request(request_url, data=body, headers=request_headers, method=method)
    with urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError("public API returned a non-200 response")
        return json.loads(response.read().decode("utf-8"))


async def request_json(
    url: str,
    *,
    method: str = "GET",
    params: Mapping[str, object] | None = None,
    json_body: Mapping[str, object] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 10.0,
) -> Any:
    return await asyncio.to_thread(
        _request_json_sync,
        url,
        method=method,
        params=params,
        json_body=json_body,
        headers=headers,
        timeout=timeout,
    )
