from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

TELEGRAM_HTTP_TIMEOUT = httpx.Timeout(
    connect=10.0,
    read=30.0,
    write=30.0,
    pool=10.0,
)


# httpx logs every request URL at INFO, and Telegram puts the bot token in the
# URL path. Request logging is raised to WARNING wherever a transport exists.
_REQUEST_LOGGERS = ("httpx", "httpcore")
MAX_RETRIES = 3
MIN_SEND_INTERVAL_SECONDS = 3.0


def _silence_request_logging() -> None:
    for name in _REQUEST_LOGGERS:
        logger = logging.getLogger(name)
        if logger.getEffectiveLevel() < logging.WARNING:
            logger.setLevel(logging.WARNING)


class TelegramTransportError(RuntimeError):
    def __init__(self, *, error_kind: str, status_code: int | None) -> None:
        self.error_kind = error_kind
        self.status_code = status_code
        status = f" status_code={status_code}" if status_code is not None else ""
        super().__init__(f"Telegram request failed error_kind={error_kind}{status}")


class TelegramTransport:
    def __init__(
        self,
        bot_token: str,
        chat_id: str | int,
        *,
        client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not bot_token:
            raise ValueError("bot_token must be non-empty")
        _silence_request_logging()
        self._bot_token = bot_token
        self._chat_id = str(chat_id)
        self._owns_client = client is None
        self._client = (
            httpx.AsyncClient(timeout=TELEGRAM_HTTP_TIMEOUT)
            if client is None
            else client
        )
        self._closed = False
        self._sleep = sleep
        self._clock = clock
        self._last_send_at: float | None = None

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            await self._client.aclose()

    async def send_text(self, text: str) -> None:
        await self._post("sendMessage", {"chat_id": self._chat_id, "text": text})

    async def send_chart(self, png: bytes, caption: str) -> None:
        await self._post(
            "sendPhoto",
            {"chat_id": self._chat_id, "caption": caption},
            files={"photo": ("spread.png", png, "image/png")},
        )

    async def _post(
        self,
        method: str,
        data: Mapping[str, str],
        *,
        files: Mapping[str, tuple[str, bytes, str]] | None = None,
    ) -> None:
        url = f"https://api.telegram.org/bot{self._bot_token}/{method}"
        wait_for_send_slot = True
        for retry_number in range(MAX_RETRIES + 1):
            if wait_for_send_slot:
                await self._wait_for_send_slot()
            wait_for_send_slot = True
            try:
                response = await self._client.post(url, data=data, files=files)
                if response.status_code == 429:
                    if retry_number >= MAX_RETRIES:
                        raise TelegramTransportError(
                            error_kind="http_status", status_code=429
                        )
                    retry_after = _retry_after_seconds(response)
                    delay = (
                        retry_after
                        if retry_after is not None
                        else float(2**retry_number)
                    )
                    await self._sleep(min(delay, 60.0))
                    # The retry delay itself separates this attempt from the
                    # next one; keep the injected clock and rate limiter in
                    # sync without adding a second wait.
                    self._last_send_at = self._clock()
                    wait_for_send_slot = False
                    continue
                response.raise_for_status()
                return
            except TelegramTransportError:
                raise
            except httpx.HTTPStatusError as error:
                raise TelegramTransportError(
                    error_kind="http_status",
                    status_code=error.response.status_code,
                ) from None
            except httpx.ConnectTimeout:
                raise TelegramTransportError(
                    error_kind="connect_timeout", status_code=None
                ) from None
            except httpx.ReadTimeout:
                raise TelegramTransportError(
                    error_kind="read_timeout", status_code=None
                ) from None
            except httpx.WriteTimeout:
                raise TelegramTransportError(
                    error_kind="write_timeout", status_code=None
                ) from None
            except httpx.PoolTimeout:
                raise TelegramTransportError(
                    error_kind="pool_timeout", status_code=None
                ) from None
            except httpx.ConnectError:
                raise TelegramTransportError(
                    error_kind="connect_error", status_code=None
                ) from None
            except httpx.RemoteProtocolError:
                raise TelegramTransportError(
                    error_kind="remote_protocol_error", status_code=None
                ) from None
            except httpx.ReadError:
                raise TelegramTransportError(
                    error_kind="read_error", status_code=None
                ) from None
            except httpx.WriteError:
                raise TelegramTransportError(
                    error_kind="write_error", status_code=None
                ) from None
            except httpx.HTTPError:
                raise TelegramTransportError(
                    error_kind="other_httpx_error", status_code=None
                ) from None

    async def _wait_for_send_slot(self) -> None:
        now = self._clock()
        if self._last_send_at is not None:
            delay = MIN_SEND_INTERVAL_SECONDS - (now - self._last_send_at)
            if delay > 0:
                await self._sleep(delay)
                now = self._clock()
        self._last_send_at = now


def _retry_after_seconds(response: Any) -> float | None:
    try:
        payload = response.json()
    except (AttributeError, ValueError, TypeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    parameters = payload.get("parameters")
    if not isinstance(parameters, Mapping):
        return None
    retry_after = parameters.get("retry_after")
    if isinstance(retry_after, bool) or not isinstance(retry_after, (int, float)):
        return None
    return retry_after if retry_after >= 0 else None
