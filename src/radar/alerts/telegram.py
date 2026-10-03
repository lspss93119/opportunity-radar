from __future__ import annotations

from collections.abc import Mapping

import httpx

TELEGRAM_HTTP_TIMEOUT = httpx.Timeout(
    connect=10.0,
    read=30.0,
    write=30.0,
    pool=10.0,
)


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
    ) -> None:
        if not bot_token:
            raise ValueError("bot_token must be non-empty")
        self._bot_token = bot_token
        self._chat_id = str(chat_id)
        self._owns_client = client is None
        self._client = (
            httpx.AsyncClient(timeout=TELEGRAM_HTTP_TIMEOUT)
            if client is None
            else client
        )
        self._closed = False

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
        try:
            response = await self._client.post(url, data=data, files=files)
            response.raise_for_status()
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
