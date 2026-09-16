from __future__ import annotations

from collections.abc import Mapping

import httpx


class TelegramTransportError(RuntimeError):
    pass


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
        self._client = client

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
            if self._client is not None:
                response = await self._client.post(url, data=data, files=files)
                response.raise_for_status()
            else:
                async with httpx.AsyncClient() as client:
                    response = await client.post(url, data=data, files=files)
                    response.raise_for_status()
        except httpx.HTTPError:
            status_code = getattr(locals().get("response"), "status_code", None)
            status = f" HTTP {status_code}" if status_code is not None else ""
            raise TelegramTransportError(
                f"Telegram request failed{status}"
            ) from None
