from __future__ import annotations

from dataclasses import dataclass

import httpx
import pytest

from radar.alerts.telegram import TelegramTransport, TelegramTransportError


@dataclass
class FakeResponse:
    status_code: int = 200

    def raise_for_status(self) -> None:
        if self.status_code < 400:
            return
        request = httpx.Request("POST", "https://api.telegram.org/bot/redacted")
        response = httpx.Response(self.status_code, request=request)
        raise httpx.HTTPStatusError(
            "telegram status failure",
            request=request,
            response=response,
        )


class FakeClient:
    def __init__(
        self,
        response: FakeResponse | None = None,
        error: Exception | None = None,
    ) -> None:
        self.response = FakeResponse() if response is None else response
        self.error = error
        self.calls: list[dict[str, object]] = []

    async def post(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if self.error is not None:
            raise self.error
        return self.response


@pytest.mark.asyncio
async def test_send_text_and_chart_use_expected_telegram_payloads():
    client = FakeClient()
    transport = TelegramTransport("secret-token", "chat", client=client)  # type: ignore[arg-type]

    await transport.send_text("alert")
    await transport.send_chart(b"png", "caption")

    assert client.calls[0]["url"].endswith("/sendMessage")  # type: ignore[union-attr]
    assert client.calls[0]["data"] == {"chat_id": "chat", "text": "alert"}
    assert client.calls[1]["url"].endswith("/sendPhoto")  # type: ignore[union-attr]
    assert client.calls[1]["data"] == {"chat_id": "chat", "caption": "caption"}
    assert client.calls[1]["files"] == {
        "photo": ("spread.png", b"png", "image/png")
    }


@pytest.mark.asyncio
async def test_http_status_failure_is_observable_without_token_leak():
    client = FakeClient(FakeResponse(status_code=500))
    transport = TelegramTransport("secret-token", "chat", client=client)  # type: ignore[arg-type]

    with pytest.raises(TelegramTransportError) as caught:
        await transport.send_text("alert")

    assert "secret-token" not in str(caught.value)
    assert "500" in str(caught.value)


@pytest.mark.asyncio
async def test_request_failure_is_observable_without_token_leak():
    client = FakeClient(error=httpx.ConnectError("secret-token transport detail"))
    transport = TelegramTransport("secret-token", "chat", client=client)  # type: ignore[arg-type]

    with pytest.raises(TelegramTransportError) as caught:
        await transport.send_text("alert")

    assert "secret-token" not in str(caught.value)
    assert "failed" in str(caught.value).lower()


@pytest.mark.asyncio
async def test_transport_closes_an_internally_created_client(monkeypatch):
    class ManagedClient(FakeClient):
        closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_value, traceback):
            self.closed = True

    managed = ManagedClient()
    monkeypatch.setattr("radar.alerts.telegram.httpx.AsyncClient", lambda: managed)

    await TelegramTransport("secret-token", "chat").send_text("alert")

    assert managed.closed is True
