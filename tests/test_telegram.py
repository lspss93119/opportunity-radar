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
        self.close_calls = 0

    async def post(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if self.error is not None:
            raise self.error
        return self.response

    async def aclose(self) -> None:
        self.close_calls += 1


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

    assert caught.value.error_kind == "http_status"
    assert caught.value.status_code == 500
    assert "secret-token" not in str(caught.value)
    assert "500" in str(caught.value)


@pytest.mark.asyncio
async def test_request_failure_is_observable_without_token_leak():
    client = FakeClient(error=httpx.ConnectError("secret-token transport detail"))
    transport = TelegramTransport("secret-token", "chat", client=client)  # type: ignore[arg-type]

    with pytest.raises(TelegramTransportError) as caught:
        await transport.send_text("alert")

    assert caught.value.error_kind == "connect_error"
    assert caught.value.status_code is None
    assert "secret-token" not in str(caught.value)
    assert "api.telegram.org" not in str(caught.value)
    assert "failed" in str(caught.value).lower()


@pytest.mark.asyncio
async def test_owned_client_is_reused_and_closed_once(monkeypatch):
    created: list[tuple[FakeClient, httpx.Timeout]] = []

    def factory(*, timeout: httpx.Timeout) -> FakeClient:
        client = FakeClient()
        created.append((client, timeout))
        return client

    monkeypatch.setattr("radar.alerts.telegram.httpx.AsyncClient", factory)
    transport = TelegramTransport("secret-token", "chat")

    await transport.send_text("alert")
    await transport.send_chart(b"png", "caption")
    await transport.aclose()
    await transport.aclose()

    assert len(created) == 1
    assert created[0][1].connect is not None
    assert len(created[0][0].calls) == 2
    assert created[0][0].close_calls == 1


@pytest.mark.asyncio
async def test_injected_client_is_reused_but_not_closed():
    client = FakeClient()
    transport = TelegramTransport("secret-token", "chat", client=client)  # type: ignore[arg-type]

    await transport.send_text("alert")
    await transport.send_chart(b"png", "caption")
    await transport.aclose()

    assert len(client.calls) == 2
    assert client.close_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "error_kind"),
    [
        (httpx.ConnectTimeout("secret body"), "connect_timeout"),
        (httpx.ReadTimeout("secret body"), "read_timeout"),
        (httpx.WriteTimeout("secret body"), "write_timeout"),
        (httpx.PoolTimeout("secret body"), "pool_timeout"),
        (httpx.ConnectError("secret body"), "connect_error"),
        (httpx.ReadError("secret body"), "read_error"),
        (httpx.WriteError("secret body"), "write_error"),
        (httpx.RemoteProtocolError("secret body"), "remote_protocol_error"),
    ],
)
async def test_httpx_failures_have_sanitized_error_kinds(error, error_kind):
    client = FakeClient(error=error)
    transport = TelegramTransport("secret-token", "chat", client=client)  # type: ignore[arg-type]

    with pytest.raises(TelegramTransportError) as caught:
        await transport.send_text("secret message")

    assert caught.value.error_kind == error_kind
    assert caught.value.status_code is None
    assert "secret" not in str(caught.value)
    assert "api.telegram.org" not in str(caught.value)
