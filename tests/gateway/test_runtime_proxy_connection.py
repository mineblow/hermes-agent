from __future__ import annotations

import pytest

from gateway.runtime_proxy_connection import RuntimeProxyAsyncConnection
from hermes_cli.live_runtime_owners import RuntimeOwner
from hermes_cli.live_runtime_protocol import frontend_hello, runtime_input


class FakeProxy:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.requests = []
        self.connected = False
        self.closed = False
        FakeProxy.instances.append(self)

    def connect(self):
        self.connected = True

    def close(self):
        self.closed = True

    def request(self, frame):
        self.requests.append(frame)
        if frame["method"] == "session.resume":
            return {
                "jsonrpc": "2.0",
                "id": frame["id"],
                "result": {"session_id": "live-session-1"},
            }
        return {
            "jsonrpc": "2.0",
            "id": frame["id"],
            "result": {"status": "streaming"},
        }


@pytest.fixture(autouse=True)
def clear_instances():
    FakeProxy.instances.clear()


@pytest.fixture
def owner(tmp_path):
    return RuntimeOwner(
        conversation_key="v1:conversation",
        owner_id="owner-1",
        generation=3,
        pid=123,
        process_start_time=1.0,
        endpoint=str(tmp_path / "runtime.sock"),
        profile_home="/profiles/worker",
        surface="tui_gateway",
        started_at=1.0,
    )


def _connection(owner):
    return RuntimeProxyAsyncConnection(
        owner=owner,
        durable_session_id="stored-session-1",
        profile="worker",
        client_id="gateway-v1:client",
        requested_capabilities=frozenset(
            {"observe", "prompt.submit", "interaction.respond"}
        ),
        proxy_factory=FakeProxy,
    )


@pytest.mark.asyncio
async def test_neutral_hello_connects_authenticated_proxy_and_resumes_session(owner):
    connection = _connection(owner)

    await connection.send(
        frontend_hello(
            client_id="gateway-v1:client",
            principal={
                "provider": "discord",
                "subject": "user-1",
                "authenticated": True,
            },
            surface="discord",
            requested_capabilities={
                "observe",
                "prompt.submit",
                "interaction.respond",
            },
            durable_root="root-1",
        )
    )
    acknowledgement = await connection.recv()

    proxy = FakeProxy.instances[0]
    assert proxy.connected is True
    assert proxy.kwargs["negotiated_capabilities"] == frozenset(
        {"observe", "prompt.submit", "ui.respond"}
    )
    assert proxy.requests[0]["method"] == "session.resume"
    assert proxy.requests[0]["params"] == {
        "session_id": "stored-session-1",
        "profile": "worker",
        "attachment_mode": "control",
        "omit_messages": True,
    }
    assert acknowledgement["kind"] == "frontend.hello.ok"
    assert acknowledgement["runtime_id"] == "live-session-1"
    assert acknowledgement["durable_session_id"] == "stored-session-1"
    assert acknowledgement["generation"] == 3


@pytest.mark.asyncio
async def test_runtime_input_maps_to_prompt_submit_without_losing_identity(owner):
    connection = _connection(owner)
    await connection.send(
        frontend_hello(
            client_id="gateway-v1:client",
            principal={
                "provider": "discord",
                "subject": "user-1",
                "authenticated": True,
            },
            surface="discord",
            requested_capabilities={
                "observe",
                "prompt.submit",
                "interaction.respond",
            },
            durable_root="root-1",
        )
    )
    await connection.recv()

    await connection.send(
        {
            "kind": "control.request",
            "protocol": 1,
            "request_id": "request-1",
            "payload": runtime_input(
                message_id="message-1",
                text="execution text",
                display_text="display text",
                submitted_at=1_000.0,
                attachment_refs=["/tmp/image-one.png", "/tmp/document.pdf"],
                image_refs=["/tmp/image-one.png"],
                display_metadata={"user_id": "user-1"},
            ),
        }
    )
    response = await connection.recv()

    prompt = FakeProxy.instances[0].requests[1]
    assert prompt["method"] == "prompt.submit"
    assert prompt["params"] == {
        "session_id": "live-session-1",
        "text": "execution text",
        "client_message_id": "message-1",
        "queued": True,
        "display_text": "display text",
        "submitted_at": 1_000.0,
        "attachment_refs": ["/tmp/image-one.png", "/tmp/document.pdf"],
        "image_paths": ["/tmp/image-one.png"],
        "display_metadata": {"user_id": "user-1"},
    }
    assert response == {
        "kind": "control.response",
        "protocol": 1,
        "request_id": "request-1",
        "result": {"status": "streaming"},
    }


@pytest.mark.asyncio
async def test_close_closes_proxy_once(owner):
    connection = _connection(owner)
    await connection.send(
        frontend_hello(
            client_id="gateway-v1:client",
            principal={
                "provider": "discord",
                "subject": "user-1",
                "authenticated": True,
            },
            surface="discord",
            requested_capabilities={"observe", "prompt.submit", "interaction.respond"},
            durable_root="root-1",
        )
    )
    await connection.recv()

    await connection.close()
    await connection.close()

    assert FakeProxy.instances[0].closed is True
