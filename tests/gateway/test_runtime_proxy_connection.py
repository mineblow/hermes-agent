from __future__ import annotations

import asyncio
import threading

import pytest

from gateway.runtime_proxy_connection import (
    RuntimeProxyAsyncConnection,
    RuntimeProxyConnectionError,
)
from hermes_cli.live_runtime_owners import RuntimeOwner
from hermes_cli.live_runtime_protocol import frontend_hello, runtime_input


class FakeProxy:
    instances = []
    replay_result = {
        "events": [],
        "latest_seq": 0,
        "truncated": False,
        "epoch": "epoch-1",
    }
    live_event_during_replay = None
    live_events_during_replay = []
    snapshot_result = {
        "session_id": "live-session-1",
        "reconcilable": True,
        "latest_assistant": None,
    }
    live_event_during_snapshot = None
    resume_result = {"session_id": "live-session-1"}

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
                "result": dict(self.resume_result),
            }
        if frame["method"] == "session.events.since":
            if self.live_event_during_replay is not None:
                self.kwargs["on_event"](self.live_event_during_replay)
            for event in self.live_events_during_replay:
                self.kwargs["on_event"](event)
            return {
                "jsonrpc": "2.0",
                "id": frame["id"],
                "result": dict(self.replay_result),
            }
        if frame["method"] == "session.presentation.snapshot":
            if self.live_event_during_snapshot is not None:
                self.kwargs["on_event"](dict(self.live_event_during_snapshot))
            return {
                "jsonrpc": "2.0",
                "id": frame["id"],
                "result": dict(self.snapshot_result),
            }
        return {
            "jsonrpc": "2.0",
            "id": frame["id"],
            "result": {"status": "streaming"},
        }


@pytest.fixture(autouse=True)
def clear_instances():
    FakeProxy.instances.clear()
    FakeProxy.replay_result = {
        "events": [],
        "latest_seq": 0,
        "truncated": False,
        "epoch": "epoch-1",
    }
    FakeProxy.live_event_during_replay = None
    FakeProxy.live_events_during_replay = []
    FakeProxy.snapshot_result = {
        "session_id": "live-session-1",
        "reconcilable": True,
        "latest_assistant": None,
    }
    FakeProxy.live_event_during_snapshot = None
    FakeProxy.resume_result = {"session_id": "live-session-1"}


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
        {"observe", "prompt.submit", "approval.respond", "clarify.respond"}
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
    assert acknowledgement["accepted_capabilities"] == [
        "interaction.respond",
        "observe",
        "prompt.submit",
    ]
    negotiated = FakeProxy.instances[0].kwargs["negotiated_capabilities"]
    assert negotiated == frozenset(
        {"observe", "prompt.submit", "approval.respond", "clarify.respond"}
    )
    assert "ui.respond" not in negotiated


@pytest.mark.asyncio
async def test_controller_interaction_uses_explicit_owner_rpc(owner):
    connection = RuntimeProxyAsyncConnection(
        owner=owner,
        durable_session_id="stored-session-1",
        profile="work",
        client_id="client-1",
        requested_capabilities=frozenset(
            {"observe", "prompt.submit", "interaction.respond"}
        ),
        proxy_factory=FakeProxy,
    )
    await connection.send(
        frontend_hello(
            client_id="client-1",
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

    await connection.send({
        "kind": "interaction.respond",
        "protocol": 1,
        "request_id": "response-1",
        "payload": {
            "interaction_type": "approval",
            "request_id": "approval-1",
            "choice": "once",
        },
    })

    response = await connection.recv()
    assert response["request_id"] == "response-1"
    assert FakeProxy.instances[0].requests[-1] == {
        "jsonrpc": "2.0",
        "id": "response-1",
        "method": "approval.respond",
        "params": {
            "session_id": "live-session-1",
            "request_id": "approval-1",
            "choice": "once",
        },
    }
    await connection.close()


@pytest.mark.asyncio
async def test_controller_hello_restores_owner_pending_interactions(owner):
    FakeProxy.resume_result = {
        "session_id": "live-session-1",
        "pending_approvals": [
            {
                "request_id": "approval-pending",
                "command": "deploy",
                "choices": ["once", "deny"],
            },
            {
                "request_id": "approval-pending-2",
                "command": "restart",
                "choices": ["once", "deny"],
            },
        ],
        "pending_clarify": {
            "request_id": "clarify-pending",
            "question": "Which environment?",
        },
    }
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
    assert acknowledgement["pending_interactions"] == [
        {
            "interaction_type": "approval",
            "request_id": "approval-pending",
            "payload": {
                "request_id": "approval-pending",
                "command": "deploy",
                "choices": ["once", "deny"],
            },
        },
        {
            "interaction_type": "approval",
            "request_id": "approval-pending-2",
            "payload": {
                "request_id": "approval-pending-2",
                "command": "restart",
                "choices": ["once", "deny"],
            },
        },
        {
            "interaction_type": "clarification",
            "request_id": "clarify-pending",
            "payload": {
                "request_id": "clarify-pending",
                "question": "Which environment?",
            },
        },
    ]
    await connection.close()


@pytest.mark.asyncio
async def test_fresh_attachment_adopts_snapshot_without_replaying_history(owner):
    FakeProxy.replay_result = {
        "events": [
            {
                "type": "message.complete",
                "session_id": "live-session-1",
                "seq": 1,
                "epoch": "epoch-1",
                "payload": {"text": "old answer"},
            },
            {
                "type": "message.user",
                "session_id": "live-session-1",
                "seq": 2,
                "epoch": "epoch-1",
                "payload": {"message_id": "old-user", "text": "old prompt"},
            },
        ],
        "latest_seq": 2,
        "truncated": False,
        "epoch": "epoch-1",
    }
    FakeProxy.live_event_during_replay = {
        "jsonrpc": "2.0",
        "method": "event",
        "params": {
            "type": "message.start",
            "session_id": "live-session-1",
            "seq": 3,
            "epoch": "epoch-1",
        },
    }
    connection = _connection(owner)

    await connection.send(
        frontend_hello(
            client_id="gateway-v1:client",
            principal={"provider": "discord", "subject": "u", "authenticated": True},
            surface="discord",
            requested_capabilities={"observe", "prompt.submit", "interaction.respond"},
            durable_root="root-1",
        )
    )

    acknowledgement = await connection.recv()
    assert acknowledgement["replay_seq"] == 2
    assert acknowledgement["replay_truncated"] is False
    assert await connection.recv() == {
        "kind": "runtime.event",
        "protocol": 1,
        "runtime_id": "live-session-1",
        "durable_session_id": "stored-session-1",
        "replay_epoch": "epoch-1",
        "seq": 3,
        "type": "message.start",
        "payload": {},
    }
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(connection.recv(), timeout=0.01)


@pytest.mark.asyncio
async def test_initial_replay_overflow_fences_before_acknowledgement(owner):
    FakeProxy.live_events_during_replay = [
        {
            "jsonrpc": "2.0",
            "method": "event",
            "params": {
                "type": "message.delta",
                "session_id": "live-session-1",
                "seq": seq,
                "epoch": "epoch-1",
                "payload": {"text": str(seq)},
            },
        }
        for seq in range(1, 66)
    ]
    connection = _connection(owner)

    with pytest.raises(RuntimeProxyConnectionError, match="overflowed during replay"):
        await connection.send(
            frontend_hello(
                client_id="gateway-v1:client",
                principal={"provider": "discord", "subject": "u", "authenticated": True},
                surface="discord",
                requested_capabilities={"observe", "prompt.submit", "interaction.respond"},
                durable_root="root-1",
            )
        )

    assert FakeProxy.instances[0].closed is True


@pytest.mark.asyncio
async def test_observer_never_resumes_as_controller(owner):
    connection = RuntimeProxyAsyncConnection(
        owner=owner,
        durable_session_id="stored-session-1",
        profile="worker",
        client_id="gateway-v1:observer",
        requested_capabilities=frozenset({"observe"}),
        proxy_factory=FakeProxy,
    )
    await connection.send(
        frontend_hello(
            client_id="gateway-v1:observer",
            principal={"provider": "discord", "subject": "u", "authenticated": True},
            surface="discord",
            requested_capabilities={"observe"},
            durable_root="root-1",
        )
    )

    acknowledgement = await connection.recv()

    assert FakeProxy.instances[0].requests[0]["params"]["attachment_mode"] == "observe"
    assert acknowledgement["accepted_capabilities"] == ["observe"]


@pytest.mark.asyncio
async def test_proxy_reader_thread_event_reaches_async_connection(owner):
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
    proxy = FakeProxy.instances[0]
    legacy_event = {
        "jsonrpc": "2.0",
        "method": "event",
        "params": {
            "type": "message.delta",
            "session_id": "live-session-1",
            "seq": 7,
            "epoch": "owner-epoch-1",
            "payload": {"text": "hello"},
        },
    }

    thread = threading.Thread(
        target=lambda: proxy.kwargs["on_event"](legacy_event)
    )
    thread.start()
    thread.join()

    event = await asyncio.wait_for(connection.recv(), timeout=0.1)
    assert event == {
        "kind": "runtime.event",
        "protocol": 1,
        "runtime_id": "live-session-1",
        "durable_session_id": "stored-session-1",
        "replay_epoch": "owner-epoch-1",
        "seq": 7,
        "type": "message.delta",
        "payload": {"text": "hello"},
    }


@pytest.mark.asyncio
async def test_truncated_snapshot_keeps_live_event_behind_hello(owner):
    FakeProxy.replay_result = {
        "events": [],
        "epoch": "epoch-1",
        "latest_seq": 5,
        "truncated": True,
    }
    FakeProxy.live_event_during_snapshot = {
        "jsonrpc": "2.0",
        "method": "event",
        "params": {
            "type": "message.delta",
            "session_id": "live-session-1",
            "seq": 6,
            "epoch": "epoch-1",
            "payload": {"text": "live-after-gap"},
        },
    }
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
            replay_epoch="epoch-1",
            replay_seq=1,
        )
    )

    acknowledgement = await connection.recv()
    live_event = await connection.recv()
    assert acknowledgement["kind"] == "frontend.hello.ok"
    assert acknowledgement["replay_truncated"] is True
    assert live_event["kind"] == "runtime.event"
    assert live_event["seq"] == 6
    await connection.close()


@pytest.mark.asyncio
async def test_truncated_snapshot_suppresses_already_presented_assistant(owner):
    FakeProxy.replay_result = {
        "events": [],
        "epoch": "epoch-1",
        "latest_seq": 10,
        "truncated": True,
    }
    FakeProxy.snapshot_result = {
        "session_id": "live-session-1",
        "reconcilable": True,
        "latest_assistant": {
            "text": "already delivered",
            "row_id": 7,
            "completion_seq": 5,
        },
    }
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
            replay_epoch="epoch-1",
            replay_seq=7,
        )
    )

    acknowledgement = await connection.recv()
    assert acknowledgement["resync_snapshot"] == {"latest_assistant": None}
    await connection.close()


@pytest.mark.asyncio
async def test_proxy_disconnect_unblocks_async_receiver(owner):
    connection = _connection(owner)
    await connection.send(
        frontend_hello(
            client_id="gateway-v1:client",
            principal={"provider": "discord", "subject": "u", "authenticated": True},
            surface="discord",
            requested_capabilities={"observe", "prompt.submit", "interaction.respond"},
            durable_root="root-1",
        )
    )
    await connection.recv()

    thread = threading.Thread(
        target=FakeProxy.instances[0].kwargs["on_disconnect"]
    )
    thread.start()
    thread.join()

    with pytest.raises(RuntimeProxyConnectionError, match="disconnected"):
        await asyncio.wait_for(connection.recv(), timeout=0.2)


@pytest.mark.asyncio
async def test_event_queue_overflow_unblocks_receiver_and_fences_connection(owner):
    connection = _connection(owner)
    await connection.send(
        frontend_hello(
            client_id="gateway-v1:client",
            principal={"provider": "discord", "subject": "u", "authenticated": True},
            surface="discord",
            requested_capabilities={"observe", "prompt.submit", "interaction.respond"},
            durable_root="root-1",
        )
    )
    await connection.recv()
    for seq in range(1, 66):
        connection._enqueue_event(
            {
                "type": "message.delta",
                "session_id": "live-session-1",
                "seq": seq,
                "epoch": "epoch-1",
                "payload": {"text": str(seq)},
            }
        )

    with pytest.raises(RuntimeProxyConnectionError, match="delivery failed"):
        await asyncio.wait_for(connection.recv(), timeout=0.2)


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

    prompt = FakeProxy.instances[0].requests[-1]
    assert prompt["method"] == "prompt.submit"
    assert prompt["params"] == {
        "session_id": "live-session-1",
        "text": "execution text",
        "client_message_id": "message-1",
        "busy_policy": "queue",
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


@pytest.mark.asyncio
async def test_hello_orders_authoritative_replay_before_concurrent_live_event(owner):
    FakeProxy.replay_result = {
        "events": [
            {"type": "message.delta", "session_id": "live-session-1", "seq": 1, "epoch": "authoritative-epoch", "payload": {"text": "one"}},
            {"type": "message.delta", "session_id": "live-session-1", "seq": 2, "epoch": "authoritative-epoch", "payload": {"text": "two"}},
        ],
        "latest_seq": 3,
        "truncated": False,
        "epoch": "authoritative-epoch",
    }
    FakeProxy.live_event_during_replay = {
        "jsonrpc": "2.0",
        "method": "event",
        "params": {
            "type": "message.complete",
            "session_id": "live-session-1",
            "seq": 3,
            "epoch": "authoritative-epoch",
            "payload": {"text": "one two three"},
        },
    }
    connection = _connection(owner)

    await connection.send(
        frontend_hello(
            client_id="gateway-v1:client",
            principal={"provider": "discord", "subject": "u", "authenticated": True},
            surface="discord",
            requested_capabilities={"observe", "prompt.submit", "interaction.respond"},
            durable_root="root-1",
            replay_epoch="authoritative-epoch",
            replay_seq=0,
        )
    )

    acknowledgement = await connection.recv()
    events = [await connection.recv() for _ in range(3)]
    assert acknowledgement["replay_epoch"] == "authoritative-epoch"
    assert [event["seq"] for event in events] == [1, 2, 3]
    assert FakeProxy.instances[0].requests[1]["method"] == "session.events.since"


@pytest.mark.asyncio
async def test_truncated_replay_advertises_authoritative_baseline(owner):
    FakeProxy.replay_result = {
        "events": [
            {"type": "message.delta", "session_id": "live-session-1", "seq": 8, "epoch": "epoch-1", "payload": {"text": "stale fragment"}}
        ],
        "latest_seq": 9,
        "truncated": True,
        "epoch": "epoch-1",
    }
    connection = _connection(owner)
    await connection.send(
        frontend_hello(
            client_id="gateway-v1:client",
            principal={"provider": "discord", "subject": "u", "authenticated": True},
            surface="discord",
            requested_capabilities={"observe", "prompt.submit", "interaction.respond"},
            durable_root="root-1",
            replay_epoch="epoch-1",
            replay_seq=7,
        )
    )

    acknowledgement = await connection.recv()
    assert acknowledgement["replay_truncated"] is True
    assert acknowledgement["replay_seq"] == 9
    assert connection._received.empty()
