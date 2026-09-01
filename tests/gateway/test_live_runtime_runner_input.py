from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.platforms.base import MessageEvent, Platform
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from hermes_cli.live_runtime_owners import RuntimeOwner


class SessionDB:
    def session_runtime_key(self, session_id):
        assert session_id == "session-1"
        return "root-1"


class Client:
    accepted_capabilities = frozenset({
        "observe",
        "prompt.submit",
        "session.steer",
        "session.interrupt",
        "approval.respond",
        "clarify.respond",
        "interaction.respond",
    })
    owner_id = "owner-1"
    owner_generation = 2
    runtime_id = "runtime-1"
    durable_session_id = "session-1"
    replay_watermark = ("epoch-1", 0)

    def __init__(self, *, request_error=None):
        self.requests = []
        self.started = 0
        self.closed = 0
        self.request_error = request_error

    async def start(self):
        self.started += 1

    async def request(self, payload):
        self.requests.append(payload)
        if self.request_error is not None:
            raise self.request_error
        return {"status": "streaming"}

    async def close(self):
        self.closed += 1


def _source():
    return SessionSource(
        platform=Platform.DISCORD,
        scope_id="guild-1",
        chat_id="channel-1",
        thread_id="thread-1",
        user_id="user-1",
        user_name="Ethan",
        profile="worker",
    )


def _runner(tmp_path, owner):
    runner = object.__new__(GatewayRunner)
    runner._sessions = {}
    runner._session_db = SessionDB()
    runner._live_runtime_registry_home = str(tmp_path)
    runner._resolve_profile_home_for_source = lambda source: tmp_path / "profiles" / "worker"

    async def lookup(_conversation_key):
        return owner

    runner._lookup_live_runtime_owner = lookup
    return runner


def _owner(tmp_path, *, profile_home=None):
    return RuntimeOwner(
        conversation_key="v1:owner-key",
        owner_id="owner-1",
        generation=2,
        pid=123,
        process_start_time=1.0,
        endpoint=str(tmp_path / "runtime.sock"),
        profile_home=str(profile_home or (tmp_path / "profiles" / "worker")),
        surface="tui_gateway",
        started_at=1.0,
    )


@pytest.mark.asyncio
async def test_authorized_message_routes_to_canonical_scheduler_without_local_agent(tmp_path):
    owner = _owner(tmp_path)
    runner = _runner(tmp_path, owner)
    client = Client()
    runner._make_live_runtime_client = lambda request: client
    event = MessageEvent(
        text="hello",
        source=_source(),
        message_id="native-1",
        timestamp=datetime(2026, 9, 1, tzinfo=UTC),
    )

    routed = await runner._try_route_live_runtime_input(
        event=event,
        source=event.source,
        session_key="worker:discord:guild-1:channel-1:thread-1",
        durable_session_id="session-1",
    )

    assert routed is True
    assert client.started == 1
    assert len(client.requests) == 1
    assert client.requests[0]["kind"] == "runtime.input"
    assert runner._running_agent_items() == []


def test_runner_client_factory_registers_renderer_before_start(monkeypatch, tmp_path):
    runner = _runner(tmp_path, _owner(tmp_path))

    class Renderer:
        async def on_event(self, _event):
            pass

        def register_local_message_id(self, _message_id):
            pass

        async def close(self):
            pass

        async def reset(self, _signal=None):
            pass

    renderer = Renderer()
    interaction_callback = {}

    def make_renderer(source, *, on_interaction=None):
        interaction_callback["callback"] = on_interaction
        return renderer

    runner._make_live_runtime_renderer = make_renderer
    runner._adapter_for_source = lambda _source: SimpleNamespace()
    runner._thread_metadata_for_source = lambda _source: {}
    captured = {}

    class CapturingClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(
        "gateway.live_runtime_client.AsyncLiveRuntimeClient", CapturingClient
    )
    request = SimpleNamespace(
        conversation_key="conversation-1",
        durable_root="root-1",
        stable_client_id="gateway-client-1",
        principal_id="user-1",
        surface="discord",
        requested_capabilities=frozenset({"observe"}),
        durable_session_id="session-1",
        routing_key=SimpleNamespace(profile="worker"),
        source=_source(),
    )

    runner._make_live_runtime_client(request)

    assert captured["on_event"] == renderer.on_event
    assert captured["on_resync_required"] == renderer.reset
    assert captured["on_local_message_id"] == renderer.register_local_message_id
    assert callable(captured["on_close"])
    assert interaction_callback["callback"] is not None


@pytest.mark.asyncio
async def test_runner_correlates_native_approvals_and_normalizes_legacy_approve(
    monkeypatch, tmp_path
):
    from gateway.stream_events import InteractionRequest
    from tools.approval import resolve_gateway_approval

    runner = _runner(tmp_path, _owner(tmp_path))
    callbacks = {}

    class Renderer:
        async def on_event(self, _event):
            pass

        def register_local_message_id(self, _message_id):
            pass

        async def reset(self, _signal=None):
            pass

        async def close(self):
            pass

    def make_renderer(_source, *, on_interaction=None):
        callbacks["interaction"] = on_interaction
        return Renderer()

    approval_keys = []
    confirmations = []

    class Adapter:
        async def send_exec_approval(self, **kwargs):
            approval_keys.append(kwargs["session_key"])
            return SimpleNamespace(success=True)

        async def send(self, _chat_id, text, **_kwargs):
            confirmations.append(text)
            return SimpleNamespace(success=True)

    created = {}

    class CapturingClient:
        accepted_capabilities = frozenset({"observe", "interaction.respond"})

        def __init__(self, **kwargs):
            created["client"] = self
            created["on_close"] = kwargs["on_close"]
            self.responses = []

        async def respond_to_interaction(self, response):
            self.responses.append(response)
            if response["request_id"] == "approval-1":
                return {"status": "expired", "resolved": 0}
            return {"status": "resolved", "resolved": 1}

    runner._make_live_runtime_renderer = make_renderer
    runner._adapter_for_source = lambda _source: Adapter()
    runner._thread_metadata_for_source = lambda _source: {"thread_id": "thread-1"}
    monkeypatch.setattr(
        "gateway.live_runtime_client.AsyncLiveRuntimeClient", CapturingClient
    )
    request = SimpleNamespace(
        conversation_key="conversation-1",
        durable_root="root-1",
        stable_client_id="gateway-client-1",
        principal_id="user-1",
        surface="discord",
        requested_capabilities=frozenset({"observe", "interaction.respond"}),
        durable_session_id="session-1",
        routing_key=SimpleNamespace(profile="worker"),
        source=_source(),
    )
    runner._make_live_runtime_client(request)

    callbacks["interaction"](
        InteractionRequest(
            request_id="approval-1",
            interaction_type="approval",
            payload={"command": "first", "description": "first approval"},
        )
    )
    callbacks["interaction"](
        InteractionRequest(
            request_id="approval-2",
            interaction_type="approval",
            payload={"command": "second", "description": "second approval"},
        )
    )
    for _ in range(20):
        if len(approval_keys) == 2:
            break
        await asyncio.sleep(0.01)

    assert len(approval_keys) == 2
    assert approval_keys[0] != approval_keys[1]
    assert resolve_gateway_approval(approval_keys[1], "once") == 1
    for _ in range(50):
        if created["client"].responses:
            break
        await asyncio.sleep(0.01)
    assert created["client"].responses == [
        {
            "interaction_type": "approval",
            "request_id": "approval-2",
            "choice": "once",
        }
    ]

    assert resolve_gateway_approval(approval_keys[0], "approve") == 1
    for _ in range(50):
        if len(created["client"].responses) == 2:
            break
        await asyncio.sleep(0.01)
    assert created["client"].responses[1] == {
        "interaction_type": "approval",
        "request_id": "approval-1",
        "choice": "once",
    }
    assert confirmations == [
        "✅ Approved by the active runtime.",
        "⌛ Approval expired or was resolved elsewhere; "
        "the command was not approved by this response.",
    ]

    real_wall_time = time.time()
    monkeypatch.setattr("gateway.run.time.time", lambda: real_wall_time - 3600.0)
    callbacks["interaction"](
        InteractionRequest(
            request_id="approval-expiring",
            interaction_type="approval",
            payload={
                "command": "expired",
                "description": "expired approval",
                "expires_at": real_wall_time + 0.05,
                "timeout_seconds": 0.05,
            },
        )
    )
    for _ in range(50):
        if len(approval_keys) == 3:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.1)
    assert resolve_gateway_approval(approval_keys[2], "once") == 0
    await created["on_close"]()


@pytest.mark.asyncio
async def test_runner_forwards_native_clarification_to_canonical_owner(
    monkeypatch, tmp_path
):
    from gateway.stream_events import InteractionRequest
    from tools.clarify_gateway import resolve_gateway_clarify

    runner = _runner(tmp_path, _owner(tmp_path))
    callbacks = {}

    class Renderer:
        async def on_event(self, _event):
            pass

        def register_local_message_id(self, _message_id):
            pass

        async def reset(self, _signal=None):
            pass

        async def close(self):
            pass

    def make_renderer(_source, *, on_interaction=None):
        callbacks["interaction"] = on_interaction
        return Renderer()

    presented = []

    class Adapter:
        async def send_clarify(self, **kwargs):
            presented.append(kwargs["clarify_id"])
            return SimpleNamespace(success=True)

    created = {}

    class CapturingClient:
        accepted_capabilities = frozenset({"observe", "interaction.respond"})

        def __init__(self, **kwargs):
            created["client"] = self
            created["on_close"] = kwargs["on_close"]
            self.responses = []

        async def respond_to_interaction(self, response):
            self.responses.append(response)
            return {"status": "accepted"}

    runner._make_live_runtime_renderer = make_renderer
    runner._adapter_for_source = lambda _source: Adapter()
    runner._thread_metadata_for_source = lambda _source: {}
    monkeypatch.setattr(
        "gateway.live_runtime_client.AsyncLiveRuntimeClient", CapturingClient
    )
    request = SimpleNamespace(
        conversation_key="conversation-1",
        durable_root="root-1",
        stable_client_id="gateway-client-1",
        principal_id="user-1",
        surface="discord",
        requested_capabilities=frozenset({"observe", "interaction.respond"}),
        durable_session_id="session-1",
        routing_key=SimpleNamespace(profile="worker"),
        source=_source(),
    )
    runner._make_live_runtime_client(request)

    callbacks["interaction"](
        InteractionRequest(
            request_id="clarify-1",
            interaction_type="clarification",
            payload={"question": "Which environment?"},
        )
    )
    callbacks["interaction"](
        InteractionRequest(
            request_id="clarify-2",
            interaction_type="clarification",
            payload={"question": "Which region?"},
        )
    )
    for _ in range(20):
        if presented:
            break
        await asyncio.sleep(0.01)

    assert presented == ["clarify-1"]
    assert resolve_gateway_clarify("clarify-2", "west") is False
    assert resolve_gateway_clarify("clarify-1", "production") is True
    for _ in range(50):
        if presented == ["clarify-1", "clarify-2"]:
            break
        await asyncio.sleep(0.01)
    assert presented == ["clarify-1", "clarify-2"]
    assert resolve_gateway_clarify("clarify-2", "west") is True
    for _ in range(50):
        if len(created["client"].responses) == 2:
            break
        await asyncio.sleep(0.01)

    assert created["client"].responses == [
        {
            "interaction_type": "clarification",
            "request_id": "clarify-1",
            "answer": "production",
        },
        {
            "interaction_type": "clarification",
            "request_id": "clarify-2",
            "answer": "west",
        },
    ]
    await created["on_close"]()


@pytest.mark.asyncio
async def test_runner_renderer_delivers_canonical_completion_once_without_persistence(
    monkeypatch, tmp_path
):
    from gateway.stream_consumer import StreamConsumerConfig
    from gateway.stream_events import Commentary, MessageChunk, MessageStop

    runner = _runner(tmp_path, _owner(tmp_path))
    runner.config = SimpleNamespace(
        streaming=SimpleNamespace(enabled=False, transport="off")
    )
    runner._session_db = MagicMock()
    adapter = SimpleNamespace(
        send=AsyncMock(
            return_value=SimpleNamespace(success=True, message_id="delivered-1")
        ),
        edit_message=AsyncMock(),
        MAX_MESSAGE_LENGTH=4096,
        REQUIRES_EDIT_FINALIZE=False,
        SENDS_FINALIZE_NOTIFICATION=False,
        platform_name="discord",
    )

    def render_message_event(event, sink):
        if isinstance(event, MessageChunk):
            sink.on_delta(event.text)
        elif isinstance(event, Commentary):
            sink.on_commentary(event.text)
        elif isinstance(event, MessageStop) and event.final:
            sink.finish(final_text=event.text)

    adapter.render_message_event = render_message_event
    adapter.format_tool_event = lambda *_args, **_kwargs: None
    runner._adapter_for_source = lambda source: adapter
    runner._thread_metadata_for_source = lambda source: {"thread_id": source.thread_id}
    runner._build_stream_consumer_config = lambda *_args, **_kwargs: (
        StreamConsumerConfig(buffer_only=True, transport="edit", cursor=""),
        None,
    )
    monkeypatch.setattr(
        "gateway.display_config.resolve_display_setting", lambda *_args: False
    )
    renderer = runner._make_live_runtime_renderer(_source())

    def event(event_type, payload, seq):
        return {
            "kind": "runtime.event",
            "protocol": 1,
            "runtime_id": "runtime-1",
            "durable_session_id": "session-1",
            "replay_epoch": "epoch-1",
            "seq": seq,
            "type": event_type,
            "payload": payload,
        }

    await renderer.on_event(event("message.delta", {"text": "hello"}, 1))
    await renderer.on_event(
        event("message.complete", {"text": "hello", "status": "ok"}, 2)
    )

    adapter.send.assert_awaited_once()
    assert adapter.send.await_args.kwargs["chat_id"] == "channel-1"
    assert adapter.send.await_args.kwargs["content"] == "hello"
    runner._session_db.assert_not_called()


@pytest.mark.asyncio
async def test_missing_owner_falls_back_before_client_or_scheduler_creation(tmp_path):
    runner = _runner(tmp_path, None)
    created = []
    runner._make_live_runtime_client = lambda request: created.append(request)
    event = MessageEvent(text="hello", source=_source(), message_id="native-1")

    routed = await runner._try_route_live_runtime_input(
        event=event,
        source=event.source,
        session_key="session-key",
        durable_session_id="session-1",
    )

    assert routed is False
    assert created == []
    assert runner._running_agent_items() == []


@pytest.mark.asyncio
async def test_existing_attachment_remains_canonical_during_registry_miss(tmp_path):
    runner = _runner(tmp_path, _owner(tmp_path))
    client = Client()
    runner._make_live_runtime_client = lambda request: client
    event = MessageEvent(text="first", source=_source(), message_id="native-1")

    assert await runner._try_route_live_runtime_input(
        event=event,
        source=event.source,
        session_key="session-key",
        durable_session_id="session-1",
    ) is True

    async def missing_owner(_conversation_key):
        return None

    runner._lookup_live_runtime_owner = missing_owner
    redelivery = MessageEvent(text="second", source=_source(), message_id="native-2")

    assert await runner._try_route_live_runtime_input(
        event=redelivery,
        source=redelivery.source,
        session_key="session-key",
        durable_session_id="session-1",
    ) is True
    assert client.started == 1
    assert [request["text"] for request in client.requests] == ["first", "second"]


@pytest.mark.asyncio
async def test_profile_mismatched_owner_fails_closed_to_local_path(tmp_path):
    owner = _owner(tmp_path, profile_home=tmp_path / "profiles" / "other")
    runner = _runner(tmp_path, owner)
    created = []
    runner._make_live_runtime_client = lambda request: created.append(request)
    event = MessageEvent(text="hello", source=_source(), message_id="native-1")

    assert await runner._try_route_live_runtime_input(
        event=event,
        source=event.source,
        session_key="session-key",
        durable_session_id="session-1",
    ) is False
    assert created == []


@pytest.mark.asyncio
async def test_submission_failure_is_not_replayed_through_local_fallback(tmp_path):
    owner = _owner(tmp_path)
    runner = _runner(tmp_path, owner)
    client = Client(request_error=RuntimeError("unknown execution outcome"))
    runner._make_live_runtime_client = lambda request: client
    event = MessageEvent(text="hello", source=_source(), message_id="native-1")

    with pytest.raises(RuntimeError, match="unknown execution outcome"):
        await runner._try_route_live_runtime_input(
            event=event,
            source=event.source,
            session_key="session-key",
            durable_session_id="session-1",
        )

    assert len(client.requests) == 1
    assert runner._running_agent_items() == []


@pytest.mark.asyncio
async def test_internal_event_retains_process_local_execution(tmp_path):
    runner = _runner(tmp_path, _owner(tmp_path))
    event = MessageEvent(
        text="internal",
        source=_source(),
        message_id="native-1",
        internal=True,
    )

    assert await runner._try_route_live_runtime_input(
        event=event,
        source=event.source,
        session_key="session-key",
        durable_session_id="session-1",
    ) is False


@pytest.mark.asyncio
async def test_preprocessed_route_separates_model_text_from_display_text(tmp_path):
    runner = _runner(tmp_path, _owner(tmp_path))
    client = Client()
    runner._make_live_runtime_client = lambda _request: client
    event = MessageEvent(
        text="raw user text",
        user_id="user-1",
        user_name="Current Speaker",
        source=_source(),
        message_id="native-1",
        media_urls=["/tmp/image-one.png", "/tmp/document.pdf"],
        media_types=["image/png", "application/pdf"],
    )

    async def prepare(**_kwargs):
        return "[Current Speaker] enriched model text"

    runner._prepare_profile_scoped_inbound_message_text = prepare
    runner._consume_pending_native_image_paths = lambda _key: ["/tmp/image-one.png"]

    routed = await runner._try_route_preprocessed_live_runtime_input(
        event=event,
        source=event.source,
        session_key="session-key",
        durable_session_id="session-1",
        history=[],
    )

    assert routed is True
    request = client.requests[0]
    assert request["text"] == "[Current Speaker] enriched model text"
    assert request["display_text"] == "raw user text"
    assert request["attachment_refs"] == [
        "/tmp/image-one.png",
        "/tmp/document.pdf",
    ]
    assert request["image_refs"] == ["/tmp/image-one.png"]
    assert runner._running_agent_items() == []
