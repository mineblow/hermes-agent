from __future__ import annotations

from datetime import UTC, datetime

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
