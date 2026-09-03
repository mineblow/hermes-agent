from __future__ import annotations

import asyncio

import pytest

from gateway.live_runtime_bridge import (
    AttachmentMode,
    LiveRuntimeBridge,
    LiveRuntimeRoutingKey,
    profile_scoped_runtime_key,
)
from gateway.platforms.base import Platform
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from gateway.session_state import SessionState


class FakeClient:
    def __init__(self, request):
        self.request = request
        self.accepted_capabilities = request.requested_capabilities
        self.owner_id = "owner-1"
        self.owner_generation = 4
        self.runtime_id = "runtime-1"
        self.durable_session_id = request.durable_session_id
        self.replay_watermark = ("epoch-2", 17)
        self.started = 0
        self.closed = 0

    async def start(self):
        self.started += 1

    async def close(self):
        self.closed += 1


def _source(*, profile=None, user_id="user-1"):
    return SessionSource(
        platform=Platform.DISCORD,
        chat_id="channel-1",
        thread_id="thread-1",
        scope_id="workspace-1",
        user_id=user_id,
        profile=profile,
    )


def _bridge():
    states = {}

    def state_for(session_key):
        return states.setdefault(session_key, SessionState())

    return LiveRuntimeBridge(state_for), states


@pytest.mark.asyncio
async def test_concurrent_attach_reuses_one_client_and_stable_opaque_identity():
    bridge, states = _bridge()
    source = _source()
    creations = []

    async def factory(request):
        creations.append(request)
        await asyncio.sleep(0)
        return FakeClient(request)

    first, second = await asyncio.gather(
        bridge.attach(
            session_key="discord:channel-1:thread-1",
            source=source,
            principal_id="principal-1",
            durable_root="root-1",
            durable_session_id="session-1",
            profile_home="/profiles/default",
            mode=AttachmentMode.CONTROL,
            client_factory=factory,
        ),
        bridge.attach(
            session_key="discord:channel-1:thread-1",
            source=source,
            principal_id="principal-1",
            durable_root="root-1",
            durable_session_id="session-1",
            profile_home="/profiles/default",
            mode=AttachmentMode.CONTROL,
            client_factory=factory,
        ),
    )

    assert first is second
    assert len(creations) == 1
    assert first.client.started == 1
    assert first.stable_client_id.startswith("gateway-")
    assert "principal-1" not in first.stable_client_id
    assert len(states["discord:channel-1:thread-1"].live_runtime_attachments) == 1


@pytest.mark.asyncio
async def test_profile_and_principal_are_part_of_authorized_routing_key():
    bridge, _states = _bridge()
    created = []

    async def factory(request):
        client = FakeClient(request)
        created.append(client)
        return client

    default = await bridge.attach(
        session_key="default-session",
        source=_source(),
        principal_id="principal-1",
        durable_root="root-default",
        durable_session_id="session-default",
        profile_home="/profiles/default",
        mode=AttachmentMode.OBSERVE,
        client_factory=factory,
    )
    worker = await bridge.attach(
        session_key="worker-session",
        source=_source(profile="worker"),
        principal_id="principal-1",
        durable_root="root-worker",
        durable_session_id="session-worker",
        profile_home="/profiles/worker",
        mode=AttachmentMode.OBSERVE,
        client_factory=factory,
    )
    other_principal = await bridge.attach(
        session_key="default-session",
        source=_source(user_id="user-2"),
        principal_id="principal-2",
        durable_root="root-default",
        durable_session_id="session-default",
        profile_home="/profiles/default",
        mode=AttachmentMode.OBSERVE,
        client_factory=factory,
    )

    assert len(created) == 3
    assert len({
        default.stable_client_id,
        worker.stable_client_id,
        other_principal.stable_client_id,
    }) == 3
    assert default.routing_key.profile == "default"
    assert worker.routing_key.profile == "worker"


@pytest.mark.asyncio
async def test_observer_capabilities_are_fail_closed_while_controller_can_submit():
    bridge, _states = _bridge()
    requests = []

    async def factory(request):
        requests.append(request)
        return FakeClient(request)

    observer = await bridge.attach(
        session_key="observer-session",
        source=_source(user_id="observer"),
        principal_id="observer-principal",
        durable_root="root-1",
        durable_session_id="session-1",
        profile_home="/profiles/default",
        mode=AttachmentMode.OBSERVE,
        client_factory=factory,
    )
    controller = await bridge.attach(
        session_key="controller-session",
        source=_source(user_id="controller"),
        principal_id="controller-principal",
        durable_root="root-2",
        durable_session_id="session-2",
        profile_home="/profiles/default",
        mode=AttachmentMode.CONTROL,
        client_factory=factory,
    )

    assert observer.accepted_capabilities == frozenset({"observe"})
    assert "prompt.submit" not in requests[0].requested_capabilities
    assert "prompt.submit" in requests[1].requested_capabilities
    assert controller.mode is AttachmentMode.CONTROL


@pytest.mark.asyncio
async def test_detach_removes_state_and_closes_exactly_once():
    bridge, states = _bridge()

    async def factory(request):
        return FakeClient(request)

    attachment = await bridge.attach(
        session_key="session-key",
        source=_source(),
        principal_id="principal-1",
        durable_root="root-1",
        durable_session_id="session-1",
        profile_home="/profiles/default",
        mode=AttachmentMode.CONTROL,
        client_factory=factory,
    )

    assert await bridge.detach("session-key", attachment.routing_key) is True
    assert await bridge.detach("session-key", attachment.routing_key) is False
    assert attachment.client.closed == 1
    assert states["session-key"].live_runtime_attachments == {}
    assert bridge._locks == {}


@pytest.mark.asyncio
async def test_detach_lock_cleanup_is_guarded_against_waiting_reattach():
    bridge, _states = _bridge()
    close_entered = asyncio.Event()
    release_close = asyncio.Event()
    creations = 0

    class BlockingCloseClient(FakeClient):
        async def close(self):
            self.closed += 1
            close_entered.set()
            await release_close.wait()

    async def factory(request):
        nonlocal creations
        creations += 1
        if creations == 1:
            return BlockingCloseClient(request)
        return FakeClient(request)

    first = await bridge.attach(
        session_key="session-key",
        source=_source(),
        principal_id="principal-1",
        durable_root="root-1",
        durable_session_id="session-1",
        profile_home="/profiles/default",
        mode=AttachmentMode.CONTROL,
        client_factory=factory,
    )
    lock_key = ("session-key", first.routing_key.token)
    original_lock_entry = bridge._locks[lock_key]

    detach_task = asyncio.create_task(
        bridge.detach("session-key", first.routing_key, expected=first)
    )
    await close_entered.wait()
    attach_task = asyncio.create_task(
        bridge.attach(
            session_key="session-key",
            source=_source(),
            principal_id="principal-1",
            durable_root="root-2",
            durable_session_id="session-2",
            profile_home="/profiles/default",
            mode=AttachmentMode.CONTROL,
            client_factory=factory,
        )
    )
    await asyncio.sleep(0)
    assert bridge._locks[lock_key] is original_lock_entry

    release_close.set()
    assert await detach_task is True
    replacement = await attach_task
    assert bridge._locks[lock_key] is original_lock_entry

    assert await bridge.detach(
        "session-key", replacement.routing_key, expected=replacement
    )
    assert bridge._locks == {}


@pytest.mark.asyncio
async def test_root_or_mode_change_replaces_old_client_with_cas_safe_cleanup():
    bridge, states = _bridge()

    async def factory(request):
        return FakeClient(request)

    first = await bridge.attach(
        session_key="session-key",
        source=_source(),
        principal_id="principal-1",
        durable_root="root-1",
        durable_session_id="session-1",
        profile_home="/profiles/default",
        mode=AttachmentMode.CONTROL,
        client_factory=factory,
    )
    replacement = await bridge.attach(
        session_key="session-key",
        source=_source(),
        principal_id="principal-1",
        durable_root="root-2",
        durable_session_id="session-2",
        profile_home="/profiles/default",
        mode=AttachmentMode.OBSERVE,
        client_factory=factory,
    )

    assert replacement is not first
    assert first.client.closed == 1
    assert await bridge.detach(
        "session-key", first.routing_key, expected=first
    ) is False
    assert (
        states["session-key"].live_runtime_attachments[first.routing_key.token]
        is replacement
    )


@pytest.mark.asyncio
async def test_gateway_runner_bridge_does_not_construct_local_agent():
    runner = object.__new__(GatewayRunner)
    bridge = runner._live_runtime_attachment_bridge()
    created = []

    async def factory(request):
        created.append(request)
        return FakeClient(request)

    attachment = await bridge.attach(
        session_key="runner-session",
        source=_source(),
        principal_id="principal-1",
        durable_root="root-1",
        durable_session_id="session-1",
        profile_home="/profiles/default",
        mode=AttachmentMode.CONTROL,
        client_factory=factory,
    )

    assert attachment.owner_generation == 4
    assert len(created) == 1
    assert runner._running_agent_items() == []
    assert runner._live_runtime_attachment_bridge() is bridge


@pytest.mark.asyncio
async def test_gateway_runner_shutdown_hook_closes_existing_attachments():
    runner = object.__new__(GatewayRunner)
    bridge = runner._live_runtime_attachment_bridge()

    async def factory(request):
        return FakeClient(request)

    attachment = await bridge.attach(
        session_key="runner-session",
        source=_source(),
        principal_id="principal-1",
        durable_root="root-1",
        durable_session_id="session-1",
        profile_home="/profiles/default",
        mode=AttachmentMode.CONTROL,
        client_factory=factory,
    )

    await runner._close_live_runtime_attachments()

    assert attachment.client.closed == 1
    assert (
        runner._session_state("runner-session").live_runtime_attachments
        == {}
    )
    assert bridge._locks == {}


def test_routing_key_ignores_delivery_only_message_metadata():
    first = _source()
    first.message_id = "message-1"
    second = _source()
    second.message_id = "message-2"
    second.user_name = "new display name"

    assert LiveRuntimeRoutingKey.from_source(first, "principal-1") == (
        LiveRuntimeRoutingKey.from_source(second, "principal-1")
    )


def test_owner_lookup_key_is_byte_compatible_with_existing_runtime_proxy():
    from tui_gateway.runtime_proxy import _profile_scoped_conversation_key

    assert profile_scoped_runtime_key("durable-root", "/profiles/default") == (
        _profile_scoped_conversation_key("durable-root", "/profiles/default")
    )
