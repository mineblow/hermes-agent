from __future__ import annotations

import asyncio
import os
import threading
import time

import pytest

from gateway.live_runtime_bridge import (
    AttachmentMode,
    LiveRuntimeBridge,
    profile_scoped_runtime_key,
)
from gateway.live_runtime_client import AsyncLiveRuntimeClient
from gateway.platforms.base import MessageEvent, Platform
from gateway.runtime_proxy_connection import RuntimeProxyAsyncConnection
from gateway.session import SessionSource
from gateway.session_state import SessionState
from hermes_cli.classic_live_runtime import build_classic_live_runtime_frontend
from hermes_cli.live_runtime_owners import RuntimeOwner
from tests.test_tui_gateway_server import _LifecycleTransport, _dispatch_sync
from tui_gateway import server
from tui_gateway.runtime_proxy import RuntimeProxyServer


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(os.name != "posix", reason="Unix runtime proxy is unavailable"),
]


async def _wait_for_event(events: list[dict], event_type: str, *, after: int = 0) -> dict:
    async with asyncio.timeout(5):
        while True:
            matches = [event for event in events if event["type"] == event_type]
            if len(matches) > after:
                return matches[after]
            await asyncio.sleep(0.01)


async def test_tui_classic_and_discord_share_one_live_runtime(monkeypatch, tmp_path):
    """TUI, classic CLI, and Discord observe and control one canonical run."""
    endpoint = tmp_path / "runtime.sock"
    profile_home = tmp_path / "profiles" / "worker"
    profile_home.mkdir(parents=True)
    durable_root = "desktop-discord-root"
    conversation_key = profile_scoped_runtime_key(durable_root, profile_home)
    desktop = _LifecycleTransport("desktop-controller")
    discord_events: list[dict] = []
    classic_rows: list[dict] = []
    run_prompts: list[str] = []

    class Agent:
        model = "test-model"
        provider = "test-provider"
        base_url = "https://example.invalid"
        api_key = object()
        api_mode = "test"

        def run_conversation(
            self, prompt, conversation_history=None, stream_callback=None, **_kwargs
        ):
            run_prompts.append(prompt)
            self._on_user_message_persisted()
            stream_callback(f"reply-{len(run_prompts)}")
            return {
                "final_response": f"reply-{len(run_prompts)}",
                "messages": [
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": f"reply-{len(run_prompts)}"},
                ],
            }

    class EmptyDB:
        def get_session(self, _target):
            return None

        def get_session_by_title(self, _target):
            return None

    previous_sessions = dict(server._sessions)
    server._sessions.clear()
    server._live_transports.clear()
    monkeypatch.setattr(server, "_completion_cwd", lambda params=None: str(tmp_path))
    monkeypatch.setattr(server, "_schedule_session_cap_enforcement", lambda: None)
    monkeypatch.setattr(server, "_start_agent_build", lambda _sid, _session: None)
    monkeypatch.setattr(server, "make_stream_renderer", lambda _cols: None)
    monkeypatch.setattr(server, "render_message", lambda _raw, _cols: None)
    monkeypatch.setattr(server, "_get_db", lambda: EmptyDB())

    _dispatch_sync(
        {
            "jsonrpc": "2.0",
            "id": "desktop-client",
            "method": "client.attach",
            "params": {"client_id": desktop.client_id},
        },
        desktop,
    )
    created = _dispatch_sync(
        {
            "jsonrpc": "2.0",
            "id": "create",
            "method": "session.create",
            "params": {"session_key": durable_root},
        },
        desktop,
    )
    assert created is not None and "result" in created
    sid = created["result"]["session_id"]
    stored_sid = created["result"]["stored_session_id"]
    session = server._sessions[sid]
    session["agent"] = Agent()
    session["agent_ready"].set()

    owner = RuntimeOwner(
        conversation_key=conversation_key,
        owner_id="desktop-owner",
        generation=1,
        pid=os.getpid(),
        process_start_time=1.0,
        endpoint=str(endpoint),
        profile_home=str(profile_home),
        surface="tui_gateway",
        started_at=time.time(),
    )
    proxy = RuntimeProxyServer(
        endpoint=endpoint,
        owner_lookup=lambda key: owner if key == conversation_key else None,
        dispatch=server.dispatch,
    )
    assert proxy.start() is True

    states: dict[str, SessionState] = {}
    bridge = LiveRuntimeBridge(lambda key: states.setdefault(key, SessionState()))
    source = SessionSource(
        platform=Platform.DISCORD,
        scope_id="guild-1",
        chat_id="channel-1",
        user_id="discord-user-1",
        user_name="Discord Controller",
        profile="worker",
    )

    async def owner_lookup(key):
        return owner if key == conversation_key else None

    connection_errors: list[str] = []

    class RecordingConnection(RuntimeProxyAsyncConnection):
        async def send(self, frame):
            try:
                await super().send(frame)
            except BaseException as exc:
                connection_errors.append(repr(exc))
                raise

    def client_factory(request):
        return AsyncLiveRuntimeClient(
            conversation_key=conversation_key,
            durable_root=request.durable_root,
            client_id=request.stable_client_id,
            principal={
                "provider": "discord",
                "subject": request.principal_id,
                "authenticated": True,
            },
            surface=request.surface,
            requested_capabilities=request.requested_capabilities,
            owner_lookup=owner_lookup,
            connector=lambda current_owner: RecordingConnection(
                owner=current_owner,
                durable_session_id=request.durable_session_id,
                profile="worker",
                client_id=request.stable_client_id,
                requested_capabilities=request.requested_capabilities,
            ),
            on_event=lambda event: discord_events.append(event),
            reconnect_delay=0.01,
        )

    try:
        classic = build_classic_live_runtime_frontend(
            durable_session_id=stored_sid,
            conversation_key=durable_root,
            profile="worker",
            profile_home=profile_home,
            client_id="classic-cli:e2e",
            on_row=classic_rows.append,
            owner_lookup=lambda key, registry_home=None: (
                owner if key == conversation_key else None
            ),
        )
        await classic.start()
        try:
            attachment = await asyncio.wait_for(
                bridge.attach(
                    session_key="discord:guild-1:channel-1",
                    source=source,
                    principal_id="discord-user-1",
                    durable_root=durable_root,
                    durable_session_id=stored_sid,
                    profile_home=profile_home,
                    mode=AttachmentMode.CONTROL,
                    client_factory=client_factory,
                ),
                timeout=10,
            )
        except TimeoutError:
            pytest.fail(f"Discord runtime attachment failed: {connection_errors[:3]}")

        desktop_submit = _dispatch_sync(
            {
                "jsonrpc": "2.0",
                "id": "desktop-submit",
                "method": "prompt.submit",
                "params": {
                    "session_id": sid,
                    "text": "desktop prompt",
                    "display_text": "desktop prompt",
                    "client_message_id": "desktop-message-1",
                    "submitted_at": 1000.0,
                },
            },
            desktop,
        )
        assert desktop_submit is not None
        assert desktop_submit["result"]["status"] == "streaming"
        await _wait_for_event(discord_events, "message.complete")
        async with asyncio.timeout(5):
            while session["running"]:
                await asyncio.sleep(0.01)
        discord_types = [event["type"] for event in discord_events]
        assert discord_types.index("message.user") < discord_types.index("message.delta")
        assert discord_types.index("message.delta") < discord_types.index("message.complete")
        with desktop._condition:
            desktop.frames.clear()

        discord_message = MessageEvent(
            text="discord prompt",
            user_id="discord-user-1",
            user_name="Discord Controller",
            source=source,
            message_id="discord-delivery-1",
        )
        first = await bridge.submit_message(attachment, discord_message)
        assert first["status"] == "streaming"
        await asyncio.to_thread(desktop.wait_for_event, "message.complete")
        async with asyncio.timeout(5):
            while len(run_prompts) < 2:
                await asyncio.sleep(0.01)
        assert run_prompts == ["desktop prompt", "discord prompt"]

        with desktop._condition:
            desktop.frames.clear()
        classic_submit = await classic.submit(
            "classic prompt",
            submitted_at=time.time(),
            message_id="classic-message-1",
        )
        assert classic_submit["status"] == "streaming"
        await asyncio.to_thread(desktop.wait_for_event, "message.complete")
        async with asyncio.timeout(5):
            while len(run_prompts) < 3:
                await asyncio.sleep(0.01)
        assert run_prompts == [
            "desktop prompt",
            "discord prompt",
            "classic prompt",
        ]
        async with asyncio.timeout(5):
            while not any(
                row.get("kind") == "assistant" and row.get("text") == "reply-3"
                for row in classic_rows
            ):
                await asyncio.sleep(0.01)
        assert any(
            row.get("kind") == "assistant" and row.get("text") == "reply-3"
            for row in classic_rows
        )

        duplicate = await bridge.submit_message(attachment, discord_message)
        assert duplicate["duplicate"] is True
        assert run_prompts == [
            "desktop prompt",
            "discord prompt",
            "classic prompt",
        ]

        watermark = attachment.client.replay_watermark
        assert watermark is not None
        await bridge.detach(
            "discord:guild-1:channel-1", attachment.routing_key, expected=attachment
        )
        discord_events.clear()
        reattached = await bridge.attach(
            session_key="discord:guild-1:channel-1",
            source=source,
            principal_id="discord-user-1",
            durable_root=durable_root,
            durable_session_id=stored_sid,
            profile_home=profile_home,
            mode=AttachmentMode.CONTROL,
            client_factory=client_factory,
        )
        assert reattached.client.replay_watermark is not None
        replay_seqs = [event["seq"] for event in discord_events]
        assert len(replay_seqs) == len(set(replay_seqs))

        clarification_result: list[str] = []
        worker = threading.Thread(
            target=lambda: clarification_result.append(
                server._block(
                    "clarify.request",
                    sid,
                    {"question": "Deploy?", "choices": ["yes", "no"]},
                    timeout=5,
                )
            ),
            daemon=True,
        )
        worker.start()
        clarification = await _wait_for_event(discord_events, "clarify.request")
        request_id = clarification["payload"]["request_id"]
        desktop_answer = _dispatch_sync(
            {
                "jsonrpc": "2.0",
                "id": "desktop-clarification",
                "method": "clarify.respond",
                "params": {"request_id": request_id, "answer": "yes"},
            },
            desktop,
        )
        assert desktop_answer is not None
        assert desktop_answer["result"]["status"] == "ok"
        worker.join(timeout=5)
        assert clarification_result == ["yes"]
        losing_answer = await reattached.client.respond_to_interaction(
            {
                "interaction_type": "clarification",
                "request_id": request_id,
                "answer": "no",
            }
        )
        assert losing_answer == {"status": "already_resolved"}

        from tools import approval

        approval_resolutions: list[tuple[str, str, dict]] = []
        monkeypatch.setattr(
            approval,
            "resolve_gateway_approval",
            lambda key, choice, **kwargs: approval_resolutions.append(
                (key, choice, kwargs)
            )
            or 1,
        )
        server._emit(
            "approval.request",
            sid,
            {"request_id": "approval-cross-surface-1", "command": "deploy"},
        )
        await _wait_for_event(discord_events, "approval.request")
        desktop_approval = _dispatch_sync(
            {
                "jsonrpc": "2.0",
                "id": "desktop-approval",
                "method": "approval.respond",
                "params": {
                    "session_id": sid,
                    "request_id": "approval-cross-surface-1",
                    "choice": "once",
                },
            },
            desktop,
        )
        assert desktop_approval is not None
        assert desktop_approval["result"]["status"] == "resolved"
        losing_approval = await reattached.client.respond_to_interaction(
            {
                "interaction_type": "approval",
                "request_id": "approval-cross-surface-1",
                "choice": "once",
            }
        )
        assert losing_approval == {
            "status": "already_resolved",
            "resolved": 0,
            "request_id": "approval-cross-surface-1",
            "choice": "once",
        }
        assert approval_resolutions == [
            (
                stored_sid,
                "once",
                {
                    "resolve_all": False,
                    "request_id": "approval-cross-surface-1",
                },
            )
        ]
    finally:
        if "classic" in locals():
            await classic.close()
        await bridge.close_all()
        proxy.stop()
        for current_sid, current_session in list(server._sessions.items()):
            server._sessions.pop(current_sid, None)
            server._teardown_session(current_session)
        server._sessions.update(previous_sessions)
        server._live_transports.clear()


async def test_unauthorized_discord_user_cannot_reach_live_runtime():
    from unittest.mock import AsyncMock

    from tests.e2e.conftest import make_runner

    runner = make_runner(Platform.DISCORD)
    runner._scale_to_zero_note_real_inbound = lambda: None
    runner._is_user_authorized_for_source = lambda _source: False
    forbidden_bridge = AsyncMock()
    forbidden_bridge.attach.side_effect = AssertionError(
        "unauthorized Discord user reached canonical runtime attachment"
    )
    runner.__dict__["_live_runtime_bridge_instance"] = forbidden_bridge
    source = SessionSource(
        platform=Platform.DISCORD,
        scope_id="guild-1",
        chat_id="shared-channel",
        chat_type="group",
        user_id="unauthorized-user",
        user_name="Unauthorized User",
        profile="worker",
    )

    result = await runner._handle_message(
        MessageEvent(
            text="attach to the live session",
            source=source,
            user_id=source.user_id,
            user_name=source.user_name,
            message_id="unauthorized-delivery-1",
        )
    )

    assert result is None
    forbidden_bridge.attach.assert_not_awaited()
    runner._handle_message_with_agent.assert_not_awaited()
