from __future__ import annotations

import os

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
from hermes_cli.live_runtime_owners import RuntimeOwner
from tui_gateway.runtime_proxy import RuntimeProxyServer


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="authenticated Unix runtime proxy is unavailable")
async def test_message_event_reaches_real_authenticated_runtime_proxy_once(tmp_path):
    endpoint = tmp_path / "runtime.sock"
    profile_home = tmp_path / "profiles" / "worker"
    profile_home.mkdir(parents=True)
    durable_root = "root-1"
    conversation_key = profile_scoped_runtime_key(durable_root, profile_home)
    owner = RuntimeOwner(
        conversation_key=conversation_key,
        owner_id="owner-1",
        generation=3,
        pid=os.getpid(),
        process_start_time=1.0,
        endpoint=str(endpoint),
        profile_home=str(profile_home),
        surface="tui_gateway",
        started_at=1.0,
    )
    dispatched: list[dict] = []

    def dispatch(request, transport):
        assert transport.auth_identity["provider"] == "runtime-proxy-peer"
        if request["method"] == "session.resume":
            return {
                "jsonrpc": "2.0",
                "id": request["id"],
                "result": {"session_id": "live-session-1"},
            }
        assert request["method"] == "prompt.submit"
        dispatched.append(request)
        return {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {"status": "streaming"},
        }

    server = RuntimeProxyServer(
        endpoint=endpoint,
        owner_lookup=lambda key: owner if key == conversation_key else None,
        dispatch=dispatch,
    )
    assert server.start() is True
    states: dict[str, SessionState] = {}
    bridge = LiveRuntimeBridge(
        lambda key: states.setdefault(key, SessionState())
    )
    source = SessionSource(
        platform=Platform.DISCORD,
        scope_id="guild-1",
        chat_id="channel-1",
        user_id="user-1",
        user_name="Current Speaker",
        profile="worker",
    )

    async def owner_lookup(key):
        return owner if key == conversation_key else None

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
            connector=lambda current_owner: RuntimeProxyAsyncConnection(
                owner=current_owner,
                durable_session_id=request.durable_session_id,
                profile="worker",
                client_id=request.stable_client_id,
                requested_capabilities=request.requested_capabilities,
            ),
        )

    try:
        attachment = await bridge.attach(
            session_key="discord:guild-1:channel-1",
            source=source,
            principal_id="user-1",
            durable_root=durable_root,
            durable_session_id="stored-session-1",
            profile_home=profile_home,
            mode=AttachmentMode.CONTROL,
            client_factory=client_factory,
        )
        event = MessageEvent(
            text="expanded execution text",
            user_id="user-1",
            user_name="Current Speaker",
            source=source,
            message_id="discord-message-1",
            media_urls=["/tmp/image.png", "/tmp/document.pdf"],
            media_types=["image/png", "application/pdf"],
            metadata={
                "live_runtime_display_text": "display text",
                "live_runtime_image_refs": ["/tmp/image.png"],
            },
        )

        result = await bridge.submit_message(attachment, event)

        assert result == {"status": "streaming"}
        assert len(dispatched) == 1
        assert dispatched[0]["params"]["text"] == "expanded execution text"
        assert dispatched[0]["params"]["display_text"] == "display text"
        assert dispatched[0]["params"]["attachment_refs"] == [
            "/tmp/image.png",
            "/tmp/document.pdf",
        ]
        assert dispatched[0]["params"]["image_paths"] == ["/tmp/image.png"]
    finally:
        await bridge.close_all()
        server.stop()
