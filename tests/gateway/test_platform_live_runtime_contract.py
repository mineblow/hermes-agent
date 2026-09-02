"""Reusable contracts for adapters attached to a canonical live runtime."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, StreamingConfig
from gateway.platform_registry import (
    PlatformEntry,
    PlatformRegistry,
    platform_registry,
)
from gateway.platforms.base import BasePlatformAdapter, SessionSource
from gateway.run import GatewayRunner
from gateway.stream_consumer import StreamConsumerConfig
from hermes_cli.live_runtime_protocol import runtime_event


REPRESENTATIVE_ADAPTERS = [
    ("plugins.platforms.discord.adapter", "DiscordAdapter"),
    ("plugins.platforms.telegram.adapter", "TelegramAdapter"),
    ("plugins.platforms.slack.adapter", "SlackAdapter"),
    ("gateway.platforms.api_server", "APIServerAdapter"),
    ("plugins.platforms.email.adapter", "EmailAdapter"),
    ("plugins.platforms.ntfy.adapter", "NtfyAdapter"),
]


def test_base_adapter_declares_runner_owned_live_runtime_presentation():
    assert BasePlatformAdapter.LIVE_RUNTIME_PRESENTATION_VIA_RUNNER is True
    assert BasePlatformAdapter.STARTS_LIVE_RUNTIME is False


@pytest.mark.parametrize("module_name,class_name", REPRESENTATIVE_ADAPTERS)
def test_representative_adapters_inherit_runner_owned_live_runtime_contract(
    module_name, class_name
):
    module = importlib.import_module(module_name)
    adapter_class = getattr(module, class_name)

    assert issubclass(adapter_class, BasePlatformAdapter)
    assert adapter_class.LIVE_RUNTIME_PRESENTATION_VIA_RUNNER is True
    assert adapter_class.STARTS_LIVE_RUNTIME is False
    assert "LIVE_RUNTIME_PRESENTATION_VIA_RUNNER" not in adapter_class.__dict__
    assert "STARTS_LIVE_RUNTIME" not in adapter_class.__dict__


def test_registry_entries_default_to_runner_owned_live_runtime_presentation():
    registry = PlatformRegistry()
    registry.register(
        PlatformEntry(
            name="minimal-plugin",
            label="Minimal Plugin",
            adapter_factory=lambda _config: object(),
            check_fn=lambda: True,
        ),
        scope=None,
    )

    entry = registry.get("minimal-plugin")
    assert entry is not None
    assert entry.live_runtime_presentation_via_runner is True
    assert entry.starts_live_runtime is False


def test_all_registered_platforms_share_runner_owned_live_runtime_contract():
    from hermes_cli.plugins import discover_plugins

    discover_plugins()
    entries = platform_registry.all_entries()
    assert entries, "bundled platform registry unexpectedly empty"
    assert all(entry.live_runtime_presentation_via_runner for entry in entries)
    assert not any(entry.starts_live_runtime for entry in entries)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "platform",
    [
        Platform.DISCORD,
        Platform.TELEGRAM,
        Platform.SLACK,
        Platform.API_SERVER,
        Platform.EMAIL,
        Platform("ntfy"),
    ],
)
async def test_common_renderer_preserves_routing_for_peer_and_assistant_delivery(
    monkeypatch, platform
):
    send = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="platform-message-1")
    )
    edit_message = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="platform-message-1")
    )
    adapter = SimpleNamespace(
        send=send,
        edit_message=edit_message,
        MAX_MESSAGE_LENGTH=4096,
        REQUIRES_EDIT_FINALIZE=False,
        supports_native_streaming=False,
        supports_draft_streaming=False,
        supports_code_blocks=False,
        splits_long_messages=False,
    )
    adapter.render_message_event = (
        lambda event, sink: BasePlatformAdapter.render_message_event(adapter, event, sink)
    )

    runner = object.__new__(GatewayRunner)
    runner.config = SimpleNamespace(streaming=StreamingConfig(enabled=True))
    runner._adapter_for_source = lambda _source: adapter
    runner._build_stream_consumer_config = lambda *_args, **_kwargs: (
        StreamConsumerConfig(edit_interval=0.001, buffer_threshold=1),
        None,
    )
    monkeypatch.setattr("gateway.run._load_gateway_config", lambda: {})
    source = SessionSource(
        platform=platform,
        scope_id="workspace-1",
        chat_id="chat-1",
        chat_type="group",
        thread_id="thread-1",
        user_id="user-1",
        user_name="Alice",
        profile="worker",
    )
    renderer = runner._make_live_runtime_renderer(source)

    def event(seq, event_type, payload):
        return runtime_event(
            runtime_id="runtime-1",
            durable_session_id="session-1",
            replay_epoch="epoch-1",
            seq=seq,
            event_type=event_type,
            payload=payload,
        )

    await renderer.on_event(
        event(
            1,
            "message.user",
            {
                "message_id": "peer-message-1",
                "text": "peer prompt",
                "timestamp": 1.0,
                "display_metadata": {"user_name": "Alice"},
            },
        )
    )
    await renderer.on_event(
        event(2, "message.delta", {"text": "assistant reply"})
    )
    await renderer.on_event(
        event(
            3,
            "message.complete",
            {"text": "assistant reply", "status": "complete"},
        )
    )

    assert adapter.send.await_count >= 2
    calls = adapter.send.await_args_list
    assert calls[0].args[:2] == ("chat-1", "Alice: peer prompt")
    assert all(call.kwargs["metadata"]["thread_id"] == "thread-1" for call in calls)
    if platform == Platform.SLACK:
        assert all(
            call.kwargs["metadata"]["slack_team_id"] == "workspace-1"
            and call.kwargs["metadata"]["user_id"] == "user-1"
            for call in calls
        )
