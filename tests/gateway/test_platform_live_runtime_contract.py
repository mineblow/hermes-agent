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
    (
        "discord",
        "plugins.platforms.discord.adapter",
        "DiscordAdapter",
        Platform.DISCORD,
    ),
    (
        "telegram",
        "plugins.platforms.telegram.adapter",
        "TelegramAdapter",
        Platform.TELEGRAM,
    ),
    ("slack", "plugins.platforms.slack.adapter", "SlackAdapter", Platform.SLACK),
    (
        None,
        "gateway.platforms.api_server",
        "APIServerAdapter",
        Platform.API_SERVER,
    ),
    ("email", "plugins.platforms.email.adapter", "EmailAdapter", Platform.EMAIL),
    ("ntfy", "plugins.platforms.ntfy.adapter", "NtfyAdapter", Platform("ntfy")),
]


def test_base_adapter_declares_runner_owned_live_runtime_presentation():
    assert BasePlatformAdapter.LIVE_RUNTIME_PRESENTATION_VIA_RUNNER is True
    assert BasePlatformAdapter.STARTS_LIVE_RUNTIME is False


def _build_production_adapter(registry_name, module_name, class_name):
    """Construct the adapter shipped by its real registry factory when possible."""
    from gateway.config import PlatformConfig
    from hermes_cli.plugins import discover_plugins

    if registry_name is None:
        adapter_class = getattr(importlib.import_module(module_name), class_name)
        adapter = adapter_class(PlatformConfig())
        return adapter, None

    discover_plugins()
    entry = platform_registry.get(registry_name)
    assert entry is not None, f"{registry_name} is missing from the platform registry"
    adapter = entry.adapter_factory(PlatformConfig())
    assert type(adapter).__name__ == class_name
    return adapter, entry


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
    "registry_name,module_name,class_name,platform",
    REPRESENTATIVE_ADAPTERS,
)
async def test_production_adapter_presents_live_runtime_events_through_runner(
    monkeypatch, registry_name, module_name, class_name, platform
):
    adapter, entry = _build_production_adapter(registry_name, module_name, class_name)
    assert isinstance(adapter, BasePlatformAdapter)
    assert adapter.platform == platform
    assert adapter.LIVE_RUNTIME_PRESENTATION_VIA_RUNNER is True
    assert adapter.STARTS_LIVE_RUNTIME is False
    if entry is not None:
        assert entry.live_runtime_presentation_via_runner is True
        assert entry.starts_live_runtime is False

    send = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="platform-message-1")
    )
    edit_message = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="platform-message-1")
    )
    monkeypatch.setattr(adapter, "send", send)
    monkeypatch.setattr(adapter, "edit_message", edit_message)

    runner = object.__new__(GatewayRunner)
    runner.config = SimpleNamespace(streaming=StreamingConfig(enabled=False))
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

    try:
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
        await renderer.on_event(event(2, "message.delta", {"text": "assistant reply"}))
        await renderer.on_event(
            event(
                3,
                "message.complete",
                {"text": "assistant reply", "status": "complete"},
            )
        )
    finally:
        await renderer.close()

    assert send.await_count == 2
    calls = send.await_args_list
    assert calls[0].args[:2] == ("chat-1", "Alice: peer prompt")
    assert all(call.kwargs["metadata"]["thread_id"] == "thread-1" for call in calls)
    if platform == Platform.SLACK:
        assert all(
            call.kwargs["metadata"]["slack_team_id"] == "workspace-1"
            and call.kwargs["metadata"]["user_id"] == "user-1"
            for call in calls
        )
