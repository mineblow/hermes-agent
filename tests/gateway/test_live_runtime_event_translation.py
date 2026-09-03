from __future__ import annotations

import asyncio

import pytest

from gateway.live_runtime_event_translation import translate_runtime_event
from gateway.stream_consumer import LiveRuntimeStreamRenderer
from gateway.stream_events import (
    Commentary,
    InteractionRequest,
    MessageChunk,
    MessageStart,
    MessageStop,
    PeerUserMessage,
    ToolCallChunk,
    ToolCallFinished,
)


def _event(event_type: str, payload: dict, *, seq: int = 1) -> dict:
    return {
        "kind": "runtime.event",
        "protocol": 1,
        "runtime_id": "live-session-1",
        "durable_session_id": "stored-session-1",
        "replay_epoch": "epoch-1",
        "seq": seq,
        "type": event_type,
        "payload": payload,
    }


def test_maps_assistant_turn_start():
    assert translate_runtime_event(_event("message.start", {})) == MessageStart()


def test_maps_assistant_text_and_authoritative_completion():
    assert translate_runtime_event(
        _event("message.delta", {"text": "hello "})
    ) == MessageChunk("hello ")
    assert translate_runtime_event(
        _event("message.complete", {"text": "hello world", "status": "complete"}, seq=2)
    ) == MessageStop(final=True, text="hello world", status="complete")


def test_translates_interim_without_redelivering_already_streamed_text():
    assert translate_runtime_event(
        _event(
            "message.interim",
            {"text": "I will inspect it.", "already_streamed": False},
        )
    ) == Commentary("I will inspect it.")
    assert translate_runtime_event(
        _event(
            "message.interim",
            {"text": "already visible", "already_streamed": True},
        )
    ) == MessageStop(final=False)


def test_translates_tool_lifecycle_without_tool_output_delivery():
    assert translate_runtime_event(
        _event(
            "tool.start",
            {
                "tool_id": "tool-7",
                "name": "terminal",
                "context": "pytest -q",
                "args": {"command": "pytest -q"},
                "index": 7,
            },
        )
    ) == ToolCallChunk(
        invocation_id="tool-7",
        tool_name="terminal",
        preview="pytest -q",
        args={"command": "pytest -q"},
        index=7,
    )
    assert translate_runtime_event(
        _event(
            "tool.complete",
            {
                "tool_id": "tool-7",
                "name": "terminal",
                "duration_s": 1.25,
                "ok": False,
                "result": "must not be delivered",
                "index": 7,
            },
            seq=2,
        )
    ) == ToolCallFinished(
        invocation_id="tool-7",
        tool_name="terminal",
        duration=1.25,
        ok=False,
        index=7,
    )


def test_translates_peer_user_row_as_presentation_only_event():
    assert translate_runtime_event(
        _event(
            "message.user",
            {
                "message_id": "desktop-message-1",
                "text": "peer prompt",
                "timestamp": 123.5,
                "attachment_refs": ["attachment://one"],
                "display_metadata": {"user_name": "Ethan"},
            },
        )
    ) == PeerUserMessage(
        message_id="desktop-message-1",
        text="peer prompt",
        timestamp=123.5,
        attachment_refs=("attachment://one",),
        display_metadata={"user_name": "Ethan"},
    )


def test_translates_interactions_without_surface_specific_capabilities():
    approval = translate_runtime_event(
        _event(
            "approval.request",
            {"request_id": "approval-1", "tool": "terminal", "choices": ["once", "deny"]},
        )
    )
    clarify = translate_runtime_event(
        _event(
            "clarify.request",
            {"request_id": "clarify-1", "question": "Continue?", "choices": ["yes", "no"]},
            seq=2,
        )
    )

    assert approval == InteractionRequest(
        request_id="approval-1",
        interaction_type="approval",
        payload={"tool": "terminal", "choices": ["once", "deny"]},
    )
    assert clarify == InteractionRequest(
        request_id="clarify-1",
        interaction_type="clarification",
        payload={"question": "Continue?", "choices": ["yes", "no"]},
    )


def test_safely_collapses_unsupported_presentation_event():
    assert translate_runtime_event(
        _event("reasoning.delta", {"text": "private chain of thought"})
    ) is None


class _Consumer:
    def __init__(self):
        self.deltas = []
        self.commentary = []
        self.segment_breaks = 0
        self.final_text = None
        self.finished = asyncio.Event()
        self.cancelled = False

    async def run(self):
        try:
            await self.finished.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def on_delta(self, text):
        self.deltas.append(text)

    def on_commentary(self, text):
        self.commentary.append(text)

    def on_segment_break(self):
        self.segment_breaks += 1

    def finish(self, final_text=None):
        self.final_text = final_text
        self.finished.set()


def _base_adapter():
    from gateway.platforms.base import BasePlatformAdapter

    concrete = type("Concrete", (BasePlatformAdapter,), {})
    concrete.__abstractmethods__ = frozenset()
    return concrete.__new__(concrete)


@pytest.mark.asyncio
async def test_renderer_owns_one_consumer_per_canonical_turn():
    consumers = []

    def factory():
        consumer = _Consumer()
        consumers.append(consumer)
        return consumer

    renderer = LiveRuntimeStreamRenderer(_base_adapter(), factory)

    await renderer.on_event(_event("message.delta", {"text": "hello"}))
    await renderer.on_event(
        _event(
            "message.complete",
            {"text": "hello world", "status": "complete"},
            seq=2,
        )
    )
    await renderer.on_event(
        _event(
            "message.interim",
            {"text": "second turn", "already_streamed": False},
            seq=3,
        )
    )
    await renderer.on_event(
        _event(
            "message.complete",
            {"text": "second final", "status": "complete"},
            seq=4,
        )
    )

    assert len(consumers) == 2
    assert consumers[0].deltas == ["hello"]
    assert consumers[0].final_text == "hello world"
    assert consumers[1].commentary == ["second turn"]
    assert consumers[1].final_text == "second final"


@pytest.mark.asyncio
async def test_message_start_fences_partial_previous_turn():
    consumers = []

    def factory():
        consumer = _Consumer()
        consumers.append(consumer)
        return consumer

    renderer = LiveRuntimeStreamRenderer(_base_adapter(), factory)

    await renderer.on_event(_event("message.start", {}))
    await asyncio.sleep(0)
    await renderer.on_event(_event("message.delta", {"text": "partial"}, seq=2))
    await renderer.on_event(_event("message.start", {}, seq=3))

    assert len(consumers) == 2
    assert consumers[0].deltas == ["partial"]
    assert consumers[0].cancelled is True

    await renderer.close()


@pytest.mark.asyncio
async def test_authoritative_resync_replaces_partial_turn_with_one_final():
    consumers = []

    def factory():
        consumer = _Consumer()
        consumers.append(consumer)
        return consumer

    renderer = LiveRuntimeStreamRenderer(_base_adapter(), factory)
    await renderer.on_event(_event("message.start", {}))
    await asyncio.sleep(0)
    await renderer.on_event(_event("message.delta", {"text": "stale"}, seq=2))

    await renderer.reset({
        "snapshot": {
            "latest_assistant": {"text": "authoritative final", "row_id": 7}
        }
    })

    assert len(consumers) == 2
    assert consumers[0].deltas == ["stale"]
    assert consumers[0].cancelled is True
    assert consumers[1].final_text == "authoritative final"


@pytest.mark.asyncio
async def test_renderer_suppresses_local_user_echo_and_routes_peer_and_interactions():
    peer_events = []
    interactions = []
    renderer = LiveRuntimeStreamRenderer(
        _base_adapter(),
        _Consumer,
        on_peer_user=peer_events.append,
        on_interaction=interactions.append,
    )
    renderer.register_local_message_id("local-1")

    await renderer.on_event(
        _event(
            "message.user",
            {"message_id": "local-1", "text": "mine", "timestamp": 1.0},
        )
    )
    await renderer.on_event(
        _event(
            "message.user",
            {"message_id": "peer-1", "text": "theirs", "timestamp": 2.0},
            seq=2,
        )
    )
    await renderer.on_event(
        _event(
            "approval.request",
            {"request_id": "approval-1", "tool": "terminal"},
            seq=3,
        )
    )

    assert [event.message_id for event in peer_events] == ["peer-1"]
    assert [event.request_id for event in interactions] == ["approval-1"]
    await renderer.close()
