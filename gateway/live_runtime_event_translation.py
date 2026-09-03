"""Translate canonical runtime output into gateway presentation events.

The canonical owner has already persisted every durable event. Objects returned
here are presentation-only and must never be written back to conversation
history by gateway renderers.
"""

from __future__ import annotations

from typing import Any

from gateway.stream_events import (
    Commentary,
    InteractionRequest,
    MessageChunk,
    MessageStart,
    MessageStop,
    PeerUserMessage,
    StreamEvent,
    ToolCallChunk,
    ToolCallFinished,
)
from hermes_cli.live_runtime_protocol import (
    LiveRuntimeProtocolError,
    validate_runtime_event,
)


def _text(payload: dict[str, Any], key: str, *, allow_empty: bool = False) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise LiveRuntimeProtocolError(f"invalid {key}")
    return value


def _index(payload: dict[str, Any]) -> int:
    value = payload.get("index", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LiveRuntimeProtocolError("invalid tool index")
    return value


def translate_runtime_event(frame: dict[str, Any]) -> StreamEvent | None:
    """Return one transport-neutral presentation event for a runtime frame.

    Unsupported presentation-only frames collapse to ``None``. Malformed known
    event families fail closed rather than reaching platform adapters.
    """

    event = validate_runtime_event(frame)
    event_type = event["type"]
    payload = event["payload"]

    if event_type == "message.start":
        return MessageStart()
    if event_type == "message.delta":
        return MessageChunk(_text(payload, "text"))
    if event_type == "message.complete":
        status = payload.get("status")
        if status is not None and not isinstance(status, str):
            raise LiveRuntimeProtocolError("invalid completion status")
        return MessageStop(
            final=True,
            text=_text(payload, "text", allow_empty=True),
            status=status,
        )
    if event_type == "message.interim":
        if payload.get("already_streamed") is True:
            return MessageStop(final=False)
        return Commentary(_text(payload, "text"))
    if event_type == "tool.start":
        args = payload.get("args")
        if args is not None and not isinstance(args, dict):
            raise LiveRuntimeProtocolError("invalid tool args")
        preview = payload.get("context")
        if preview is not None and not isinstance(preview, str):
            raise LiveRuntimeProtocolError("invalid tool preview")
        return ToolCallChunk(
            tool_name=_text(payload, "name"),
            invocation_id=_text(payload, "tool_id"),
            preview=preview,
            args=args,
            index=_index(payload),
        )
    if event_type == "tool.complete":
        duration = payload.get("duration_s", 0.0)
        if isinstance(duration, bool) or not isinstance(duration, (int, float)):
            raise LiveRuntimeProtocolError("invalid tool duration")
        ok = payload.get("ok", True)
        if not isinstance(ok, bool):
            raise LiveRuntimeProtocolError("invalid tool result status")
        return ToolCallFinished(
            tool_name=_text(payload, "name"),
            invocation_id=_text(payload, "tool_id"),
            duration=float(duration),
            ok=ok,
            index=_index(payload),
        )
    if event_type == "message.user":
        timestamp = payload.get("timestamp")
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            raise LiveRuntimeProtocolError("invalid user message timestamp")
        refs = payload.get("attachment_refs", [])
        if not isinstance(refs, list) or any(
            not isinstance(ref, str) or not ref.strip() for ref in refs
        ):
            raise LiveRuntimeProtocolError("invalid user attachment refs")
        metadata = payload.get("display_metadata", {})
        if not isinstance(metadata, dict):
            raise LiveRuntimeProtocolError("invalid user display metadata")
        return PeerUserMessage(
            message_id=_text(payload, "message_id"),
            text=_text(payload, "text", allow_empty=True),
            timestamp=float(timestamp),
            attachment_refs=tuple(refs),
            display_metadata=dict(metadata),
        )
    if event_type in {"approval.request", "clarify.request"}:
        request_id = _text(payload, "request_id")
        interaction_payload = dict(payload)
        interaction_payload.pop("request_id", None)
        return InteractionRequest(
            request_id=request_id,
            interaction_type=(
                "approval" if event_type == "approval.request" else "clarification"
            ),
            payload=interaction_payload,
        )
    return None


__all__ = ["translate_runtime_event"]
