"""Transport-neutral protocol primitives for canonical live runtimes.

This module describes frontend identity, user input, ordered runtime events, and
point-to-point control responses. It deliberately has no dependency on TUI,
Desktop, social-platform, rendering, or socket implementations.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from typing import Any

LIVE_RUNTIME_PROTOCOL_VERSION = 1

SUPPORTED_CAPABILITIES = frozenset(
    {
        "observe",
        "prompt.submit",
        "session.steer",
        "session.interrupt",
        "approval.respond",
        "clarify.respond",
        "interaction.respond",
    }
)
LEGACY_PROXY_CAPABILITIES = frozenset(
    (SUPPORTED_CAPABILITIES - {"interaction.respond"}) | {"ui.respond"}
)
SUPPORTED_BUSY_POLICIES = frozenset({"queue", "reject", "interrupt"})


class LiveRuntimeProtocolError(ValueError):
    """A frontend or runtime envelope violates the canonical protocol."""


def _nonempty(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LiveRuntimeProtocolError(f"invalid {label}")
    return value


def _plain_json(value: Any, label: str) -> Any:
    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return json.loads(encoded)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LiveRuntimeProtocolError(f"invalid {label}") from exc


def validate_client_id(client_id: Any) -> str:
    return _nonempty(client_id, "client identity")


def validate_capabilities(
    capabilities: Any,
    *,
    supported: frozenset[str] = SUPPORTED_CAPABILITIES,
) -> frozenset[str]:
    if not isinstance(capabilities, (list, tuple, set, frozenset)):
        raise LiveRuntimeProtocolError("invalid requested capabilities")
    values = list(capabilities)
    if (
        len(values) > len(supported)
        or any(
            not isinstance(capability, str)
            or capability not in supported
            for capability in values
        )
        or len(set(values)) != len(values)
    ):
        raise LiveRuntimeProtocolError("invalid requested capabilities")
    return frozenset(values)


def _validate_principal(principal: Any) -> dict[str, Any]:
    if not isinstance(principal, dict):
        raise LiveRuntimeProtocolError("invalid principal descriptor")
    normalized = _plain_json(principal, "principal descriptor")
    _nonempty(normalized.get("provider"), "principal descriptor")
    _nonempty(normalized.get("subject"), "principal descriptor")
    if normalized.get("authenticated") is not True:
        raise LiveRuntimeProtocolError("invalid principal descriptor")
    return normalized


def _validate_replay(replay: Any) -> dict[str, Any]:
    if not isinstance(replay, dict) or set(replay) != {"epoch", "seq"}:
        raise LiveRuntimeProtocolError("invalid replay watermark")
    epoch = replay.get("epoch")
    seq = replay.get("seq")
    if (
        not isinstance(epoch, str)
        or not epoch.strip()
        or isinstance(seq, bool)
        or not isinstance(seq, int)
        or seq < 0
    ):
        raise LiveRuntimeProtocolError("invalid replay watermark")
    return {"epoch": epoch, "seq": seq}


def frontend_hello(
    *,
    client_id: str,
    principal: Mapping[str, Any],
    surface: str,
    requested_capabilities: Iterable[str],
    durable_root: str,
    replay_epoch: str | None = None,
    replay_seq: int | None = None,
) -> dict[str, Any]:
    capabilities = validate_capabilities(requested_capabilities)
    frame: dict[str, Any] = {
        "kind": "frontend.hello",
        "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
        "client_id": client_id,
        "principal": dict(principal),
        "surface": surface,
        "requested_capabilities": sorted(capabilities),
        "durable_root": durable_root,
    }
    if replay_epoch is not None or replay_seq is not None:
        frame["replay"] = {"epoch": replay_epoch, "seq": replay_seq}
    return validate_frontend_hello(frame)


def validate_frontend_hello(frame: Any) -> dict[str, Any]:
    if (
        not isinstance(frame, dict)
        or frame.get("kind") != "frontend.hello"
        or frame.get("protocol") != LIVE_RUNTIME_PROTOCOL_VERSION
    ):
        raise LiveRuntimeProtocolError("invalid frontend hello")
    validate_client_id(frame.get("client_id"))
    _validate_principal(frame.get("principal"))
    _nonempty(frame.get("surface"), "surface")
    _nonempty(frame.get("durable_root"), "durable root")
    capabilities = validate_capabilities(frame.get("requested_capabilities"))
    raw_capabilities = frame.get("requested_capabilities")
    if not isinstance(raw_capabilities, list) or raw_capabilities != sorted(capabilities):
        raise LiveRuntimeProtocolError("invalid requested capabilities")
    if "replay" in frame:
        _validate_replay(frame["replay"])
    return _plain_json(frame, "frontend hello")


def runtime_input(
    *,
    message_id: str,
    text: str,
    display_text: str | None = None,
    submitted_at: float | None = None,
    attachment_refs: Iterable[str] | None = None,
    display_metadata: Mapping[str, Any] | None = None,
    busy_policy: str = "queue",
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "kind": "runtime.input",
        "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
        "message_id": message_id,
        "text": text,
        "busy_policy": busy_policy,
    }
    if display_text is not None:
        frame["display_text"] = display_text
    if submitted_at is not None:
        frame["submitted_at"] = submitted_at
    if attachment_refs is not None:
        frame["attachment_refs"] = list(attachment_refs)
    if display_metadata is not None:
        frame["display_metadata"] = dict(display_metadata)
    return validate_runtime_input(frame)


def validate_runtime_input(frame: Any) -> dict[str, Any]:
    if (
        not isinstance(frame, dict)
        or frame.get("kind") != "runtime.input"
        or frame.get("protocol") != LIVE_RUNTIME_PROTOCOL_VERSION
    ):
        raise LiveRuntimeProtocolError("invalid runtime input")
    _nonempty(frame.get("message_id"), "message identity")
    if not isinstance(frame.get("text"), str):
        raise LiveRuntimeProtocolError("invalid input text")
    if frame.get("busy_policy") not in SUPPORTED_BUSY_POLICIES:
        raise LiveRuntimeProtocolError("invalid busy policy")
    if "display_text" in frame and not isinstance(frame["display_text"], str):
        raise LiveRuntimeProtocolError("invalid display text")
    if "submitted_at" in frame:
        submitted_at = frame["submitted_at"]
        if (
            isinstance(submitted_at, bool)
            or not isinstance(submitted_at, (int, float))
            or not math.isfinite(float(submitted_at))
        ):
            raise LiveRuntimeProtocolError("invalid submission timestamp")
    refs = frame.get("attachment_refs", [])
    if (
        not isinstance(refs, list)
        or any(not isinstance(ref, str) or not ref.strip() for ref in refs)
        or len(set(refs)) != len(refs)
    ):
        raise LiveRuntimeProtocolError("invalid attachment refs")
    if "display_metadata" in frame and not isinstance(frame["display_metadata"], dict):
        raise LiveRuntimeProtocolError("invalid display metadata")
    return _plain_json(frame, "runtime input")


def runtime_event(
    *,
    runtime_id: str,
    durable_session_id: str,
    replay_epoch: str,
    seq: int,
    event_type: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return validate_runtime_event(
        {
            "kind": "runtime.event",
            "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
            "runtime_id": runtime_id,
            "durable_session_id": durable_session_id,
            "replay_epoch": replay_epoch,
            "seq": seq,
            "type": event_type,
            "payload": dict(payload),
        }
    )


def validate_runtime_event(frame: Any) -> dict[str, Any]:
    if (
        not isinstance(frame, dict)
        or frame.get("kind") != "runtime.event"
        or frame.get("protocol") != LIVE_RUNTIME_PROTOCOL_VERSION
    ):
        raise LiveRuntimeProtocolError("invalid runtime event")
    for field, label in (
        ("runtime_id", "runtime identity"),
        ("durable_session_id", "durable session identity"),
        ("replay_epoch", "replay epoch"),
        ("type", "event type"),
    ):
        _nonempty(frame.get(field), label)
    seq = frame.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
        raise LiveRuntimeProtocolError("invalid event sequence")
    if not isinstance(frame.get("payload"), dict):
        raise LiveRuntimeProtocolError("invalid event payload")
    return _plain_json(frame, "runtime event")


_MISSING = object()


def control_response(
    request_id: str,
    *,
    result: Any = _MISSING,
    error: Mapping[str, Any] | object = _MISSING,
) -> dict[str, Any]:
    if (result is _MISSING) == (error is _MISSING):
        raise ValueError("exactly one of result or error is required")
    _nonempty(request_id, "request identity")
    frame: dict[str, Any] = {
        "kind": "control.response",
        "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
        "request_id": request_id,
    }
    if result is not _MISSING:
        frame["result"] = _plain_json(result, "control result")
    else:
        if not isinstance(error, Mapping):
            raise LiveRuntimeProtocolError("invalid control error")
        normalized = _plain_json(dict(error), "control error")
        if isinstance(normalized.get("code"), bool) or not isinstance(
            normalized.get("code"), int
        ):
            raise LiveRuntimeProtocolError("invalid control error")
        _nonempty(normalized.get("message"), "control error")
        frame["error"] = normalized
    return frame
