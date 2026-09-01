from __future__ import annotations

import pytest

from hermes_cli import live_runtime_protocol as protocol


PRINCIPAL = {
    "provider": "gateway-auth",
    "subject": "user-1",
    "authenticated": True,
}


def test_protocol_capabilities_have_no_surface_or_renderer_names():
    assert "interaction.respond" in protocol.SUPPORTED_CAPABILITIES
    assert "ui.respond" not in protocol.SUPPORTED_CAPABILITIES
    assert all(
        marker not in capability.lower()
        for capability in protocol.SUPPORTED_CAPABILITIES
        for marker in ("desktop", "discord", "tui")
    )


def test_frontend_hello_carries_transport_neutral_identity_and_replay_watermark():
    frame = protocol.frontend_hello(
        client_id="discord-installation-1",
        principal=PRINCIPAL,
        surface="discord",
        requested_capabilities={"observe", "prompt.submit"},
        durable_root="durable-session-root",
        replay_epoch="epoch-2",
        replay_seq=41,
    )

    assert frame == {
        "kind": "frontend.hello",
        "protocol": protocol.LIVE_RUNTIME_PROTOCOL_VERSION,
        "client_id": "discord-installation-1",
        "principal": PRINCIPAL,
        "surface": "discord",
        "requested_capabilities": ["observe", "prompt.submit"],
        "durable_root": "durable-session-root",
        "replay": {"epoch": "epoch-2", "seq": 41},
    }
    assert protocol.validate_frontend_hello(frame) == frame


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("client_id", "", "client identity"),
        ("principal", {"subject": "user-1"}, "principal"),
        ("surface", "", "surface"),
        ("durable_root", "", "durable root"),
        ("requested_capabilities", ["runtime.admin"], "capabilities"),
        ("replay", {"epoch": "epoch-1", "seq": -1}, "replay watermark"),
    ],
)
def test_frontend_hello_rejects_invalid_or_privilege_escalating_fields(
    field, value, message
):
    frame = protocol.frontend_hello(
        client_id="client-1",
        principal=PRINCIPAL,
        surface="web",
        requested_capabilities={"observe"},
        durable_root="root-1",
    )
    frame[field] = value

    with pytest.raises(protocol.LiveRuntimeProtocolError, match=message):
        protocol.validate_frontend_hello(frame)


def test_runtime_input_preserves_stable_identity_display_metadata_and_attachments():
    frame = protocol.runtime_input(
        message_id="message-1",
        text="deploy staging",
        display_text="Deploy staging",
        submitted_at=1_725_000_000.5,
        attachment_refs=["attachment://manifest"],
        display_metadata={"source_message_id": "discord-99"},
        busy_policy="queue",
    )

    assert protocol.validate_runtime_input(frame) == frame
    assert frame["message_id"] == "message-1"
    assert frame["display_metadata"] == {"source_message_id": "discord-99"}
    assert frame["attachment_refs"] == ["attachment://manifest"]


def test_runtime_input_rejects_unknown_busy_policy_and_duplicate_attachment_refs():
    frame = protocol.runtime_input(message_id="message-1", text="hello")
    frame["busy_policy"] = "silently-drop"
    with pytest.raises(protocol.LiveRuntimeProtocolError, match="busy policy"):
        protocol.validate_runtime_input(frame)

    frame = protocol.runtime_input(
        message_id="message-1",
        text="hello",
        attachment_refs=["attachment://one"],
    )
    frame["attachment_refs"].append("attachment://one")
    with pytest.raises(protocol.LiveRuntimeProtocolError, match="attachment refs"):
        protocol.validate_runtime_input(frame)


def test_runtime_event_envelope_has_ordering_runtime_and_durable_identity():
    frame = protocol.runtime_event(
        runtime_id="runtime-1",
        durable_session_id="session-1",
        replay_epoch="epoch-4",
        seq=19,
        event_type="tool.start",
        payload={"tool_id": "tool-1", "name": "terminal"},
    )

    assert protocol.validate_runtime_event(frame) == frame
    assert frame == {
        "kind": "runtime.event",
        "protocol": protocol.LIVE_RUNTIME_PROTOCOL_VERSION,
        "runtime_id": "runtime-1",
        "durable_session_id": "session-1",
        "replay_epoch": "epoch-4",
        "seq": 19,
        "type": "tool.start",
        "payload": {"tool_id": "tool-1", "name": "terminal"},
    }


@pytest.mark.parametrize("field", ["runtime_id", "durable_session_id", "replay_epoch", "type"])
def test_runtime_event_rejects_missing_identity_or_type(field):
    frame = protocol.runtime_event(
        runtime_id="runtime-1",
        durable_session_id="session-1",
        replay_epoch="epoch-1",
        seq=1,
        event_type="message.delta",
        payload={"text": "hello"},
    )
    frame[field] = ""

    with pytest.raises(protocol.LiveRuntimeProtocolError):
        protocol.validate_runtime_event(frame)


def test_control_response_is_point_to_point_and_exactly_one_of_result_or_error():
    success = protocol.control_response("request-1", result={"status": "ok"})
    failure = protocol.control_response(
        "request-2",
        error={"code": -32072, "message": "owner unavailable"},
    )

    assert success == {
        "kind": "control.response",
        "protocol": protocol.LIVE_RUNTIME_PROTOCOL_VERSION,
        "request_id": "request-1",
        "result": {"status": "ok"},
    }
    assert failure["error"]["code"] == -32072
    with pytest.raises(ValueError, match="exactly one"):
        protocol.control_response(
            "request-3",
            result={"ok": True},
            error={"code": 1, "message": "bad"},
        )
