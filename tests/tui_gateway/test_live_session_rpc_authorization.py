from __future__ import annotations

from contextlib import contextmanager

import pytest

from tui_gateway import server
from tui_gateway.session_events import AttachmentMode, SessionEventHub
from tui_gateway.transport import bind_transport, reset_transport


class _Transport:
    def __init__(self, client_id: str, capabilities: frozenset[str]) -> None:
        self.client_id = client_id
        self.connection_id = client_id
        self.negotiated_capabilities = capabilities

    def write(self, _frame: dict) -> bool:
        return True

    def close(self) -> None:
        return None


@contextmanager
def _bound(transport: _Transport):
    token = bind_transport(transport)
    try:
        yield
    finally:
        reset_transport(token)


@pytest.fixture(autouse=True)
def _isolate_server_state():
    methods = dict(server._methods)
    sessions = dict(server._sessions)
    server._sessions.clear()
    try:
        yield
    finally:
        for session in server._sessions.values():
            hub = session.get("event_hub")
            if hub is not None:
                hub.close()
        server._sessions.clear()
        server._sessions.update(sessions)
        server._methods.clear()
        server._methods.update(methods)


def _live_session(
    sid: str, transport: _Transport, mode: AttachmentMode
) -> SessionEventHub:
    hub = SessionEventHub()
    hub.attach(transport, client_id=transport.client_id, mode=mode)
    server._sessions[sid] = {"event_hub": hub}
    return hub


def _request(method: str, session_id: str = "live") -> dict:
    response = server.handle_request({
        "jsonrpc": "2.0",
        "id": method,
        "method": method,
        "params": {"session_id": session_id},
    })
    assert response is not None
    return response


def test_active_list_only_reveals_sessions_attached_to_request_transport(monkeypatch):
    caller = _Transport("caller", frozenset({"observe"}))
    other = _Transport("other", frozenset({"observe"}))
    _live_session("attached", caller, AttachmentMode.OBSERVE)
    _live_session("foreign", other, AttachmentMode.OBSERVE)
    monkeypatch.setattr(
        server,
        "_session_live_item",
        lambda sid, _session, _current: {"session_id": sid},
    )

    with _bound(caller):
        response = server.handle_request({
            "id": "list",
            "method": "session.active_list",
            "params": {},
        })

    assert response["result"]["sessions"] == [{"session_id": "attached"}]


def test_active_list_reveals_nothing_to_unattached_transport(monkeypatch):
    attached = _Transport("attached", frozenset({"observe"}))
    stranger = _Transport("stranger", frozenset({"observe"}))
    _live_session("live", attached, AttachmentMode.OBSERVE)
    monkeypatch.setattr(
        server,
        "_session_live_item",
        lambda sid, _session, _current: {"session_id": sid},
    )

    with _bound(stranger):
        response = server.handle_request({
            "id": "list",
            "method": "session.active_list",
            "params": {},
        })

    assert response["result"]["sessions"] == []


def test_unbound_active_list_does_not_bypass_stdio_attachment_membership(monkeypatch):
    attached = _Transport("attached", frozenset({"observe"}))
    _live_session("live", attached, AttachmentMode.OBSERVE)
    monkeypatch.setattr(
        server,
        "_session_live_item",
        lambda sid, _session, _current: {"session_id": sid},
    )

    response = server.handle_request({
        "id": "list",
        "method": "session.active_list",
        "params": {},
    })

    assert response["result"]["sessions"] == []


def test_activate_rejects_transport_without_existing_attachment(monkeypatch):
    attached = _Transport("attached", frozenset({"observe"}))
    stranger = _Transport("stranger", frozenset({"observe"}))
    _live_session("live", attached, AttachmentMode.OBSERVE)
    monkeypatch.setattr(server, "_live_session_payload", lambda *_args, **_kwargs: {})

    with _bound(stranger):
        response = _request("session.activate")

    assert response["error"]["code"] == 4003


def test_unbound_activate_does_not_implicitly_attach_stdio_as_controller(monkeypatch):
    attached = _Transport("attached", frozenset({"observe"}))
    hub = _live_session("live", attached, AttachmentMode.OBSERVE)
    monkeypatch.setattr(server, "_live_session_payload", lambda *_args, **_kwargs: {})

    response = _request("session.activate")

    assert response["error"]["code"] == 4003
    assert hub.count() == 1


def test_activate_preserves_observe_only_attachment(monkeypatch):
    observer = _Transport("observer", frozenset({"observe"}))
    hub = _live_session("live", observer, AttachmentMode.OBSERVE)
    monkeypatch.setattr(
        server,
        "_live_session_payload",
        lambda sid, _session, **_kwargs: {"session_id": sid},
    )

    with _bound(observer):
        response = _request("session.activate")

    assert response["result"] == {"session_id": "live"}
    assert hub.snapshots() == [
        {"client_id": "observer", "mode": "observe", "capabilities": ["observe"]}
    ]


@pytest.mark.parametrize(
    "method",
    [
        "session.cwd.set",
        "session.title",
        "session.set_hidden",
        "session.compress",
        "config.set",
        "handoff.request",
        "message.react",
        "llm.oneshot",
    ],
)
def test_live_session_mutations_require_controller_capability(method):
    observer = _Transport("observer", frozenset({"observe"}))
    _live_session("live", observer, AttachmentMode.OBSERVE)
    called = []
    server._methods[method] = lambda rid, _params: (
        called.append(True) or server._ok(rid, {})
    )

    with _bound(observer):
        response = _request(method)

    assert response["error"]["code"] == 4003
    assert called == []


def test_workspace_move_resolves_live_session_by_durable_key(monkeypatch):
    owner = _Transport("owner", frozenset({"observe", "session.steer"}))
    stranger = _Transport("stranger", frozenset({"observe", "session.steer"}))
    _live_session("live", owner, AttachmentMode.CONTROL)
    server._sessions["live"]["session_key"] = "durable"
    called = []
    monkeypatch.setitem(server._methods, "session.workspace.move", lambda rid, params: called.append(params) or {})

    with _bound(stranger):
        response = server.handle_request({
            "id": "rpc",
            "method": "session.workspace.move",
            "params": {"cwd": "/tmp", "session_key": "durable"},
        })

    assert response["error"]["code"] == 4003
    assert called == []


def test_unclassified_session_rpc_resolving_live_session_fails_closed():
    controller = _Transport("controller", AttachmentMode.CONTROL.capabilities)
    _live_session("live", controller, AttachmentMode.CONTROL)
    called = []
    server._methods["session.future_mutation"] = lambda rid, _params: (
        called.append(True) or server._ok(rid, {})
    )

    with _bound(controller):
        response = _request("session.future_mutation")

    assert response["error"]["code"] == 4003
    assert "authorization policy" in response["error"]["message"]
    assert called == []


def test_unclassified_session_rpc_keeps_non_live_legacy_behavior():
    server._methods["session.legacy_lookup"] = lambda rid, _params: server._ok(
        rid, {"legacy": True}
    )
    transport = _Transport("caller", frozenset({"observe"}))

    with _bound(transport):
        response = _request("session.legacy_lookup", session_id="stored-only")

    assert response["result"] == {"legacy": True}
