import threading

import pytest

from tui_gateway import server
from tui_gateway.session_events import AttachmentMode
from tui_gateway.transport import bind_transport, reset_transport


class _Transport:
    def __init__(self, client_id: str, auth_identity=None):
        self.client_id = client_id
        self.connection_id = f"connection-{client_id}"
        self.auth_identity = auth_identity
        self.frames = []

    def write(self, obj: dict) -> bool:
        self.frames.append(obj)
        return True

    def close(self) -> None:
        pass


def _dispatch(method: str, params: dict, transport: _Transport):
    token = bind_transport(transport)
    try:
        return server.handle_request({"id": "request", "method": method, "params": params})
    finally:
        reset_transport(token)


def _live_session(agent):
    return {
        "agent": agent,
        "history": [],
        "history_lock": threading.Lock(),
        "inflight_turn": {
            "user": "original",
            "assistant": "partial",
            "streaming": True,
        },
        "running": True,
        "session_key": "durable-session",
    }


@pytest.mark.parametrize(
    ("method", "agent_factory", "accepted_status"),
    [
        (
            "session.steer",
            lambda calls: type(
                "Agent", (), {"steer": lambda self, text: calls.append(text) or True}
            )(),
            "queued",
        ),
        (
            "session.redirect",
            lambda calls: type(
                "Agent",
                (),
                {
                    "_supports_active_turn_redirect": True,
                    "redirect": lambda self, text: calls.append(text) or True,
                },
            )(),
            "redirected",
        ),
    ],
)
def test_control_rpc_idempotency_is_scoped_to_stable_client(
    method, agent_factory, accepted_status
):
    calls = []
    session = _live_session(agent_factory(calls))
    first = _Transport("client-a")
    second = _Transport("client-b")
    hub = server._ensure_session_event_hub("sid", session)
    hub.attach(first, client_id=first.client_id, mode=AttachmentMode.CONTROL)
    hub.attach(second, client_id=second.client_id, mode=AttachmentMode.CONTROL)
    server._sessions["sid"] = session
    params = {"session_id": "sid", "text": "correction", "client_message_id": "same-id"}
    try:
        accepted = _dispatch(method, params, first)
        duplicate = _dispatch(method, params, first)
        other_client = _dispatch(method, params, second)
    finally:
        server._sessions.pop("sid", None)
        hub.close()

    assert accepted["result"]["status"] == accepted_status
    assert duplicate["result"] == {
        "status": "duplicate",
        "duplicate": True,
        "client_message_id": "same-id",
    }
    assert other_client["result"]["status"] == accepted_status
    assert calls == ["correction", "correction"]


def test_authenticated_principal_is_stable_across_reconnected_client_ids():
    first = _Transport("window-before-reload", {"user_id": "account-1"})
    reconnected = _Transport("window-after-reload", {"user_id": "account-1"})

    assert server._client_message_identity("message-1", first) == server._client_message_identity(
        "message-1", reconnected
    )


def test_durable_scoped_identity_accepts_other_clients_with_same_message_id():
    first = _Transport("client-a")
    second = _Transport("client-b")
    first_identity = server._client_message_identity("same-id", first)
    session = {
        "history": [
            {
                "role": "user",
                "content": "hello",
                "display_metadata": {
                    "client_message_id": "same-id",
                    "client_identity": first_identity[0],
                },
            }
        ]
    }

    assert server._client_message_id_is_accepted(
        session, "same-id", server._client_message_identity("same-id", first)
    )
    assert not server._client_message_id_is_accepted(
        session, "same-id", server._client_message_identity("same-id", second)
    )


def test_old_durable_bare_message_ids_remain_global_duplicates():
    session = {
        "history": [
            {
                "role": "user",
                "content": "hello",
                "display_metadata": {"client_message_id": "legacy-id"},
            }
        ]
    }

    assert server._client_message_id_is_accepted(
        session,
        "legacy-id",
        server._client_message_identity("legacy-id", _Transport("new-client")),
    )
