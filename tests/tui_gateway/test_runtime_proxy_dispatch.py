from __future__ import annotations

from hermes_cli.live_runtime_owners import OwnerClaimResult, RuntimeOwner
from tui_gateway import runtime_proxy
from tui_gateway import server


class _DB:
    def __init__(self):
        self.closed = False

    def get_session(self, session_id):
        return {"id": session_id} if session_id == "stored-session" else None

    def get_session_by_title(self, title):
        return None

    def resolve_resume_session_id(self, session_id):
        assert session_id == "stored-session"
        return "compression-tip"

    def session_runtime_key(self, session_id):
        assert session_id == "compression-tip"
        return "conversation-root"

    def close(self):
        self.closed = True


class _Coordinator:
    def __init__(self):
        self.calls = []
        self.routed = []

    def prepare_resume(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "jsonrpc": "2.0",
            "id": kwargs["request"]["id"],
            "result": {"proxied": True},
        }

    def has_remote_route(self, transport, session_id):
        return session_id == "canonical-live"

    def route_request(self, request, transport):
        self.routed.append((request, transport))
        return {"jsonrpc": "2.0", "id": request["id"], "result": {"routed": True}}

    def detach_transport(self, transport):
        self.detached = transport


class _Transport:
    def __init__(self):
        self.writes = []

    def write(self, frame):
        self.writes.append(frame)
        return True


class _InlinePool:
    def submit(self, callback):
        callback()


def test_teardown_releases_owner_after_agent_close(monkeypatch):
    order = []

    class Agent:
        def close(self):
            order.append("agent.close")

    class Lease:
        released = False

        def release(self):
            order.append("lease.release")
            self.released = True
            return True

    session = {"agent": Agent(), "runtime_owner_lease": Lease()}
    monkeypatch.setattr(server, "_finalize_session", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_announce_session_reclaimed", lambda *args: None)

    server._teardown_session(session)

    assert order == ["agent.close", "lease.release"]
    assert "runtime_owner_lease" not in session


def test_deferred_record_detach_closes_live_sid_and_releases_owner_before_build(
    monkeypatch, tmp_path
):
    class Lease:
        released = False

        def release(self):
            self.released = True
            return True

    lease = Lease()
    transport = _Transport()
    sessions = {}
    monkeypatch.setattr(server, "_sessions", sessions)
    monkeypatch.setattr(server, "_finalize_session", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, "_announce_session_reclaimed", lambda *_args: None)

    token = server.bind_transport(transport)
    try:
        record = server._deferred_session_record(
            "live-session",
            "stored-session",
            cols=80,
            cwd=str(tmp_path),
            history=[],
            lease=None,
            runtime_owner_lease=lease,
            close_on_disconnect=True,
        )
    finally:
        server.reset_transport(token)
    sessions["live-session"] = record

    assert record["event_hub"].detach(transport) is True
    assert sessions == {}
    assert lease.released is True


def test_released_owner_lease_is_removed_from_coordinator(monkeypatch, tmp_path):
    class Lease:
        released = False

        def release(self):
            self.released = True
            return True

    lease = Lease()
    owner = RuntimeOwner(
        conversation_key="scoped-root",
        owner_id="owner-a",
        generation=1,
        pid=1,
        process_start_time=None,
        endpoint=str(tmp_path / "runtime.sock"),
        profile_home=str(tmp_path),
        surface="test",
        started_at=1.0,
    )
    monkeypatch.setattr(
        runtime_proxy,
        "claim_runtime_owner",
        lambda **_kwargs: OwnerClaimResult(kind="owned", owner=owner, lease=lease),
    )
    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "runtime.sock",
        owner_id="owner-a",
        surface="test",
        dispatch=lambda _request, _transport: None,
    )

    claimed = coordinator.claim_local(conversation_key="root", profile_home=tmp_path)
    assert coordinator._local_leases

    assert claimed.release() is True
    assert lease.released is True
    assert coordinator._local_leases == {}


def test_deferred_build_uses_stored_compression_root(monkeypatch, tmp_path):
    lease = object()
    claimed = []
    monkeypatch.setattr(
        server,
        "_claim_local_runtime_owner",
        lambda key, home: claimed.append((key, home)) or lease,
    )
    session = {
        "session_key": "compression-tip",
        "runtime_owner_key": "compression-root",
        "profile_home": str(tmp_path),
    }

    server._ensure_runtime_owner_for_session(session)

    assert claimed == [("compression-root", tmp_path)]
    assert session["runtime_owner_lease"] is lease


def test_live_runtime_lookup_is_profile_scoped(monkeypatch, tmp_path):
    profile_a = tmp_path / "profiles" / "a"
    profile_b = tmp_path / "profiles" / "b"
    sessions = {
        "a": {"runtime_owner_key": "shared-root", "profile_home": str(profile_a)},
        "b": {"runtime_owner_key": "shared-root", "profile_home": str(profile_b)},
    }
    monkeypatch.setattr(server, "_sessions", sessions)

    assert server._find_live_session_by_runtime_key("shared-root", profile_a) == (
        "a",
        sessions["a"],
    )
    assert server._find_live_session_by_runtime_key("shared-root", profile_b) == (
        "b",
        sessions["b"],
    )


def test_session_create_claims_owner_before_scheduling_agent(monkeypatch, tmp_path):
    order = []
    lease = object()
    sessions = {}

    def claim(conversation_key, profile_home):
        order.append(("claim", conversation_key, profile_home))
        return lease

    def schedule(session_id):
        assert sessions[session_id]["runtime_owner_lease"] is lease
        order.append(("schedule", session_id))

    monkeypatch.setattr(server, "_sessions", sessions)
    monkeypatch.setattr(server, "_new_session_key", lambda: "new-conversation-key")
    monkeypatch.setattr(server, "_coerce_seed_history", lambda messages: [])
    monkeypatch.setattr(server, "_completion_cwd", lambda params: str(tmp_path))
    monkeypatch.setattr(server, "_resolve_session_source", lambda source: "desktop")
    monkeypatch.setattr(server, "_enable_gateway_prompts", lambda: None)
    monkeypatch.setattr(server, "_profile_home", lambda profile: tmp_path)
    monkeypatch.setattr(server, "_load_show_reasoning", lambda: False)
    monkeypatch.setattr(server, "_load_tool_progress_mode", lambda: "compact")
    monkeypatch.setattr(server, "_register_session_cwd", lambda session: None)
    monkeypatch.setattr(server, "_attach_current_transport", lambda *args: None)
    monkeypatch.setattr(server, "_schedule_agent_build", schedule)
    monkeypatch.setattr(server, "_schedule_session_cap_enforcement", lambda: None)
    monkeypatch.setattr(server, "_history_to_messages", lambda history: [])
    monkeypatch.setattr(server, "_resolve_model", lambda: "test-model")
    monkeypatch.setattr(server, "_git_branch_for_cwd", lambda cwd: None)
    monkeypatch.setattr(server, "_project_info_for_cwd", lambda cwd: None)
    monkeypatch.setattr(server, "_response_profile_name", lambda profile: "ops")
    monkeypatch.setattr(server, "_claim_local_runtime_owner", claim)

    response = server.handle_request({
        "jsonrpc": "2.0",
        "id": "create-1",
        "method": "session.create",
        "params": {"profile": "ops", "source": "desktop"},
    })

    live_id = response["result"]["session_id"]
    assert sessions[live_id]["runtime_owner_lease"] is lease
    assert order == [
        ("claim", "new-conversation-key", tmp_path),
        ("schedule", live_id),
    ]


def test_session_create_without_cross_process_transport_uses_registry_fencing(
    monkeypatch, tmp_path
):
    sessions = {}

    monkeypatch.setattr(server, "_sessions", sessions)
    monkeypatch.setattr(server, "_cross_process_runtime_proxy_supported", lambda: False)
    monkeypatch.setattr(
        server,
        "_get_runtime_proxy_coordinator",
        lambda: (_ for _ in ()).throw(AssertionError("proxy coordinator started")),
    )
    monkeypatch.setattr(server, "_new_session_key", lambda: "windows-local-session")
    monkeypatch.setattr(server, "_coerce_seed_history", lambda _messages: [])
    monkeypatch.setattr(server, "_completion_cwd", lambda _params: str(tmp_path))
    monkeypatch.setattr(server, "_resolve_session_source", lambda _source: "desktop")
    monkeypatch.setattr(server, "_enable_gateway_prompts", lambda: None)
    monkeypatch.setattr(server, "_profile_home", lambda _profile: None)
    monkeypatch.setattr(server, "_load_show_reasoning", lambda: False)
    monkeypatch.setattr(server, "_load_tool_progress_mode", lambda: "compact")
    monkeypatch.setattr(server, "_register_session_cwd", lambda _session: None)
    monkeypatch.setattr(server, "_attach_current_transport", lambda *_args: None)
    monkeypatch.setattr(server, "_schedule_agent_build", lambda _sid: None)
    monkeypatch.setattr(server, "_schedule_session_cap_enforcement", lambda: None)
    monkeypatch.setattr(server, "_history_to_messages", lambda _history: [])
    monkeypatch.setattr(server, "_resolve_model", lambda: "test-model")
    monkeypatch.setattr(server, "_git_branch_for_cwd", lambda _cwd: None)
    monkeypatch.setattr(server, "_project_info_for_cwd", lambda _cwd: None)
    monkeypatch.setattr(server, "_response_profile_name", lambda _profile: "default")

    response = server.handle_request({
        "jsonrpc": "2.0",
        "id": "create-local",
        "method": "session.create",
        "params": {"source": "desktop"},
    })

    assert "error" not in response
    live_id = response["result"]["session_id"]
    lease = sessions[live_id]["runtime_owner_lease"]
    assert lease.owner.endpoint.startswith("process-local:")
    assert lease.owner.surface == "tui_gateway_process_local"
    assert lease.release() is True


def test_cross_process_runtime_proxy_requires_so_peercred(monkeypatch):
    monkeypatch.delattr(server.socket, "SO_PEERCRED", raising=False)

    assert server._cross_process_runtime_proxy_supported() is False


def test_disconnect_detaches_remote_transport(monkeypatch):
    coordinator = _Coordinator()
    transport = _Transport()
    monkeypatch.setattr(server, "_runtime_proxy_coordinator", coordinator)

    server.detach_runtime_proxy_transport(transport)

    assert coordinator.detached is transport


def test_dispatch_routes_retained_session_to_owner_without_local_handler(monkeypatch):
    coordinator = _Coordinator()
    transport = _Transport()
    request = {
        "jsonrpc": "2.0",
        "id": "interrupt-1",
        "method": "session.interrupt",
        "params": {"session_id": "canonical-live"},
    }
    monkeypatch.setattr(server, "_runtime_proxy_coordinator", coordinator)
    monkeypatch.setattr(server, "_pool", _InlinePool())
    monkeypatch.setattr(
        server,
        "handle_request",
        lambda request: (_ for _ in ()).throw(AssertionError("local handler called")),
    )

    assert server.dispatch(request, transport) is None
    assert coordinator.routed == [(request, transport)]
    assert transport.writes[0]["result"] == {"routed": True}


def test_dispatch_answers_when_selected_remote_route_disappears(monkeypatch):
    coordinator = _Coordinator()
    transport = _Transport()
    request = {
        "jsonrpc": "2.0",
        "id": "compress-1",
        "method": "session.compress",
        "params": {"session_id": "canonical-live"},
    }
    coordinator.route_request = lambda _request, _transport: None
    monkeypatch.setattr(server, "_runtime_proxy_coordinator", coordinator)
    monkeypatch.setattr(server, "_pool", _InlinePool())

    assert server.dispatch(request, transport) is None
    assert len(transport.writes) == 1
    response = transport.writes[0]
    assert response["id"] == "compress-1"
    assert response["error"]["code"] == -32072
    assert response["error"]["data"] == {"outcome": "unknown", "retryable": False}


def test_resume_proxy_resolves_compression_root_before_local_handler(
    monkeypatch, tmp_path
):
    db = _DB()
    coordinator = _Coordinator()
    transport = object()
    request = {
        "jsonrpc": "2.0",
        "id": "resume-1",
        "method": "session.resume",
        "params": {"session_id": "stored-session"},
    }
    monkeypatch.setattr(server, "_db_for_profile", lambda profile: (db, True))
    monkeypatch.setattr(server, "_profile_home", lambda profile: tmp_path)
    monkeypatch.setattr(server, "_get_runtime_proxy_coordinator", lambda: coordinator)

    response = server._prepare_runtime_resume_proxy(request, transport)

    assert response["result"] == {"proxied": True}
    assert coordinator.calls[0]["conversation_key"] == "conversation-root"
    assert coordinator.calls[0]["profile_home"] == tmp_path
    assert coordinator.calls[0]["transport"] is transport
    assert db.closed is True
