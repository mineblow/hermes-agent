from __future__ import annotations

import threading
import types

from tui_gateway import server


class _Resource:
    def __init__(self) -> None:
        self.closed = 0
        self.released = 0

    def close(self) -> None:
        self.closed += 1

    def release(self) -> bool:
        self.released += 1
        return True


class _ImmediateThread:
    def __init__(self, *, target, daemon):
        self.target = target
        self.daemon = daemon

    def start(self) -> None:
        self.target()


def test_deferred_resume_hydration_failure_claims_and_fully_tears_down(
    monkeypatch,
):
    sid = "partial-hydration"
    hub = _Resource()
    runtime_owner = _Resource()
    active_lease = _Resource()
    worker = _Resource()
    ready = threading.Event()
    session = {
        "session_key": "stored-id",
        "history": [],
        "history_lock": threading.Lock(),
        "resume_history_ready": ready,
        "agent_ready": threading.Event(),
        "resume_hydrating": True,
        "event_hub": hub,
        "runtime_owner_lease": runtime_owner,
        "active_session_lease": active_lease,
        "slash_worker": worker,
    }

    class FailingDB:
        def reopen_session(self, _stored_id):
            raise RuntimeError("history unavailable")

    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(server, "_emit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, "_notify_session_boundary", lambda *_args: None)
    server._sessions[sid] = session

    try:
        server._schedule_resume_hydration(sid, "stored-id", FailingDB())

        assert sid not in server._sessions
        assert hub.closed == 1
        assert runtime_owner.released == 1
        assert active_lease.released == 1
        assert worker.closed == 1
        assert ready.is_set()
    finally:
        partial = server._sessions.pop(sid, None)
        if partial is not None:
            server._teardown_session(partial)


def test_branch_init_failure_claims_partial_record_and_fully_tears_down(monkeypatch):
    parent = {
        "session_key": "parent-key",
        "history": [{"role": "user", "content": "hello"}],
        "history_lock": threading.Lock(),
        "running": False,
        "source": "tui",
        "cwd": "/tmp",
    }
    hub = _Resource()
    runtime_owner = _Resource()
    active_lease = _Resource()
    worker = _Resource()
    agent = _Resource()
    created_sids = []

    class BranchDB:
        def get_resume_conversations(self, _key):
            history = [{"role": "user", "content": "hello"}]
            return history, history

        def get_session_title(self, _key):
            return "parent"

        def get_next_title_in_lineage(self, title):
            return f"{title} (branch)"

        def create_session(self, *_args, **_kwargs):
            return None

        def append_messages_batch(self, *_args, **_kwargs):
            return [1]

        def set_session_title(self, *_args, **_kwargs):
            return True

    class DBContext:
        def __enter__(self):
            return BranchDB()

        def __exit__(self, *_args):
            return False

    def fail_after_registration(sid, key, built_agent, history, **_kwargs):
        created_sids.append(sid)
        server._sessions[sid] = {
            "session_key": key,
            "agent": built_agent,
            "history": history,
            "history_lock": threading.Lock(),
            "event_hub": hub,
            "active_session_lease": active_lease,
            "slash_worker": worker,
        }
        raise RuntimeError("partial init")

    monkeypatch.setattr(server, "_sess", lambda _params, _rid: (parent, None))
    monkeypatch.setattr(server, "_session_db", lambda _session: DBContext())
    monkeypatch.setattr(server, "_session_cwd", lambda _session: "/tmp")
    monkeypatch.setattr(server, "_resolve_model", lambda: "test")
    monkeypatch.setattr(server, "_make_agent", lambda *_args, **_kwargs: agent)
    monkeypatch.setattr(server, "_init_session", fail_after_registration)
    monkeypatch.setattr(
        server, "_claim_local_runtime_owner", lambda *_args: runtime_owner
    )
    monkeypatch.setattr(server, "_notify_session_boundary", lambda *_args: None)
    monkeypatch.setattr(server, "_new_session_key", lambda: "branch-key")

    try:
        response = server._methods["session.branch"](
            "branch-request", {"session_id": "parent"}
        )

        assert response["error"]["code"] == 5000
        assert created_sids[0] not in server._sessions
        assert hub.closed == 1
        assert runtime_owner.released == 1
        assert active_lease.released == 1
        assert worker.closed == 1
        assert agent.closed == 1
    finally:
        for sid in created_sids:
            partial = server._sessions.pop(sid, None)
            if partial is not None:
                server._teardown_session(partial)
