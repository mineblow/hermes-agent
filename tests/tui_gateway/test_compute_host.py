import json
import os
import queue
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from tui_gateway.compute_host import ComputeHost, HostSession


def _stdout_queue(proc: subprocess.Popen) -> queue.Queue[dict]:
    out: queue.Queue[dict] = queue.Queue()
    assert proc.stdout is not None

    def drain() -> None:
        for line in proc.stdout or []:
            out.put(json.loads(line))

    threading.Thread(target=drain, daemon=True).start()
    return out


def _read_json_line(out: queue.Queue[dict], timeout: float = 2.0) -> dict:
    try:
        return out.get(timeout=timeout)
    except queue.Empty as exc:
        raise AssertionError("timed out waiting for compute host JSON") from exc


def test_compute_host_line_json_seed_turn_interrupt():
    repo = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, "-m", "tui_gateway.compute_host"],
        cwd=str(repo),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    assert proc.stdin is not None
    out = _stdout_queue(proc)
    try:
        hello = _read_json_line(out)
        assert hello["type"] == "hello"
        assert hello["host_pid"] == proc.pid

        proc.stdin.write(json.dumps({"type": "session.seed", "sid": "s1", "request_id": "seed"}) + "\n")
        proc.stdin.flush()
        assert _read_json_line(out)["type"] == "session.seeded"

        proc.stdin.write(
            json.dumps(
                {
                    "type": "turn.start",
                    "sid": "s1",
                    "request_id": "turn",
                    "prompt": "hello",
                    "delta_count": 3,
                    "delay_s": 0,
                }
            )
            + "\n"
        )
        proc.stdin.flush()

        seen = []
        while True:
            frame = _read_json_line(out)
            seen.append(frame["type"])
            if frame["type"] == "turn.end":
                assert frame["history_version"] == 1
                assert frame["message_count"] == 2
                break
        assert seen.count("delta") == 3

        proc.stdin.write(json.dumps({"type": "shutdown", "request_id": "stop"}) + "\n")
        proc.stdin.flush()
        assert _read_json_line(out)["type"] == "shutdown.ack"
        proc.wait(timeout=2)
    finally:
        if proc.poll() is None:
            proc.kill()


@pytest.mark.parametrize("kind", ["legacy", "hard-only", "dynamic-getattr"])
def test_compute_host_interrupt_uses_explicit_stop_compatibility(kind):
    calls = []

    class _Legacy:
        def interrupt(self):
            calls.append("legacy")

    class _HardOnly:
        def hard_interrupt(self):
            calls.append("hard")

    class _Dynamic:
        def interrupt(self):
            calls.append("legacy")

        def __getattr__(self, name):
            if name == "hard_interrupt":
                return lambda: calls.append("fabricated-hard")
            raise AttributeError(name)

    agent = {
        "legacy": _Legacy(),
        "hard-only": _HardOnly(),
        "dynamic-getattr": _Dynamic(),
    }[kind]
    host = ComputeHost(heartbeat_secs=0)
    host._sessions["s1"] = HostSession(sid="s1", agent=agent)
    emitted = []
    host.emit = emitted.append
    try:
        host._handle_interrupt({"sid": "s1", "request_id": "stop"})
    finally:
        host.close()

    assert calls == ["hard" if kind == "hard-only" else "legacy"]
    assert emitted[-1]["applied"] is True


def test_real_turn_forwards_user_event_metadata_to_gateway_runner(monkeypatch):
    from tui_gateway import server

    captured = {}
    session = {
        "history_lock": threading.RLock(),
        "history": [],
        "history_version": 0,
        "running": False,
        "session_key": "session-key",
        "agent": None,
    }
    host = ComputeHost(heartbeat_secs=0)
    host.emit = lambda _frame: None
    monkeypatch.setattr(host, "_ensure_server_session", lambda _server, _frame: session)
    monkeypatch.setattr(server, "_start_inflight_turn", lambda *_args: None)
    monkeypatch.setattr(server, "_ensure_session_db_row", lambda *_args: None)
    monkeypatch.setattr(server, "_persist_branch_seed", lambda *_args: None)
    monkeypatch.setattr(server, "_session_info", lambda *_args: {})
    monkeypatch.setattr(
        server,
        "_run_prompt_submit",
        lambda rid, sid, session, text, **kwargs: captured.update(
            rid=rid, sid=sid, text=text, **kwargs
        ),
    )

    try:
        host._run_real_turn({
            "type": "turn.start",
            "sid": "sid",
            "request_id": "r1",
            "session_key": "session-key",
            "text": "expanded model-facing prompt",
            "client_message_id": "user-pc1-123",
            "display_text": "hello from pc 1",
            "submitted_at": 1234.5,
            "attachment_refs": ["attachment://one"],
            "display_metadata": {"platform": "discord", "user_id": "user-1"},
        })
    finally:
        host.close()

    assert captured == {
        "rid": "r1",
        "sid": "sid",
        "text": "expanded model-facing prompt",
        "display_kind": None,
        "display_metadata": {"platform": "discord", "user_id": "user-1"},
        "client_message_id": "user-pc1-123",
        "client_identity": None,
        "display_text": "hello from pc 1",
        "submitted_at": 1234.5,
        "attachment_refs": ["attachment://one"],
    }
