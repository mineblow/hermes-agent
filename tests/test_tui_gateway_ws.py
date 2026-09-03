import asyncio
import concurrent.futures
import json
import threading
import time
import uuid

from hermes_cli import mcp_startup
from tui_gateway import server
from tui_gateway import ws as ws_mod




def _run_disconnect(monkeypatch, seed):
    """Drive handle_ws to its disconnect `finally`, seeding sessions against the
    live WSTransport the moment it exists. Returns nothing; inspect _sessions."""
    # Disable the grace-reap Timer: detached sessions normally schedule a
    # threading.Timer via _schedule_ws_orphan_reap, which would outlive the test
    # and fire _reap during interpreter teardown — touching _sessions/DB and
    # producing spurious post-run errors under the per-file CI runner. Grace=0
    # short-circuits the Timer (see _schedule_ws_orphan_reap) so the test leaves
    # no lingering thread.
    monkeypatch.setattr(server, "_WS_ORPHAN_REAP_GRACE_S", 0)

    # Mirror the real _finalize_session chokepoint: it is the single place that
    # closes the slash-worker (#38095). Stub it but keep that behavior so the
    # disconnect-reap path still exercises worker teardown.
    def _fake_finalize(s, end_reason="tui_close"):
        w = s.get("slash_worker")
        if w:
            w.close()

    monkeypatch.setattr(server, "_finalize_session", _fake_finalize)

    created = []
    real_transport = ws_mod.WSTransport
    monkeypatch.setattr(
        ws_mod, "WSTransport",
        lambda ws, loop, **kw: created.append(real_transport(ws, loop, **kw)) or created[-1],
    )

    class FakeWS:
        async def accept(self):
            pass

        async def send_text(self, line):
            pass

        async def receive_text(self):
            seed(created[0])  # transport now exists; attach it to sessions
            raise ws_mod._WebSocketDisconnect()

        async def close(self):
            pass

    asyncio.run(ws_mod.handle_ws(FakeWS()))


def test_ws_disconnect_reaps_flagged_session_and_closes_worker(monkeypatch):
    closed = []

    class FakeWorker:
        def close(self):
            closed.append(True)

    server._sessions.clear()
    try:
        _run_disconnect(
            monkeypatch,
            lambda t: server._sessions.update(
                flagged={
                    "transport": t,
                    "close_on_disconnect": True,
                    "slash_worker": FakeWorker(),
                    "session_key": "k",
                }
            ),
        )
        assert "flagged" not in server._sessions
        assert closed == [True]
    finally:
        server._sessions.clear()




def test_ws_connection_registers_then_disconnect_unregisters_live_transport(monkeypatch):
    """A connected client must be tracked in the live-transport registry so a
    session-less global broadcast (skin.changed from the background watcher)
    reaches it, and dropped on disconnect so no stale write targets a dead peer.
    This is the WS half of the cross-surface live-theme fix."""
    server._sessions.clear()
    server._live_transports.clear()
    seen = {}
    try:
        _run_disconnect(
            monkeypatch,
            lambda t: seen.__setitem__("registered", t in server._live_transports),
        )
        # Seeded at receive_text time — i.e. after gateway.ready registered it.
        assert seen["registered"] is True
        # handle_ws's finally must have unregistered it.
        assert not server._live_transports
    finally:
        server._sessions.clear()
        server._live_transports.clear()


def test_ws_disconnect_releases_wake_word_owner(monkeypatch):
    released = []
    created = []
    monkeypatch.setattr(
        server,
        "_release_wake_for_transport",
        lambda transport: released.append(transport) or True,
    )

    _run_disconnect(monkeypatch, lambda transport: created.append(transport))

    assert released == created




def test_ws_starts_mcp_discovery_before_ready(monkeypatch):
    import tui_gateway.entry as entry

    calls = []
    events = []

    monkeypatch.setattr(server, "_WS_ORPHAN_REAP_GRACE_S", 0)
    monkeypatch.setattr(entry, "ensure_mcp_discovery_started", lambda: calls.append("mcp"))

    class FakeWS:
        async def accept(self):
            events.append("accept")

        async def send_text(self, line):
            if '"gateway.ready"' in line:
                events.append(f"ready_after_{len(calls)}")

        async def receive_text(self):
            raise ws_mod._WebSocketDisconnect()

        async def close(self):
            pass

    asyncio.run(ws_mod.handle_ws(FakeWS()))

    # Discovery moved to profile-aware agent construction. WebSocket transport
    # should not start MCP discovery before a profile has been bound.
    assert calls == []
    assert events == ["accept", "ready_after_0"]


def test_ws_ready_advertises_heartbeat_and_ping_is_inline(monkeypatch):
    sent = []
    inbound = iter(
        [
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": "heartbeat-1",
                    "method": "gateway.ping",
                    "params": {},
                }
            )
        ]
    )
    monkeypatch.setattr(server, "_WS_ORPHAN_REAP_GRACE_S", 0)

    class FakeWS:
        async def accept(self):
            pass

        async def send_text(self, line):
            sent.append(json.loads(line))

        async def receive_text(self):
            try:
                return next(inbound)
            except StopIteration:
                raise ws_mod._WebSocketDisconnect()

        async def close(self):
            pass

    asyncio.run(ws_mod.handle_ws(FakeWS()))

    ready = sent[0]["params"]
    assert ready["type"] == "gateway.ready"
    assert ready["payload"]["heartbeat"] is True
    uuid.UUID(hex=ready["payload"]["connection_id"])
    assert ready["payload"]["runtime_host_id"] == server._backend_id_for_this_process()
    assert ready["payload"]["multi_client_sessions"] == 1
    assert ready["payload"]["capabilities"] == [
        "client.attach",
        "session.attach",
        "session.detach",
        "session.attachments",
    ]
    assert ready["payload"]["multi_client"] == {
        "protocol_version": 1,
        "attachment_modes": ["observe", "control"],
        "methods": [
            "client.attach",
            "session.attach",
            "session.detach",
            "session.attachments",
            "session.events.since",
        ],
    }
    assert sent[1] == {
        "jsonrpc": "2.0",
        "result": {"ok": True},
        "id": "heartbeat-1",
    }


def test_ws_transport_serializes_concurrent_sends():
    active_sends = 0
    max_active_sends = 0
    sent = []

    class FakeWS:
        async def send_text(self, line):
            nonlocal active_sends, max_active_sends
            active_sends += 1
            max_active_sends = max(max_active_sends, active_sends)
            try:
                await asyncio.sleep(0.05)
                sent.append(line)
            finally:
                active_sends -= 1

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        transport = ws_mod.WSTransport(FakeWS(), loop, peer="serialize-test")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(transport.write, {"idx": 1}),
                pool.submit(transport.write, {"idx": 2}),
            ]
            assert [f.result(timeout=2) for f in futures] == [True, True]

        assert len(sent) == 2
        assert max_active_sends == 1
        assert transport._closed is False
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2)
        loop.close()


def test_ws_transport_mints_unique_connection_identity():
    loop = asyncio.new_event_loop()
    try:
        first = ws_mod.WSTransport(object(), loop)
        second = ws_mod.WSTransport(object(), loop)

        uuid.UUID(hex=first.connection_id)
        uuid.UUID(hex=second.connection_id)
        assert first.connection_id != second.connection_id
        assert first.client_id is None
        assert second.client_id is None
    finally:
        loop.close()


def test_stdio_transport_has_stable_implicit_identity():
    from tui_gateway.transport import StdioTransport

    transport = StdioTransport(lambda: None, threading.Lock())

    assert transport.connection_id == "stdio"
    assert transport.client_id == "stdio"


def test_ws_transport_preserves_cross_batch_order():
    async def scenario():
        entered = []
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        second_started = asyncio.Event()

        class FakeWS:
            async def send_text(self, line):
                entered.append(line)
                if line == "A1":
                    first_entered.set()
                    await release_first.wait()

        transport = ws_mod.WSTransport(
            FakeWS(), asyncio.get_running_loop(), peer="batch-order-test"
        )
        first = asyncio.create_task(transport._safe_send_many(["A1", "A2"]))
        await first_entered.wait()

        async def send_second():
            second_started.set()
            await transport._safe_send_many(["B1", "B2"])

        second = asyncio.create_task(send_second())
        await second_started.wait()

        # The second task has reached the transport. Without whole-batch
        # serialization it runs B1/B2 before this task can resume.
        assert entered == ["A1"]

        release_first.set()
        await asyncio.gather(first, second)
        assert entered == ["A1", "A2", "B1", "B2"]

    asyncio.run(scenario())


def test_authenticated_asgi_websockets_share_one_durable_ordered_session(
    monkeypatch, tmp_path
):
    """Exercise controller/observer semantics through the real ``/api/ws`` route."""
    from fastapi.testclient import TestClient
    from hermes_cli import web_server


    run_prompts: list[str] = []
    durable_rows: list[dict] = []

    class Agent:
        model = "test-model"
        provider = "test-provider"
        base_url = "https://example.invalid"
        api_key = object()
        api_mode = "test"

        def run_conversation(
            self, prompt, conversation_history=None, stream_callback=None, **_kwargs
        ):
            run_prompts.append(prompt)
            durable_rows.append(
                {
                    "message_id": f"message-{len(run_prompts)}",
                    "text": "same visible text",
                }
            )
            self._on_user_message_persisted()
            stream_callback(f"reply-{len(run_prompts)}")
            return {
                "final_response": f"reply-{len(run_prompts)}",
                "messages": [
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": f"reply-{len(run_prompts)}"},
                ],
            }

    def receive_rpc(socket, request_id):
        events = []
        while True:
            frame = socket.receive_json()
            if frame.get("id") == request_id:
                return frame, events
            events.append(frame)

    def rpc(socket, request_id, method, params=None):
        socket.send_json(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params or {},
            }
        )
        return receive_rpc(socket, request_id)

    def receive_through(socket, event_type):
        events = []
        while True:
            frame = socket.receive_json()
            events.append(frame)
            if (frame.get("params") or {}).get("type") == event_type:
                return events

    def receive_until(socket, predicate):
        events = []
        while True:
            frame = socket.receive_json()
            events.append(frame)
            if predicate(frame):
                return events

    old_auth_required = getattr(web_server.app.state, "auth_required", False)
    previous_sessions = dict(server._sessions)
    server._sessions.clear()
    server._live_transports.clear()
    web_server.app.state.auth_required = False
    monkeypatch.setattr(web_server, "_DASHBOARD_EMBEDDED_CHAT_ENABLED", True)
    monkeypatch.setattr(web_server, "_ws_request_is_allowed", lambda _ws: True)
    monkeypatch.setattr(server, "_completion_cwd", lambda params=None: str(tmp_path))
    monkeypatch.setattr(server, "_schedule_session_cap_enforcement", lambda: None)
    monkeypatch.setattr(server, "_schedule_ws_orphan_reap", lambda _sid: None)
    monkeypatch.setattr(server, "_start_agent_build", lambda _sid, _session: None)

    monkeypatch.setattr(server, "make_stream_renderer", lambda _cols: None)
    monkeypatch.setattr(server, "render_message", lambda _raw, _cols: None)
    monkeypatch.setattr(server, "_get_db", lambda: None)

    url = f"/api/ws?token={web_server._SESSION_TOKEN}"
    client = TestClient(web_server.app)
    try:
        with client.websocket_connect(url) as controller_a:
            assert controller_a.receive_json()["params"]["type"] == "gateway.ready"
            assert "result" in rpc(
                controller_a,
                "client-a",
                "client.attach",
                {"client_id": "controller-a"},
            )[0]
            created, _ = rpc(controller_a, "create", "session.create")
            sid = created["result"]["session_id"]
            session = server._sessions[sid]
            session["agent"] = Agent()
            session["agent_ready"].set()

            with client.websocket_connect(url) as controller_c:
                assert controller_c.receive_json()["params"]["type"] == "gateway.ready"
                rpc(
                    controller_c,
                    "client-c",
                    "client.attach",
                    {"client_id": "controller-c"},
                )
                attached_c, _ = rpc(
                    controller_c,
                    "attach-c",
                    "session.attach",
                    {"session_id": sid, "mode": "control"},
                )
                assert attached_c["result"]["mode"] == "control"

                with client.websocket_connect(url) as observer_b:
                    assert observer_b.receive_json()["params"]["type"] == "gateway.ready"
                    rpc(
                        observer_b,
                        "client-b",
                        "client.attach",
                        {"client_id": "observer-b"},
                    )
                    attached_b, _ = rpc(
                        observer_b,
                        "attach-b",
                        "session.attach",
                        {"session_id": sid, "mode": "observe"},
                    )
                    assert attached_b["result"]["mode"] == "observe"

                    denied, _ = rpc(
                        observer_b,
                        "observer-submit",
                        "prompt.submit",
                        {"session_id": sid, "text": "must fail"},
                    )
                    assert denied["error"]["code"] == 4003

                    accepted, events_a = rpc(
                        controller_a,
                        "submit-1",
                        "prompt.submit",
                        {
                            "session_id": sid,
                            "text": "model-facing prompt",
                            "display_text": "same visible text",
                            "client_message_id": "message-1",
                            "submitted_at": 1000.0,
                        },
                    )
                    assert accepted["result"]["status"] == "streaming"
                    events_a.extend(receive_through(controller_a, "message.complete"))
                    events_a.extend(
                        receive_until(
                            controller_a,
                            lambda frame: (
                                (frame.get("params") or {}).get("type")
                                == "session.info"
                                and not (
                                    (frame.get("params") or {}).get("payload") or {}
                                ).get("running", True)
                            ),
                        )
                    )
                    events_c = receive_through(controller_c, "message.complete")
                    events_b = receive_through(observer_b, "message.user")
                    observer_seq = events_b[-1]["params"]["seq"]

                    for events in (events_a, events_c):
                        event_types = [frame["params"]["type"] for frame in events]
                        assert event_types.index("message.user") < event_types.index(
                            "message.delta"
                        )
                        assert event_types.index("message.delta") < event_types.index(
                            "message.complete"
                        )
                    assert events_b[-1]["params"]["payload"] == {
                        "message_id": "message-1",
                        "text": "same visible text",
                        "timestamp": 1000.0,
                    }

                with client.websocket_connect(url) as observer_b_reconnected:
                    assert (
                        observer_b_reconnected.receive_json()["params"]["type"]
                        == "gateway.ready"
                    )
                    rpc(
                        observer_b_reconnected,
                        "client-b-reconnect",
                        "client.attach",
                        {"client_id": "observer-b"},
                    )
                    replayed, _ = rpc(
                        observer_b_reconnected,
                        "attach-b-reconnect",
                        "session.attach",
                        {
                            "session_id": sid,
                            "mode": "observe",
                            "last_seen_seq": observer_seq,
                        },
                    )
                    replay = replayed["result"]["events"]
                    assert replay
                    assert all(event["seq"] > observer_seq for event in replay)
                    assert len({event["seq"] for event in replay}) == len(replay)
                    assert not any(
                        event["type"] == "message.user"
                        and event["payload"].get("message_id") == "message-1"
                        for event in replay
                    )

                duplicate, _ = rpc(
                    controller_a,
                    "retry-1",
                    "prompt.submit",
                    {
                        "session_id": sid,
                        "text": "model-facing prompt",
                        "display_text": "same visible text",
                        "client_message_id": "message-1",
                        "submitted_at": 1001.0,
                    },
                )
                assert duplicate["result"]["duplicate"] is True

                distinct, _ = rpc(
                    controller_a,
                    "submit-2",
                    "prompt.submit",
                    {
                        "session_id": sid,
                        "text": "model-facing prompt",
                        "display_text": "same visible text",
                        "client_message_id": "message-2",
                        "submitted_at": 1002.0,
                    },
                )
                assert distinct["result"]["status"] == "streaming"
                receive_through(controller_a, "message.complete")
                receive_through(controller_c, "message.complete")

                assert run_prompts == ["model-facing prompt", "model-facing prompt"]
                assert durable_rows == [
                    {"message_id": "message-1", "text": "same visible text"},
                    {"message_id": "message-2", "text": "same visible text"},
                ]
    finally:
        web_server.app.state.auth_required = old_auth_required
        for sid, session in list(server._sessions.items()):
            server._sessions.pop(sid, None)
            server._teardown_session(session)
        server._sessions.update(previous_sessions)
        server._live_transports.clear()
