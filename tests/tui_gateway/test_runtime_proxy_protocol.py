from __future__ import annotations

import io
import json
import multiprocessing
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from hermes_cli.live_runtime_owners import RuntimeOwner
from tui_gateway import runtime_proxy
from tui_gateway.session_events import SessionEventHub


def _run_cross_process_runtime_owner(registry_home: str, endpoint: str, ready) -> None:
    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=registry_home,
        endpoint=Path(endpoint),
        owner_id=f"owner-{os.getpid()}",
        surface="cross-process-test",
        dispatch=lambda request, _transport: {
            "jsonrpc": "2.0",
            "id": request.get("id"),
            "result": {"owner_pid": os.getpid()},
        },
    )
    if not coordinator.start():
        ready.put(RuntimeError("owner proxy failed to start"))
        return
    lease = coordinator.claim_local(
        conversation_key="durable-root",
        profile_home=registry_home,
    )
    ready.put(lease.owner)
    while True:
        time.sleep(1)


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(socket, "SO_PEERCRED"),
    reason="authenticated Unix runtime proxy is unavailable",
)
def test_cross_process_owner_proxy_crash_and_single_winner_takeover(tmp_path):
    from hermes_cli.live_runtime_owners import claim_runtime_owner

    context = multiprocessing.get_context("fork")
    ready = context.Queue()
    endpoint = tmp_path / "owner.sock"
    owner_process = context.Process(
        target=_run_cross_process_runtime_owner,
        args=(str(tmp_path), str(endpoint), ready),
    )
    owner_process.start()
    owner = ready.get(timeout=5)
    assert isinstance(owner, RuntimeOwner)

    client = runtime_proxy.RuntimeProxyClient(
        owner=owner,
        client_id="cross-process-client",
        on_event=lambda _frame: None,
    )
    try:
        client.connect()
        response = client.request({
            "jsonrpc": "2.0",
            "id": "cross-process-request",
            "method": "session.get",
            "params": {"session_id": "live-session"},
        })
        assert response["result"]["owner_pid"] == owner_process.pid
    finally:
        client.close()

    owner_process.terminate()
    owner_process.join(timeout=5)
    assert not owner_process.is_alive()

    barrier = threading.Barrier(3)
    claims = []

    def contend(index: int) -> None:
        barrier.wait()
        claims.append(
            claim_runtime_owner(
                conversation_key=owner.conversation_key,
                owner_id=f"takeover-{index}",
                endpoint=str(tmp_path / f"takeover-{index}.sock"),
                surface="cross-process-takeover",
                registry_home=tmp_path,
                profile_home=tmp_path,
            )
        )

    contenders = [threading.Thread(target=contend, args=(index,)) for index in range(2)]
    for contender in contenders:
        contender.start()
    barrier.wait()
    for contender in contenders:
        contender.join(timeout=5)
        assert not contender.is_alive()

    assert sorted(claim.kind for claim in claims) == ["owned", "remote"]
    winner = next(claim for claim in claims if claim.kind == "owned")
    assert winner.owner.generation == owner.generation + 1
    assert winner.lease is not None
    assert winner.lease.release() is True


def test_repeated_local_claim_reuses_lease_without_deleting_registry(tmp_path):
    from hermes_cli.live_runtime_owners import lookup_runtime_owner

    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "owner.sock",
        owner_id="same-owner",
        surface="test",
        dispatch=lambda request, _transport: request,
    )
    assert coordinator.start()
    try:
        first = coordinator.claim_local(
            conversation_key="durable-root", profile_home=tmp_path
        )
        second = coordinator.claim_local(
            conversation_key="durable-root", profile_home=tmp_path
        )

        assert second is first
        assert (
            lookup_runtime_owner(
                conversation_key=first.owner.conversation_key,
                registry_home=tmp_path,
            )
            == first.owner
        )
    finally:
        coordinator.stop()


def test_concurrent_local_claims_share_one_current_lease(tmp_path, monkeypatch):
    from hermes_cli.live_runtime_owners import lookup_runtime_owner

    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "owner.sock",
        owner_id="same-owner",
        surface="test",
        dispatch=lambda request, _transport: request,
    )
    real_claim = runtime_proxy.claim_runtime_owner
    rendezvous = threading.Event()
    registry_calls = 0
    calls_lock = threading.Lock()

    def synchronized_claim(**kwargs):
        nonlocal registry_calls
        result = real_claim(**kwargs)
        with calls_lock:
            registry_calls += 1
            if registry_calls == 2:
                rendezvous.set()
        rendezvous.wait(timeout=0.2)
        return result

    monkeypatch.setattr(runtime_proxy, "claim_runtime_owner", synchronized_claim)
    assert coordinator.start()
    leases: list[runtime_proxy._TrackedRuntimeOwnerLease] = []

    def claim() -> None:
        leases.append(
            coordinator.claim_local(
                conversation_key="durable-root", profile_home=tmp_path
            )
        )

    threads = [threading.Thread(target=claim) for _ in range(2)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)

        assert all(not thread.is_alive() for thread in threads)
        assert registry_calls == 1
        assert len(leases) == 2
        assert leases[0] is leases[1]
        assert leases[0].is_current()
        assert (
            lookup_runtime_owner(
                conversation_key=leases[0].owner.conversation_key,
                registry_home=tmp_path,
            )
            == leases[0].owner
        )
    finally:
        coordinator.stop()


def test_stop_serializes_with_inflight_local_claim(tmp_path, monkeypatch):
    from hermes_cli.live_runtime_owners import lookup_runtime_owner

    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "proxy.sock",
        owner_id="owner-a",
        surface="test",
        dispatch=lambda _request, _transport: None,
    )
    claimed = threading.Event()
    unblock = threading.Event()
    real_claim = runtime_proxy.claim_runtime_owner

    def delayed_claim(**kwargs):
        result = real_claim(**kwargs)
        claimed.set()
        assert unblock.wait(timeout=2)
        return result

    monkeypatch.setattr(runtime_proxy, "claim_runtime_owner", delayed_claim)
    returned = []
    claim_thread = threading.Thread(
        target=lambda: returned.append(
            coordinator.claim_local(
                conversation_key="conversation-1",
                profile_home=tmp_path,
            )
        )
    )
    claim_thread.start()
    assert claimed.wait(timeout=2)
    stop_thread = threading.Thread(target=coordinator.stop)
    stop_thread.start()
    unblock.set()
    claim_thread.join(timeout=2)
    stop_thread.join(timeout=2)

    owner_key = runtime_proxy._profile_scoped_conversation_key(
        "conversation-1", tmp_path
    )
    assert len(returned) == 1
    assert returned[0].released is True
    assert coordinator._local_leases == {}
    assert (
        lookup_runtime_owner(
            conversation_key=owner_key,
            registry_home=tmp_path,
        )
        is None
    )


def test_stop_serializes_with_inflight_resume_claim(tmp_path, monkeypatch):
    from hermes_cli.live_runtime_owners import lookup_runtime_owner

    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "proxy.sock",
        owner_id="owner-a",
        surface="test",
        dispatch=lambda _request, _transport: None,
    )
    claimed = threading.Event()
    unblock = threading.Event()
    real_claim = runtime_proxy.claim_runtime_owner

    def delayed_claim(**kwargs):
        result = real_claim(**kwargs)
        claimed.set()
        assert unblock.wait(timeout=2)
        return result

    monkeypatch.setattr(runtime_proxy, "claim_runtime_owner", delayed_claim)
    resumed = []
    resume_thread = threading.Thread(
        target=lambda: resumed.append(
            coordinator.prepare_resume(
                conversation_key="conversation-1",
                profile_home=tmp_path,
                request={
                    "jsonrpc": "2.0",
                    "id": "resume-1",
                    "method": "session.resume",
                },
                transport=type(
                    "Transport",
                    (),
                    {
                        "connection_id": "frontend",
                        "client_id": "client",
                        "auth_identity": "user",
                    },
                )(),
            )
        )
    )
    resume_thread.start()
    assert claimed.wait(timeout=2)
    stop_thread = threading.Thread(target=coordinator.stop)
    stop_thread.start()
    unblock.set()
    resume_thread.join(timeout=2)
    stop_thread.join(timeout=2)

    owner_key = runtime_proxy._profile_scoped_conversation_key(
        "conversation-1", tmp_path
    )
    assert resumed == [None]
    assert coordinator._local_leases == {}
    assert (
        lookup_runtime_owner(
            conversation_key=owner_key,
            registry_home=tmp_path,
        )
        is None
    )


def _owner(
    *, owner_id: str = "owner-a", generation: int = 3, endpoint: str = "/tmp/owner.sock"
) -> RuntimeOwner:
    return RuntimeOwner(
        conversation_key="root-session",
        owner_id=owner_id,
        generation=generation,
        pid=os.getpid(),
        process_start_time=None,
        endpoint=endpoint,
        profile_home="/tmp/profile",
        surface="test",
        started_at=1.0,
    )


def test_stable_routes_are_lru_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_proxy, "MAX_STABLE_RUNTIME_ROUTES", 2)
    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "owner.sock",
        owner_id="owner",
        surface="test",
        dispatch=lambda request, _transport: request,
    )
    coordinator._remember_stable_route(("client", "session-1"), _owner())
    coordinator._remember_stable_route(("client", "session-2"), _owner())
    assert coordinator._stable_route_owner(("client", "session-1")) is not None
    coordinator._remember_stable_route(("client", "session-3"), _owner())

    assert list(coordinator._stable_routes) == [
        ("client", "session-1"),
        ("client", "session-3"),
    ]


def test_handshake_rejects_owner_id_or_generation_mismatch():
    owner = _owner()
    for payload in (
        runtime_proxy.handshake_frame(_owner(owner_id="wrong"), client_id="client-a"),
        runtime_proxy.handshake_frame(_owner(generation=4), client_id="client-a"),
    ):
        with pytest.raises(
            runtime_proxy.RuntimeProxyProtocolError, match="owner identity mismatch"
        ):
            runtime_proxy.validate_handshake(
                payload, expected_owner=owner, current_owner=owner
            )


def test_handshake_rejects_unattested_auth_identity():
    owner = _owner()
    payload = runtime_proxy.handshake_frame(owner, client_id="client-a")
    payload["auth_identity"] = {"subject": "forged-admin"}

    with pytest.raises(
        runtime_proxy.RuntimeProxyProtocolError, match="unattested auth identity"
    ):
        runtime_proxy.validate_handshake(
            payload,
            expected_owner=owner,
            current_owner=owner,
        )


def test_handshake_rejects_unknown_negotiated_capability():
    owner = _owner()
    payload = runtime_proxy.handshake_frame(owner, client_id="client-a")
    payload["negotiated_capabilities"] = ["observe", "runtime.admin"]

    with pytest.raises(
        runtime_proxy.RuntimeProxyProtocolError,
        match="invalid negotiated capabilities",
    ):
        runtime_proxy.validate_handshake(
            payload,
            expected_owner=owner,
            current_owner=owner,
        )


def test_handshake_preserves_legacy_absence_separately_from_empty_capabilities():
    owner = _owner()

    legacy = runtime_proxy.handshake_frame(owner, client_id="legacy-client")
    explicit_empty = runtime_proxy.handshake_frame(
        owner, client_id="restricted-client", negotiated_capabilities=frozenset()
    )

    assert runtime_proxy.validate_handshake(
        legacy, expected_owner=owner, current_owner=owner
    ) == ("legacy-client", None)
    assert runtime_proxy.validate_handshake(
        explicit_empty, expected_owner=owner, current_owner=owner
    ) == ("restricted-client", frozenset())


def test_handshake_rejects_claim_no_longer_current():
    expected = _owner()
    payload = runtime_proxy.handshake_frame(expected, client_id="client-a")

    with pytest.raises(
        runtime_proxy.RuntimeProxyProtocolError, match="claim is no longer current"
    ):
        runtime_proxy.validate_handshake(
            payload,
            expected_owner=expected,
            current_owner=_owner(generation=4),
        )


def test_protocol_round_trips_megabyte_resume_payload():
    frame = {
        "kind": "rpc.response",
        "frame": {
            "jsonrpc": "2.0",
            "id": "large-resume",
            "result": {
                "messages": [{"role": "assistant", "content": "x" * (1024 * 1024)}]
            },
        },
    }

    encoded = runtime_proxy.encode_frame(frame)

    assert runtime_proxy.read_frame(io.BytesIO(encoded)) == frame


def test_protocol_rejects_oversized_or_malformed_frame_without_dispatch():
    oversized = io.BytesIO(b"x" * (runtime_proxy.MAX_FRAME_BYTES + 1) + b"\n")
    malformed = io.BytesIO(b"not-json\n")

    with pytest.raises(runtime_proxy.RuntimeProxyProtocolError, match="too large"):
        runtime_proxy.read_frame(oversized)
    with pytest.raises(runtime_proxy.RuntimeProxyProtocolError, match="malformed"):
        runtime_proxy.read_frame(malformed)


def test_posix_peer_uid_is_verified_before_rpc():
    if not hasattr(socket, "SO_PEERCRED"):
        pytest.skip("SO_PEERCRED is unavailable")
    left, right = socket.socketpair()
    try:
        assert runtime_proxy.require_posix_peer_uid(left) == os.getuid()
        with pytest.raises(
            runtime_proxy.RuntimeProxyProtocolError, match="uid mismatch"
        ):
            runtime_proxy.require_posix_peer_uid(left, expected_uid=os.getuid() + 1)
    finally:
        left.close()
        right.close()


def test_proxy_principal_is_derived_from_kernel_attested_process():
    first = runtime_proxy.proxy_peer_auth_identity(peer_pid=101, peer_uid=1000)
    second = runtime_proxy.proxy_peer_auth_identity(peer_pid=202, peer_uid=1000)

    assert first == {
        "provider": "runtime-proxy-peer",
        "peer_pid": 101,
        "peer_uid": 1000,
    }
    assert first != second


def test_proxy_client_id_is_scoped_to_locally_authenticated_principal():
    class Transport:
        client_id = "same-browser-id"
        connection_id = "connection"

        def __init__(self, user_id: str) -> None:
            self.auth_identity = {"provider": "oidc", "user_id": user_id}

    alice = runtime_proxy.proxy_scoped_client_id(Transport("alice"))
    alice_reconnect = runtime_proxy.proxy_scoped_client_id(Transport("alice"))
    bob = runtime_proxy.proxy_scoped_client_id(Transport("bob"))

    assert alice == alice_reconnect
    assert alice != bob
    assert "same-browser-id" not in alice
    assert "alice" not in alice


def test_proxy_transport_can_acknowledge_physical_sender_completion():
    entered = threading.Event()
    release = threading.Event()
    completed = threading.Event()

    def blocked_send(_frame):
        entered.set()
        release.wait(timeout=2)
        return True

    transport = runtime_proxy.ProxyTransport(
        client_id="ordered-client", send=blocked_send
    )

    def write() -> None:
        assert transport.write_and_wait({"jsonrpc": "2.0", "method": "event"}) is True
        completed.set()

    try:
        worker = threading.Thread(target=write)
        worker.start()
        assert entered.wait(timeout=1)
        assert not completed.is_set()
        release.set()
        assert completed.wait(timeout=1)
        worker.join(timeout=1)
        assert not worker.is_alive()
    finally:
        release.set()
        transport.close()


@pytest.mark.parametrize("resolution_type", ["approval.resolved", "clarify.resolved"])
def test_resolution_event_bytes_precede_rpc_response_over_proxy(resolution_type):
    sender, receiver = socket.socketpair()
    stream = receiver.makefile("rb", buffering=0)

    def send(envelope):
        sender.sendall(runtime_proxy.encode_frame(envelope))
        return True

    transport = runtime_proxy.ProxyTransport(client_id="ordered-client", send=send)
    hub = SessionEventHub()
    hub.attach(transport, client_id="ordered-client", mode="control")
    try:
        assert hub.publish(
            {
                "jsonrpc": "2.0",
                "method": "event",
                "params": {"type": resolution_type, "session_id": "ordered-session"},
            },
            wait_for_delivery=True,
        )
        sender.sendall(
            runtime_proxy.encode_frame({
                "kind": "rpc.response",
                "frame": {
                    "jsonrpc": "2.0",
                    "id": "response",
                    "result": {"status": "ok"},
                },
            })
        )

        first = runtime_proxy.read_frame(stream)
        second = runtime_proxy.read_frame(stream)
        assert first["kind"] == "event"
        assert first["frame"]["params"]["type"] == resolution_type
        assert second["kind"] == "rpc.response"
    finally:
        hub.close()
        transport.close()
        stream.close()
        receiver.close()
        sender.close()


def test_proxy_transport_fails_closed_when_slow_sender_fills_bounded_queue():
    sending = threading.Event()
    release = threading.Event()

    def blocked_send(_frame):
        sending.set()
        release.wait(timeout=2)
        return True

    transport = runtime_proxy.ProxyTransport(
        client_id="slow-client",
        send=blocked_send,
        outbound_queue_size=1,
    )
    frame = {"jsonrpc": "2.0", "method": "event", "params": {"seq": 1}}
    try:
        started = time.monotonic()
        assert transport.write(frame) is True
        assert time.monotonic() - started < 0.1
        assert sending.wait(timeout=1)
        assert transport.write(frame) is True
        assert transport.write(frame) is False
        assert transport.closed is True
    finally:
        release.set()
        transport.close()


def test_proxy_responses_are_point_to_point_and_events_are_not_restamped():
    sent_a = []
    sent_b = []
    transport_a = runtime_proxy.ProxyTransport(
        client_id="client-a",
        send=sent_a.append,
        auth_identity={"subject": "user-a"},
    )
    transport_b = runtime_proxy.ProxyTransport(
        client_id="client-b",
        send=sent_b.append,
        auth_identity={"subject": "user-b"},
    )
    response = {"jsonrpc": "2.0", "id": "request-a", "result": {"ok": True}}
    event = {
        "jsonrpc": "2.0",
        "method": "event",
        "params": {
            "type": "message.delta",
            "session_id": "live-session",
            "seq": 17,
            "epoch": "owner-epoch",
            "payload": {"text": "hello"},
        },
    }

    assert transport_a.write(response) is True
    assert sent_b == []
    deadline = time.monotonic() + 1
    while not sent_a and time.monotonic() < deadline:
        time.sleep(0.001)
    assert sent_a == [{"kind": "rpc.response", "frame": response}]

    assert transport_b.write(event) is True
    deadline = time.monotonic() + 1
    while not sent_b and time.monotonic() < deadline:
        time.sleep(0.001)
    assert sent_b == [{"kind": "event", "frame": event}]
    assert sent_b[0]["frame"] is event
    assert transport_a.connection_id != transport_b.connection_id


def test_unix_proxy_routes_response_and_preserves_async_event(tmp_path):
    if os.name == "nt":
        pytest.skip("Unix socket integration")
    endpoint = tmp_path / "runtime.sock"
    owner = _owner(endpoint=str(endpoint))
    received_event = threading.Event()
    events = []

    def dispatch(request, transport):
        assert transport.auth_identity == {
            "provider": "runtime-proxy-peer",
            "peer_pid": os.getpid(),
            "peer_uid": os.getuid(),
        }
        assert transport.negotiated_capabilities == frozenset({"observe"})
        transport.write({
            "jsonrpc": "2.0",
            "method": "event",
            "params": {
                "type": "message.delta",
                "session_id": "live-session",
                "seq": 8,
                "epoch": "owner-epoch",
            },
        })
        return {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {"owner": owner.owner_id},
        }

    server = runtime_proxy.RuntimeProxyServer(
        endpoint=endpoint,
        owner_lookup=lambda key: owner if key == owner.conversation_key else None,
        dispatch=dispatch,
    )
    assert server.start() is True
    client = runtime_proxy.RuntimeProxyClient(
        owner=owner,
        client_id="remote-client",
        negotiated_capabilities=frozenset({"observe"}),
        on_event=lambda frame: (events.append(frame), received_event.set()),
    )
    try:
        client.connect()
        response = client.request({
            "jsonrpc": "2.0",
            "id": "rpc-1",
            "method": "session.attach",
        })
        assert response == {
            "jsonrpc": "2.0",
            "id": "rpc-1",
            "result": {"owner": "owner-a"},
        }
        assert received_event.wait(timeout=2)
        assert events[0]["params"]["seq"] == 8
        assert events[0]["params"]["epoch"] == "owner-epoch"
    finally:
        client.close()
        server.stop()


def test_concurrent_proxy_connect_installs_one_client_and_ignores_loser_disconnect(
    monkeypatch, tmp_path
):
    owner = _owner(endpoint=str(tmp_path / "remote.sock"))
    connect_barrier = threading.Barrier(2)
    candidates = []

    class Claim:
        kind = "remote"
        lease = None

        def __init__(self):
            self.owner = owner

    class FakeClient:
        def __init__(self, *, on_disconnect, **_kwargs):
            self._on_disconnect = on_disconnect
            self.closed = False
            candidates.append(self)

        def connect(self):
            connect_barrier.wait(timeout=2)

        def request(self, frame):
            return {
                "jsonrpc": "2.0",
                "id": frame["id"],
                "result": {"session_id": "live-session"},
            }

        def close(self):
            self.closed = True

    class Transport:
        connection_id = "frontend-connection"
        client_id = "stable-client"
        auth_identity = None

        def __init__(self):
            self.writes = []

        def write(self, frame):
            self.writes.append(frame)
            return True

    monkeypatch.setattr(runtime_proxy, "claim_runtime_owner", lambda **_kwargs: Claim())
    monkeypatch.setattr(runtime_proxy, "RuntimeProxyClient", FakeClient)
    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "local.sock",
        owner_id="local-owner",
        surface="test",
        dispatch=lambda _request, _transport: None,
    )
    transport = Transport()
    responses = []

    def prepare(request_id):
        responses.append(
            coordinator.prepare_resume(
                conversation_key=owner.conversation_key,
                request={
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "session.resume",
                },
                transport=transport,
                profile_home=tmp_path,
            )
        )

    first = threading.Thread(target=prepare, args=("r1",))
    second = threading.Thread(target=prepare, args=("r2",))
    first.start()
    second.start()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert len(responses) == 2
    assert len(candidates) == 2
    assert sum(candidate.closed for candidate in candidates) == 1
    assert len(coordinator._clients) == 1
    winner = next(iter(coordinator._clients.values()))
    loser = next(candidate for candidate in candidates if candidate is not winner)
    loser._on_disconnect()
    assert next(iter(coordinator._clients.values())) is winner
    assert coordinator.has_remote_route(transport, "live-session") is True
    assert transport.writes == []


def test_runtime_owner_keys_are_profile_scoped(tmp_path):
    coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path / "registry",
        endpoint=tmp_path / "runtime.sock",
        owner_id="local-owner",
        surface="test",
        dispatch=lambda _request, _transport: None,
    )
    profile_a = tmp_path / "profiles" / "a"
    profile_b = tmp_path / "profiles" / "b"

    lease_a = coordinator.claim_local(
        conversation_key="shared-root", profile_home=profile_a
    )
    lease_b = coordinator.claim_local(
        conversation_key="shared-root", profile_home=profile_b
    )

    assert lease_a.owner.conversation_key != lease_b.owner.conversation_key
    assert lease_a.owner.profile_home == str(profile_a.resolve())
    assert lease_b.owner.profile_home == str(profile_b.resolve())
    key_a = runtime_proxy._profile_scoped_conversation_key("shared-root", profile_a)
    assert key_a == runtime_proxy._profile_scoped_conversation_key(
        "shared-root", profile_a / "."
    )
    assert lease_a.release() is True
    assert lease_b.release() is True


def test_proxy_timeout_returns_unknown_non_retryable_and_fences_connection(tmp_path):
    endpoint = tmp_path / "runtime.sock"
    owner = _owner(endpoint=str(endpoint))
    release = threading.Event()
    disconnected = threading.Event()
    dispatch_calls = []

    def dispatch(request, _transport):
        dispatch_calls.append(request["id"])
        release.wait(timeout=2)
        return {"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}}

    server = runtime_proxy.RuntimeProxyServer(
        endpoint=endpoint,
        owner_lookup=lambda _key: owner,
        dispatch=dispatch,
    )
    client = runtime_proxy.RuntimeProxyClient(
        owner=owner,
        client_id="timeout-client",
        on_event=lambda _frame: None,
        on_disconnect=disconnected.set,
    )
    try:
        assert server.start() is True
        client.connect()
        response = client.request(
            {"jsonrpc": "2.0", "id": "slow-mutation", "method": "session.compress"},
            timeout=0.05,
        )
        assert response["id"] == "slow-mutation"
        assert response["error"]["code"] == -32072
        assert response["error"]["data"]["outcome"] == "unknown"
        assert response["error"]["data"]["retryable"] is False
        assert client._closed is True
        assert disconnected.wait(timeout=1)
        assert dispatch_calls == ["slow-mutation"]
    finally:
        release.set()
        client.close()
        server.stop()


def test_stale_owner_generation_cannot_publish_async_event(tmp_path):
    endpoint = tmp_path / "runtime.sock"
    owner = _owner(endpoint=str(endpoint))
    current = [owner]
    captured = []
    lost = threading.Event()

    def dispatch(request, transport):
        captured.append(transport)
        return {"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}}

    server = runtime_proxy.RuntimeProxyServer(
        endpoint=endpoint,
        owner_lookup=lambda _key: current[0],
        dispatch=dispatch,
    )
    client = runtime_proxy.RuntimeProxyClient(
        owner=owner,
        client_id="client-a",
        on_event=lambda _frame: None,
        on_disconnect=lost.set,
    )
    try:
        assert server.start() is True
        client.connect()
        assert client.request({"jsonrpc": "2.0", "id": 1, "method": "session.resume"})[
            "result"
        ] == {"ok": True}
        current[0] = _owner(generation=owner.generation + 1, endpoint=str(endpoint))

        assert (
            captured[0].write({
                "jsonrpc": "2.0",
                "method": "event",
                "params": {"seq": 8},
            })
            is True
        )
        assert lost.wait(timeout=2)
        assert captured[0].closed is True
        assert (
            captured[0].write({
                "jsonrpc": "2.0",
                "method": "event",
                "params": {"seq": 9},
            })
            is False
        )
    finally:
        client.close()
        server.stop()


def test_owner_loss_fails_pending_request_without_retry_and_notifies_once(tmp_path):
    owner = _owner(endpoint=str(tmp_path / "owner.sock"))
    lost = []
    client = runtime_proxy.RuntimeProxyClient(
        owner=owner,
        client_id="stable-client",
        on_event=lambda _frame: None,
        on_disconnect=lambda: lost.append(True),
    )
    local, remote = socket.socketpair()
    client._socket = local
    client._stream = local.makefile("rwb", buffering=0)
    client._reader_thread = threading.Thread(target=client._read_loop, daemon=True)
    client._reader_thread.start()
    response = []

    requester = threading.Thread(
        target=lambda: response.append(
            client.request({
                "jsonrpc": "2.0",
                "id": "pending",
                "method": "session.interrupt",
                "params": {"session_id": "live"},
            })
        )
    )
    requester.start()
    runtime_proxy.read_frame(remote.makefile("rb", buffering=0))
    remote.close()
    requester.join(timeout=2)
    client._reader_thread.join(timeout=2)

    assert response[0]["error"]["code"] == -32072
    assert response[0]["error"]["data"]["outcome"] == "unknown"
    assert response[0]["error"]["data"]["retryable"] is False
    assert lost == [True]
    client.close()


def test_coordinator_routes_remote_resume_without_local_dispatch(tmp_path):
    if os.name == "nt":
        pytest.skip("Unix socket integration")
    owner_calls = []
    local_calls = []

    def owner_dispatch(request, _transport):
        owner_calls.append(request)
        return {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {"session_id": "canonical-live", "resumed": "stored-session"},
        }

    owner_coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "owner.sock",
        owner_id="owner-process",
        surface="test",
        dispatch=owner_dispatch,
    )
    remote_coordinator = runtime_proxy.RuntimeProxyCoordinator(
        registry_home=tmp_path,
        endpoint=tmp_path / "remote.sock",
        owner_id="remote-process",
        surface="test",
        dispatch=lambda request, transport: local_calls.append(request),
    )
    owner_transport = runtime_proxy.ProxyTransport(
        client_id="owner-ui", send=lambda frame: True
    )
    remote_frames = []

    class FrontendTransport:
        client_id = "remote-ui"

        def __init__(self, connection_id, auth_identity):
            self.connection_id = connection_id
            self.auth_identity = auth_identity

        def write(self, frame):
            remote_frames.append(frame)
            return True

    principal = {"provider": "oidc", "user_id": "user-a"}
    remote_transport = FrontendTransport("remote-connection", principal)
    request = {
        "jsonrpc": "2.0",
        "id": "resume-1",
        "method": "session.resume",
        "params": {"session_id": "stored-session"},
    }
    try:
        assert owner_coordinator.start() is True
        assert remote_coordinator.start() is True
        assert (
            owner_coordinator.prepare_resume(
                conversation_key="conversation-root",
                request=request,
                transport=owner_transport,
                profile_home=tmp_path,
            )
            is None
        )
        response = remote_coordinator.prepare_resume(
            conversation_key="conversation-root",
            request=request,
            transport=remote_transport,
            profile_home=tmp_path,
        )

        assert response["result"]["session_id"] == "canonical-live"
        assert owner_calls == [request]
        assert local_calls == []
        assert remote_coordinator.has_remote_route(remote_transport, "canonical-live")

        remote_coordinator.detach_transport(remote_transport)
        reconnected = FrontendTransport("reconnected-physical", principal)
        different_principal = FrontendTransport(
            "other-principal", {"provider": "oidc", "user_id": "user-b"}
        )
        attach = {
            "jsonrpc": "2.0",
            "id": "attach-1",
            "method": "session.attach",
            "params": {"session_id": "canonical-live", "mode": "control"},
        }
        assert remote_coordinator.has_remote_route(reconnected, "canonical-live")
        assert not remote_coordinator.has_remote_route(
            different_principal, "canonical-live"
        )
        assert remote_coordinator.route_request(attach, reconnected)["id"] == "attach-1"
        assert owner_calls == [request, attach]
        assert local_calls == []

        owner_coordinator.stop()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not remote_frames:
            time.sleep(0.01)
        assert (
            remote_coordinator.has_remote_route(reconnected, "canonical-live") is False
        )
        assert remote_frames == [
            {
                "jsonrpc": "2.0",
                "method": "event",
                "params": {
                    "type": "session.runtime_owner_lost",
                    "payload": {
                        "session_ids": ["canonical-live"],
                        "owner_id": "owner-process",
                        "generation": 1,
                        "outcome": "unknown",
                    },
                },
            }
        ]
    finally:
        remote_coordinator.stop()
        owner_coordinator.stop()


def test_encoded_registry_handshake_contains_no_credentials():
    encoded = json.dumps(
        runtime_proxy.handshake_frame(_owner(), client_id="client-a")
    ).lower()
    assert "secret" not in encoded
    assert "token" not in encoded
