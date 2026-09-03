"""Authenticated local wire primitives for canonical runtime proxying."""

from __future__ import annotations

import hashlib
import json
import os
import queue
import socket
import stat
import struct
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable

from hermes_cli.live_runtime_owners import (
    RuntimeOwner,
    RuntimeOwnerLease,
    assert_runtime_owner,
    claim_runtime_owner,
    lookup_runtime_owner,
)
from hermes_cli.live_runtime_protocol import (
    LEGACY_PROXY_CAPABILITIES,
    LIVE_RUNTIME_PROTOCOL_VERSION,
    LiveRuntimeProtocolError,
    validate_capabilities,
    validate_client_id,
)

RUNTIME_PROXY_PROTOCOL_VERSION = LIVE_RUNTIME_PROTOCOL_VERSION
# Resume transcripts and tool results can legitimately exceed the old 512 KiB
# ceiling. Keep the authenticated local transport bounded, but align it with a
# practical websocket/message ceiling rather than rejecting normal sessions.
MAX_FRAME_BYTES = 16 * 1024 * 1024
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30 * 60
MAX_STABLE_RUNTIME_ROUTES = 4096
DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 2.0
DEFAULT_MAX_CONCURRENT_CONNECTIONS = 64


def _profile_scoped_conversation_key(
    conversation_key: str, profile_home: str | Path
) -> str:
    profile = str(Path(profile_home).expanduser().resolve())
    material = f"{profile}\0{conversation_key}".encode("utf-8")
    return f"v1:{hashlib.sha256(material).hexdigest()}"


def _stable_route_identity(transport: Any) -> str:
    client_id = str(
        getattr(transport, "client_id", None)
        or getattr(transport, "connection_id", id(transport))
    )
    auth_identity = getattr(transport, "auth_identity", None)
    if auth_identity is None:
        return f"{client_id}:legacy"
    encoded = json.dumps(
        auth_identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return f"{client_id}:{hashlib.sha256(encoded).hexdigest()}"


def proxy_scoped_client_id(transport: Any) -> str:
    """Return an opaque owner-visible id scoped to the validated local principal."""
    material = ("runtime-proxy-client-v1\0" + _stable_route_identity(transport)).encode(
        "utf-8"
    )
    return f"proxy-v1:{hashlib.sha256(material).hexdigest()}"


def _owner_unavailable_response(owner: RuntimeOwner, request_id: Any) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": -32072,
            "message": "canonical runtime owner unavailable",
            "data": {
                "outcome": "unknown",
                "retryable": False,
                "owner_id": owner.owner_id,
                "generation": owner.generation,
            },
        },
    }


class RuntimeProxyProtocolError(RuntimeError):
    """A local proxy peer violated identity or framing requirements."""


def proxy_peer_auth_identity(*, peer_pid: int, peer_uid: int) -> dict[str, Any]:
    """Build an owner-attested principal from kernel-reported peer credentials."""
    if peer_pid <= 0 or peer_uid < 0:
        raise RuntimeProxyProtocolError("invalid runtime proxy peer credentials")
    return {
        "provider": "runtime-proxy-peer",
        "peer_pid": peer_pid,
        "peer_uid": peer_uid,
    }


class ProxyTransport:
    """Owner-side virtual transport for one authenticated remote client."""

    def __init__(
        self,
        *,
        client_id: str,
        send: Callable[[dict[str, Any]], Any],
        auth_identity: dict[str, Any] | None = None,
        negotiated_capabilities: frozenset[str] | None = None,
        outbound_queue_size: int = 256,
    ) -> None:
        if not isinstance(client_id, str) or not client_id.strip():
            raise ValueError("client_id must be a non-empty string")
        if outbound_queue_size < 1:
            raise ValueError("outbound_queue_size must be positive")
        self.connection_id = uuid.uuid4().hex
        self.client_id = client_id
        self.auth_identity = auth_identity
        self.negotiated_capabilities = (
            frozenset(negotiated_capabilities)
            if negotiated_capabilities is not None
            else None
        )
        self._send = send
        self._outbound: queue.Queue[
            tuple[dict[str, Any], threading.Event | None, list[bool]] | None
        ] = queue.Queue(maxsize=outbound_queue_size)
        self._state_lock = threading.Lock()
        self._closed = False
        self._writer = threading.Thread(target=self._write_loop, daemon=True)
        self._writer.start()

    @property
    def closed(self) -> bool:
        with self._state_lock:
            return self._closed

    def write(self, frame: dict[str, Any]) -> bool:
        kind = "event" if frame.get("method") == "event" else "rpc.response"
        overflow = False
        with self._state_lock:
            if self._closed:
                return False
            try:
                self._outbound.put_nowait(({"kind": kind, "frame": frame}, None, []))
            except queue.Full:
                self._closed = True
                overflow = True
        if overflow:
            self._fail_pending_writes()
            return False
        return True

    def write_and_wait(self, frame: dict[str, Any]) -> bool:
        """Write a frame and wait until the proxy sender has completed it."""
        kind = "event" if frame.get("method") == "event" else "rpc.response"
        completed = threading.Event()
        outcome: list[bool] = []
        overflow = False
        with self._state_lock:
            if self._closed:
                return False
            try:
                self._outbound.put_nowait((
                    {"kind": kind, "frame": frame},
                    completed,
                    outcome,
                ))
            except queue.Full:
                self._closed = True
                overflow = True
        if overflow:
            self._fail_pending_writes()
            return False
        if not completed.wait(timeout=11.0):
            with self._state_lock:
                self._closed = True
            self._fail_pending_writes()
            return False
        return bool(outcome and outcome[0])

    @staticmethod
    def _fail_write(
        item: tuple[dict[str, Any], threading.Event | None, list[bool]] | None,
    ) -> None:
        if item is None:
            return
        _envelope, completed, outcome = item
        if completed is not None:
            outcome.append(False)
            completed.set()

    def _fail_pending_writes(self) -> None:
        while True:
            try:
                item = self._outbound.get_nowait()
            except queue.Empty:
                return
            self._fail_write(item)

    def _write_loop(self) -> None:
        while True:
            item = self._outbound.get()
            if item is None:
                return
            envelope, completed, outcome = item
            if self._closed:
                self._fail_write(item)
                self._fail_pending_writes()
                return
            try:
                result = self._send(envelope)
            except Exception:
                self._closed = True
                self._fail_write(item)
                self._fail_pending_writes()
                return
            if result is False:
                self._closed = True
                self._fail_write(item)
                self._fail_pending_writes()
                return
            if completed is not None:
                outcome.append(True)
                completed.set()

    def close(self) -> None:
        with self._state_lock:
            self._closed = True
            self._fail_pending_writes()
            try:
                self._outbound.put_nowait(None)
            except queue.Full:
                pass


def handshake_frame(
    owner: RuntimeOwner,
    *,
    client_id: str,
    negotiated_capabilities: frozenset[str] | None = None,
) -> dict[str, Any]:
    try:
        validate_client_id(client_id)
    except LiveRuntimeProtocolError as exc:
        raise ValueError("client_id must be a non-empty string") from exc
    frame: dict[str, Any] = {
        "kind": "hello",
        "protocol": RUNTIME_PROXY_PROTOCOL_VERSION,
        "conversation_key": owner.conversation_key,
        "owner_id": owner.owner_id,
        "generation": owner.generation,
        "client_id": client_id,
        "negotiated_capabilities": (
            sorted(negotiated_capabilities)
            if negotiated_capabilities is not None
            else None
        ),
    }
    return frame


def validate_handshake(
    frame: Any,
    *,
    expected_owner: RuntimeOwner,
    current_owner: RuntimeOwner | None,
) -> tuple[str, frozenset[str] | None]:
    if not isinstance(frame, dict) or frame.get("kind") != "hello":
        raise RuntimeProxyProtocolError("invalid runtime proxy handshake")
    if frame.get("protocol") != RUNTIME_PROXY_PROTOCOL_VERSION:
        raise RuntimeProxyProtocolError("unsupported runtime proxy protocol")
    if "auth_identity" in frame:
        raise RuntimeProxyProtocolError("unattested auth identity is forbidden")
    claimed = (
        frame.get("conversation_key"),
        frame.get("owner_id"),
        frame.get("generation"),
    )
    expected = (
        expected_owner.conversation_key,
        expected_owner.owner_id,
        expected_owner.generation,
    )
    if claimed != expected:
        raise RuntimeProxyProtocolError("runtime owner identity mismatch")
    if current_owner != expected_owner:
        raise RuntimeProxyProtocolError("runtime owner claim is no longer current")
    client_id = frame.get("client_id")
    if not isinstance(client_id, str) or not client_id.strip():
        raise RuntimeProxyProtocolError("invalid runtime proxy client identity")
    raw_capabilities = frame.get("negotiated_capabilities")
    if raw_capabilities is None:
        return client_id, None
    if not isinstance(raw_capabilities, list):
        raise RuntimeProxyProtocolError("invalid negotiated capabilities")
    try:
        return client_id, validate_capabilities(
            raw_capabilities,
            supported=LEGACY_PROXY_CAPABILITIES,
        )
    except LiveRuntimeProtocolError as exc:
        raise RuntimeProxyProtocolError("invalid negotiated capabilities") from exc


def encode_frame(frame: dict[str, Any]) -> bytes:
    try:
        encoded = json.dumps(frame, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeProxyProtocolError(
            "runtime proxy frame is not serializable"
        ) from exc
    if len(encoded) > MAX_FRAME_BYTES:
        raise RuntimeProxyProtocolError("runtime proxy frame is too large")
    return encoded + b"\n"


def read_frame(stream: Any) -> dict[str, Any]:
    raw = stream.readline(MAX_FRAME_BYTES + 2)
    if not raw:
        raise EOFError("runtime proxy peer closed")
    if len(raw) > MAX_FRAME_BYTES + 1 or not raw.endswith(b"\n"):
        raise RuntimeProxyProtocolError("runtime proxy frame is too large")
    try:
        frame = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeProxyProtocolError("runtime proxy frame is malformed") from exc
    if not isinstance(frame, dict):
        raise RuntimeProxyProtocolError("runtime proxy frame is malformed")
    return frame


def read_frame_before_deadline(
    connection: socket.socket, *, timeout_seconds: float
) -> dict[str, Any]:
    """Read exactly one frame before a wall-clock deadline.

    ``socket.settimeout`` alone is an idle timeout: a peer can retain the
    connection forever by dripping bytes. Peeking lets us consume through the
    first newline without stealing bytes from the next RPC frame.
    """
    deadline = time.monotonic() + timeout_seconds
    raw = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("runtime proxy handshake timed out")
        connection.settimeout(remaining)
        chunk = connection.recv(
            min(64 * 1024, MAX_FRAME_BYTES + 2 - len(raw)), socket.MSG_PEEK
        )
        if not chunk:
            raise EOFError("runtime proxy peer closed")
        newline = chunk.find(b"\n")
        take = newline + 1 if newline >= 0 else len(chunk)
        raw.extend(connection.recv(take))
        if newline >= 0:
            break
        if len(raw) >= MAX_FRAME_BYTES + 2:
            raise RuntimeProxyProtocolError("runtime proxy frame is too large")

    if len(raw) > MAX_FRAME_BYTES + 1:
        raise RuntimeProxyProtocolError("runtime proxy frame is too large")
    try:
        frame = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeProxyProtocolError("runtime proxy frame is malformed") from exc
    if not isinstance(frame, dict):
        raise RuntimeProxyProtocolError("runtime proxy frame is malformed")
    return frame


class RuntimeProxyServer:
    """Persistent authenticated Unix-socket owner endpoint."""

    def __init__(
        self,
        *,
        endpoint: str | Path,
        owner_lookup: Callable[[str], RuntimeOwner | None],
        dispatch: Callable[[dict[str, Any], ProxyTransport], dict[str, Any] | None],
        detach: Callable[[ProxyTransport], None] | None = None,
        handshake_timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
        max_concurrent_connections: int = DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    ) -> None:
        if handshake_timeout_seconds <= 0:
            raise ValueError("handshake_timeout_seconds must be positive")
        if max_concurrent_connections < 1:
            raise ValueError("max_concurrent_connections must be positive")
        self.endpoint = Path(endpoint)
        self._owner_lookup = owner_lookup
        self._dispatch = dispatch
        self._detach = detach
        self._listener: socket.socket | None = None
        self._stopping = threading.Event()
        self._accept_thread: threading.Thread | None = None
        self._connections_lock = threading.Lock()
        self._connections: set[socket.socket] = set()
        self._handshake_timeout_seconds = handshake_timeout_seconds
        self._max_concurrent_connections = max_concurrent_connections
        self._connection_slots = threading.BoundedSemaphore(max_concurrent_connections)

    def start(self) -> bool:
        if os.name == "nt":
            raise RuntimeProxyProtocolError(
                "Windows runtime proxy transport is not implemented"
            )
        self.endpoint.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.endpoint.unlink(missing_ok=True)
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            old_umask = os.umask(0o177)
            try:
                listener.bind(str(self.endpoint))
            finally:
                os.umask(old_umask)
            os.chmod(self.endpoint, 0o600)
            listener.listen(self._max_concurrent_connections)
            listener.settimeout(0.2)
        except OSError:
            try:
                listener.close()
            except (NameError, OSError):
                pass
            return False
        self._listener = listener
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()
        return True

    def stop(self) -> None:
        self._stopping.set()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
            self._listener = None
        with self._connections_lock:
            connections = list(self._connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.close()
            except OSError:
                pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2)
            self._accept_thread = None
        try:
            self.endpoint.unlink(missing_ok=True)
        except OSError:
            pass

    def _accept_loop(self) -> None:
        while not self._stopping.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                connection, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            if not self._connection_slots.acquire(blocking=False):
                try:
                    connection.close()
                except OSError:
                    pass
                continue
            with self._connections_lock:
                self._connections.add(connection)
            try:
                threading.Thread(
                    target=self._run_connection, args=(connection,), daemon=True
                ).start()
            except BaseException:
                with self._connections_lock:
                    self._connections.discard(connection)
                try:
                    connection.close()
                except OSError:
                    pass
                self._connection_slots.release()
                raise

    def _run_connection(self, connection: socket.socket) -> None:
        try:
            self._handle_connection(connection)
        finally:
            self._connection_slots.release()

    def _handle_connection(self, connection: socket.socket) -> None:
        transport: ProxyTransport | None = None
        stream = None
        try:
            peer_pid, peer_uid, _peer_gid = require_posix_peer_credentials(connection)
            hello = read_frame_before_deadline(
                connection, timeout_seconds=self._handshake_timeout_seconds
            )
            stream = connection.makefile("rwb", buffering=0)
            key = hello.get("conversation_key")
            current = self._owner_lookup(key) if isinstance(key, str) else None
            if current is None:
                raise RuntimeProxyProtocolError(
                    "runtime owner claim is no longer current"
                )
            client_id, negotiated_capabilities = validate_handshake(
                hello,
                expected_owner=current,
                current_owner=self._owner_lookup(current.conversation_key),
            )
            connection.settimeout(None)

            def send(envelope: dict[str, Any]) -> bool:
                if self._owner_lookup(current.conversation_key) != current:
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    raise RuntimeProxyProtocolError(
                        "runtime owner claim is no longer current"
                    )
                connection.sendall(encode_frame(envelope))
                return True

            transport = ProxyTransport(
                client_id=client_id,
                send=send,
                auth_identity=proxy_peer_auth_identity(
                    peer_pid=peer_pid,
                    peer_uid=peer_uid,
                ),
                negotiated_capabilities=negotiated_capabilities,
            )
            send({"kind": "hello.ok", "protocol": RUNTIME_PROXY_PROTOCOL_VERSION})
            while not self._stopping.is_set():
                envelope = read_frame(stream)
                if envelope.get("kind") != "rpc.request" or not isinstance(
                    envelope.get("frame"), dict
                ):
                    raise RuntimeProxyProtocolError(
                        "invalid runtime proxy request envelope"
                    )
                if self._owner_lookup(current.conversation_key) != current:
                    raise RuntimeProxyProtocolError(
                        "runtime owner claim is no longer current"
                    )
                response = self._dispatch(envelope["frame"], transport)
                if response is not None:
                    transport.write(response)
        except (EOFError, OSError, RuntimeProxyProtocolError):
            pass
        finally:
            if transport is not None:
                transport.close()
                if self._detach is not None:
                    self._detach(transport)
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass
            try:
                connection.close()
            except OSError:
                pass
            with self._connections_lock:
                self._connections.discard(connection)


class _TrackedRuntimeOwnerLease:
    """Release a registry lease and remove its coordinator bookkeeping entry."""

    def __init__(
        self,
        lease: RuntimeOwnerLease,
        on_release: Callable[[Any], None],
    ) -> None:
        self._lease = lease
        self._on_release = on_release
        self._lock = threading.Lock()
        self._released = False

    @property
    def owner(self) -> RuntimeOwner:
        return self._lease.owner

    @property
    def released(self) -> bool:
        return self._released or self._lease.released

    def is_current(self) -> bool:
        return not self.released and assert_runtime_owner(self._lease)

    def release(self) -> bool:
        with self._lock:
            if self._released:
                return False
            self._released = True
        try:
            return self._lease.release()
        finally:
            self._on_release(self)


class RuntimeProxyCoordinator:
    """Own local leases and proxy remote frontend transports to canonical owners."""

    def __init__(
        self,
        *,
        registry_home: str | Path,
        endpoint: str | Path,
        owner_id: str,
        surface: str,
        dispatch: Callable[[dict[str, Any], ProxyTransport], dict[str, Any] | None],
        detach: Callable[[ProxyTransport], None] | None = None,
    ) -> None:
        self.registry_home = Path(registry_home)
        self.endpoint = Path(endpoint)
        self.owner_id = owner_id
        self.surface = surface
        self._lock = threading.RLock()
        self._claim_lock = threading.Lock()
        self._stopped = False
        self._local_leases: dict[str, _TrackedRuntimeOwnerLease] = {}
        self._clients: dict[tuple[str, str, int], RuntimeProxyClient] = {}
        self._client_connecting: set[tuple[str, str, int]] = set()
        self._client_connect_condition = threading.Condition(self._lock)
        self._routes: dict[tuple[str, str], tuple[str, str, int]] = {}
        self._stable_routes: OrderedDict[tuple[str, str], RuntimeOwner] = OrderedDict()
        self._server = RuntimeProxyServer(
            endpoint=self.endpoint,
            owner_lookup=lambda key: lookup_runtime_owner(
                conversation_key=key,
                registry_home=self.registry_home,
            ),
            dispatch=dispatch,
            detach=detach,
        )

    def start(self) -> bool:
        with self._claim_lock:
            if self._stopped:
                return False

            return self._server.start()

    def stop(self) -> None:
        with self._claim_lock:
            with self._lock:
                self._stopped = True
                clients = list(self._clients.values())
                leases = list(self._local_leases.values())
                self._clients.clear()
                self._routes.clear()
                self._stable_routes.clear()
                self._local_leases.clear()
        for client in clients:
            client.close()
        for lease in leases:
            lease.release()
        self._server.stop()

    def claim_local(
        self,
        *,
        conversation_key: str,
        profile_home: str | Path,
    ) -> _TrackedRuntimeOwnerLease:
        """Claim a newly-created local runtime, failing closed on any collision."""
        owner_key = _profile_scoped_conversation_key(conversation_key, profile_home)
        with self._claim_lock:
            if self._stopped:
                raise RuntimeError("runtime proxy coordinator is stopped")

            with self._lock:
                existing = self._local_leases.get(owner_key)
            if existing is not None and existing.is_current():
                with self._lock:
                    if (
                        self._local_leases.get(owner_key) is existing
                        and not existing.released
                    ):
                        return existing
            claim = claim_runtime_owner(
                conversation_key=owner_key,
                owner_id=self.owner_id,
                endpoint=str(self.endpoint),
                surface=self.surface,
                registry_home=self.registry_home,
                profile_home=profile_home,
            )
            if claim.kind != "owned" or claim.lease is None:
                raise RuntimeError(
                    f"canonical runtime already owned by {claim.owner.owner_id}"
                )
            return self._track_local_lease(owner_key, claim.lease)

    def _track_local_lease(
        self, owner_key: str, lease: RuntimeOwnerLease
    ) -> _TrackedRuntimeOwnerLease:
        def remove(released: _TrackedRuntimeOwnerLease) -> None:
            with self._lock:
                if self._local_leases.get(owner_key) is released:
                    self._local_leases.pop(owner_key, None)

        tracked = _TrackedRuntimeOwnerLease(lease, remove)
        with self._lock:
            previous = self._local_leases.get(owner_key)
            self._local_leases[owner_key] = tracked
        if previous is not None:
            previous.release()
        return tracked

    def _remember_stable_route(
        self, route: tuple[str, str], owner: RuntimeOwner
    ) -> None:
        with self._lock:
            self._stable_routes.pop(route, None)
            self._stable_routes[route] = owner
            while len(self._stable_routes) > MAX_STABLE_RUNTIME_ROUTES:
                self._stable_routes.popitem(last=False)

    def _stable_route_owner(self, route: tuple[str, str]) -> RuntimeOwner | None:
        with self._lock:
            owner = self._stable_routes.pop(route, None)
            if owner is not None:
                self._stable_routes[route] = owner
            return owner

    def _connect_remote_client(
        self, owner: RuntimeOwner, transport: Any
    ) -> tuple[tuple[str, str, int], "RuntimeProxyClient"] | None:
        connection_id = str(getattr(transport, "connection_id", id(transport)))
        client_key = (connection_id, owner.owner_id, owner.generation)
        with self._client_connect_condition:
            while client_key in self._client_connecting:
                self._client_connect_condition.wait()
            client = self._clients.get(client_key)
            if client is not None:
                return client_key, client
            if self._stopped:
                return None
            self._client_connecting.add(client_key)

        client_id = proxy_scoped_client_id(transport)
        raw_capabilities = getattr(transport, "negotiated_capabilities", None)
        candidate = RuntimeProxyClient(
            owner=owner,
            client_id=client_id,
            negotiated_capabilities=(
                frozenset(raw_capabilities) if raw_capabilities is not None else None
            ),
            on_event=lambda frame: transport.write(frame),
            on_disconnect=lambda: self._handle_remote_owner_loss(
                client_key, candidate, transport, owner
            ),
        )
        client = None
        try:
            candidate.connect()
            with self._client_connect_condition:
                if not candidate.closed and not self._stopped:
                    self._clients[client_key] = candidate
                    client = candidate
        finally:
            with self._client_connect_condition:
                self._client_connecting.discard(client_key)
                self._client_connect_condition.notify_all()
        if client is None:
            candidate.close()
        return (client_key, client) if client is not None else None

    def prepare_resume(
        self,
        *,
        conversation_key: str,
        request: dict[str, Any],
        transport: Any,
        profile_home: str | Path,
    ) -> dict[str, Any] | None:
        owner_key = _profile_scoped_conversation_key(conversation_key, profile_home)
        with self._claim_lock:
            if self._stopped:
                raise RuntimeError("runtime proxy coordinator is stopped")

            with self._lock:
                existing = self._local_leases.get(owner_key)
            if existing is not None and existing.is_current():
                with self._lock:
                    if (
                        self._local_leases.get(owner_key) is existing
                        and not existing.released
                    ):
                        return None
            claim = claim_runtime_owner(
                conversation_key=owner_key,
                owner_id=self.owner_id,
                endpoint=str(self.endpoint),
                surface=self.surface,
                registry_home=self.registry_home,
                profile_home=profile_home,
            )
            if claim.kind == "owned":
                if claim.lease is not None:
                    self._track_local_lease(owner_key, claim.lease)
                return None

        connection_id = str(getattr(transport, "connection_id", id(transport)))
        connected = self._connect_remote_client(claim.owner, transport)
        if connected is None:
            return _owner_unavailable_response(claim.owner, request.get("id"))
        client_key, client = connected
        response = client.request(request)
        result = response.get("result")
        if isinstance(result, dict):
            live_session_id = result.get("session_id")
            if isinstance(live_session_id, str) and live_session_id:
                stable_identity = _stable_route_identity(transport)
                with self._lock:
                    self._routes[(connection_id, live_session_id)] = client_key
                self._remember_stable_route(
                    (stable_identity, live_session_id), claim.owner
                )
        return response

    def _handle_remote_owner_loss(
        self,
        client_key: tuple[str, str, int],
        client: "RuntimeProxyClient",
        transport: Any,
        owner: RuntimeOwner,
    ) -> None:
        with self._lock:
            if self._clients.get(client_key) is not client:
                return
            self._clients.pop(client_key, None)
            affected = sorted(
                session_id
                for (connection_id, session_id), route_key in self._routes.items()
                if route_key == client_key
            )
            self._routes = {
                route: route_key
                for route, route_key in self._routes.items()
                if route_key != client_key
            }
        current_owner = lookup_runtime_owner(
            conversation_key=owner.conversation_key,
            registry_home=self.registry_home,
        )
        if current_owner != owner:
            with self._lock:
                self._stable_routes = OrderedDict(
                    (route, stable_owner)
                    for route, stable_owner in self._stable_routes.items()
                    if stable_owner != owner
                )
        if not affected:
            return
        try:
            transport.write({
                "jsonrpc": "2.0",
                "method": "event",
                "params": {
                    "type": "session.runtime_owner_lost",
                    "payload": {
                        "session_ids": affected,
                        "owner_id": owner.owner_id,
                        "generation": owner.generation,
                        "outcome": "unknown",
                    },
                },
            })
        except Exception:
            pass

    def has_remote_route(self, transport: Any, session_id: str) -> bool:
        connection_id = str(getattr(transport, "connection_id", id(transport)))
        stable_identity = _stable_route_identity(transport)
        with self._lock:
            if (connection_id, session_id) in self._routes:
                return True
        stable_owner = self._stable_route_owner((stable_identity, session_id))
        if stable_owner is None:
            return False
        current_owner = lookup_runtime_owner(
            conversation_key=stable_owner.conversation_key,
            registry_home=self.registry_home,
        )
        if current_owner == stable_owner:
            return True
        with self._lock:
            self._stable_routes.pop((stable_identity, session_id), None)
        return False

    def route_request(
        self, request: dict[str, Any], transport: Any
    ) -> dict[str, Any] | None:
        params = request.get("params")
        session_id = params.get("session_id") if isinstance(params, dict) else None
        if not isinstance(session_id, str) or not session_id:
            return None
        connection_id = str(getattr(transport, "connection_id", id(transport)))
        stable_identity = _stable_route_identity(transport)
        with self._lock:
            client_key = self._routes.get((connection_id, session_id))
            client = self._clients.get(client_key) if client_key is not None else None
        stable_owner = self._stable_route_owner((stable_identity, session_id))
        if client is not None:
            return client.request(request)
        if stable_owner is None:
            return None

        current_owner = lookup_runtime_owner(
            conversation_key=stable_owner.conversation_key,
            registry_home=self.registry_home,
        )
        if current_owner != stable_owner:
            with self._lock:
                self._stable_routes.pop((stable_identity, session_id), None)
            return _owner_unavailable_response(stable_owner, request.get("id"))
        try:
            connected = self._connect_remote_client(stable_owner, transport)
        except (OSError, RuntimeProxyProtocolError):
            return _owner_unavailable_response(stable_owner, request.get("id"))
        if connected is None:
            return _owner_unavailable_response(stable_owner, request.get("id"))
        client_key, client = connected
        with self._lock:
            self._routes[(connection_id, session_id)] = client_key
        return client.request(request)

    def detach_transport(self, transport: Any) -> None:
        connection_id = str(getattr(transport, "connection_id", id(transport)))
        with self._lock:
            keys = [key for key in self._clients if key[0] == connection_id]
            clients = [self._clients.pop(key) for key in keys]
            self._routes = {
                route: key
                for route, key in self._routes.items()
                if route[0] != connection_id
            }
        for client in clients:
            client.close()


class RuntimeProxyClient:
    """Remote-process client for one canonical owner connection."""

    def __init__(
        self,
        *,
        owner: RuntimeOwner,
        client_id: str,
        on_event: Callable[[dict[str, Any]], None],
        negotiated_capabilities: frozenset[str] | None = None,
        on_disconnect: Callable[[], None] | None = None,
    ) -> None:
        self.owner = owner
        self.client_id = client_id
        self.negotiated_capabilities = (
            frozenset(negotiated_capabilities)
            if negotiated_capabilities is not None
            else None
        )
        self._on_event = on_event
        self._on_disconnect = on_disconnect
        self._socket: socket.socket | None = None
        self._stream: Any = None
        self._reader_thread: threading.Thread | None = None
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: dict[Any, tuple[threading.Event, list[dict[str, Any]]]] = {}
        self._closed = False
        self._disconnect_notify_lock = threading.Lock()
        self._disconnect_notified = False

    @property
    def closed(self) -> bool:
        with self._write_lock:
            return self._closed

    def connect(self) -> None:
        endpoint = Path(self.owner.endpoint)
        metadata = endpoint.stat()
        if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise RuntimeProxyProtocolError("runtime proxy socket ownership is unsafe")
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.connect(str(endpoint))
        try:
            require_posix_peer_uid(connection)
            stream = connection.makefile("rwb", buffering=0)
            connection.sendall(
                encode_frame(
                    handshake_frame(
                        self.owner,
                        client_id=self.client_id,
                        negotiated_capabilities=self.negotiated_capabilities,
                    )
                )
            )
            acknowledgement = read_frame(stream)
            if acknowledgement.get("kind") != "hello.ok":
                raise RuntimeProxyProtocolError("runtime proxy handshake was rejected")
        except Exception:
            connection.close()
            raise
        self._socket = connection
        self._stream = stream
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

    def request(
        self,
        frame: dict[str, Any],
        *,
        timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        request_id = frame.get("id")
        if request_id is None:
            raise ValueError("proxied JSON-RPC request requires an id")
        payload = encode_frame({"kind": "rpc.request", "frame": frame})
        waiter = threading.Event()
        result: list[dict[str, Any]] = []
        failed_pending = []
        failed_socket = None
        send_failed = False

        with self._write_lock:
            if self._closed or self._socket is None:
                return _owner_unavailable_response(self.owner, request_id)
            with self._pending_lock:
                if request_id in self._pending:
                    raise ValueError("duplicate proxied JSON-RPC request id")
                self._pending[request_id] = (waiter, result)
            try:
                self._socket.sendall(payload)
            except OSError:
                send_failed = True
                failed_socket = self._detach_socket_locked()
                with self._pending_lock:
                    failed_pending = list(self._pending.items())
                    self._pending.clear()

        if send_failed:
            self._close_socket(failed_socket)
            self._resolve_unavailable(failed_pending)
            self._notify_disconnect()
        elif not waiter.wait(timeout=timeout):
            timed_out = False
            with self._write_lock:
                with self._pending_lock:
                    if request_id in self._pending:
                        timed_out = True
                        failed_socket = self._detach_socket_locked()
                        failed_pending = list(self._pending.items())
                        self._pending.clear()
            if timed_out:
                self._close_socket(failed_socket)
                self._resolve_unavailable(failed_pending)
                self._notify_disconnect()
            else:
                waiter.wait()
        return result[0]

    def _detach_socket_locked(self) -> socket.socket | None:
        self._closed = True
        connection = self._socket
        self._socket = None
        return connection

    @staticmethod
    def _close_socket(connection: socket.socket | None) -> None:
        if connection is None:
            return
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            connection.close()
        except OSError:
            pass

    def close(self) -> None:
        with self._write_lock:
            connection = self._detach_socket_locked()
            with self._pending_lock:
                pending = list(self._pending.items())
                self._pending.clear()
        self._close_socket(connection)
        self._resolve_unavailable(pending)
        reader = self._reader_thread
        if reader is not None and reader is not threading.current_thread():
            reader.join(timeout=2)
        self._reader_thread = None

    def _read_loop(self) -> None:
        try:
            while not self.closed and self._stream is not None:
                envelope = read_frame(self._stream)
                frame = envelope.get("frame")
                if not isinstance(frame, dict):
                    raise RuntimeProxyProtocolError(
                        "invalid runtime proxy response envelope"
                    )
                if envelope.get("kind") == "event":
                    self._on_event(frame)
                    continue
                if envelope.get("kind") != "rpc.response":
                    raise RuntimeProxyProtocolError(
                        "invalid runtime proxy response envelope"
                    )
                request_id = frame.get("id")
                with self._pending_lock:
                    pending = self._pending.pop(request_id, None)
                    if pending is not None:
                        pending[1].append(frame)
                        pending[0].set()
        except (EOFError, OSError, RuntimeProxyProtocolError):
            with self._write_lock:
                unexpected = not self._closed
                connection = self._detach_socket_locked()
                with self._pending_lock:
                    pending = list(self._pending.items())
                    self._pending.clear()
            self._close_socket(connection)
            self._resolve_unavailable(pending)
            if unexpected:
                self._notify_disconnect()

    def _notify_disconnect(self) -> None:
        with self._disconnect_notify_lock:
            if self._disconnect_notified:
                return
            self._disconnect_notified = True
        if self._on_disconnect is not None:
            self._on_disconnect()

    def _fail_pending(self) -> None:
        with self._pending_lock:
            pending = list(self._pending.items())
            self._pending.clear()
        self._resolve_unavailable(pending)

    def _resolve_unavailable(
        self,
        pending: list[tuple[Any, tuple[threading.Event, list[dict[str, Any]]]]],
    ) -> None:
        for request_id, (waiter, result) in pending:
            result.append(_owner_unavailable_response(self.owner, request_id))
            waiter.set()


def require_posix_peer_credentials(
    sock: socket.socket, *, expected_uid: int | None = None
) -> tuple[int, int, int]:
    """Return kernel-attested credentials for an authorized Unix peer."""
    if expected_uid is None:
        expected_uid = os.getuid()
    if not hasattr(socket, "SO_PEERCRED"):
        raise RuntimeProxyProtocolError("peer credential verification is unavailable")
    try:
        credentials = sock.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
        )
        pid, uid, gid = struct.unpack("3i", credentials)
    except (OSError, struct.error) as exc:
        raise RuntimeProxyProtocolError("peer credential verification failed") from exc
    if uid != expected_uid:
        raise RuntimeProxyProtocolError("runtime proxy peer uid mismatch")
    if pid <= 0:
        raise RuntimeProxyProtocolError("runtime proxy peer pid is invalid")
    return pid, uid, gid


def require_posix_peer_uid(
    sock: socket.socket, *, expected_uid: int | None = None
) -> int:
    """Backward-compatible UID-only peer verifier."""
    _pid, uid, _gid = require_posix_peer_credentials(sock, expected_uid=expected_uid)
    return uid
