"""Bounded async frontend client for a canonical live runtime owner."""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from hermes_cli.live_runtime_owners import RuntimeOwner
from hermes_cli.live_runtime_protocol import (
    LIVE_RUNTIME_PROTOCOL_VERSION,
    SUPPORTED_CAPABILITIES,
    LiveRuntimeProtocolError,
    frontend_hello,
    validate_interaction_response,
    validate_runtime_event,
)


class AsyncRuntimeConnection(Protocol):
    async def send(self, frame: dict[str, Any]) -> None: ...

    async def recv(self) -> dict[str, Any]: ...

    async def close(self) -> None: ...


class LiveRuntimeClientError(RuntimeError):
    pass


class LiveRuntimeBackpressure(LiveRuntimeClientError):
    pass


class OwnerGenerationFenceError(LiveRuntimeClientError):
    pass


class UnknownExecutionOutcome(LiveRuntimeClientError):
    pass


class LiveRuntimeClosed(LiveRuntimeClientError):
    pass


@dataclass
class _OutboundRequest:
    request_id: str
    kind: str
    payload: dict[str, Any]
    future: asyncio.Future[Any]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class AsyncLiveRuntimeClient:
    """Reconnect a generic gateway frontend to one fenced runtime owner.

    Requests removed from the outbound queue and handed to a connection are
    never retried. A disconnect before their correlated response therefore
    yields :class:`UnknownExecutionOutcome`.
    """

    def __init__(
        self,
        *,
        conversation_key: str,
        durable_root: str,
        client_id: str,
        principal: Mapping[str, Any],
        surface: str,
        requested_capabilities: set[str] | frozenset[str],
        owner_lookup: Callable[[str], RuntimeOwner | None | Awaitable[RuntimeOwner | None]],
        connector: Callable[
            [RuntimeOwner], AsyncRuntimeConnection | Awaitable[AsyncRuntimeConnection]
        ],
        on_event: Callable[[dict[str, Any]], Any] | None = None,
        on_resync_required: Callable[[dict[str, Any]], Any] | None = None,
        on_pending_interaction: Callable[[dict[str, Any]], Any] | None = None,
        on_local_message_id: Callable[[str], Any] | None = None,
        on_close: Callable[[], Any] | None = None,
        queue_size: int = 64,
        reconnect_delay: float = 0.1,
        delivery_drain_timeout: float = 30.0,
        close_hook_timeout: float = 2.0,
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        if reconnect_delay < 0:
            raise ValueError("reconnect_delay must not be negative")
        if delivery_drain_timeout <= 0:
            raise ValueError("delivery_drain_timeout must be positive")
        if close_hook_timeout <= 0:
            raise ValueError("close_hook_timeout must be positive")
        if not requested_capabilities <= SUPPORTED_CAPABILITIES:
            raise ValueError("unsupported frontend capability")
        self.conversation_key = conversation_key
        self.durable_root = durable_root
        self.client_id = client_id
        self.principal = dict(principal)
        self.surface = surface
        self.requested_capabilities = frozenset(requested_capabilities)
        self._owner_lookup = owner_lookup
        self._connector = connector
        self._on_event = on_event
        self._on_resync_required = on_resync_required
        self._on_pending_interaction = on_pending_interaction
        self._on_local_message_id = on_local_message_id
        self._on_close = on_close
        self._close_hook_called = False
        self._reconnect_delay = reconnect_delay
        self._delivery_drain_timeout = delivery_drain_timeout
        self._close_hook_timeout = close_hook_timeout
        self._outbound: asyncio.Queue[_OutboundRequest] = asyncio.Queue(
            maxsize=queue_size
        )
        self._inbound: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=queue_size
        )
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._connection: AsyncRuntimeConnection | None = None
        self._runner: asyncio.Task[None] | None = None
        self._closing = False
        self._fatal: BaseException | None = None
        self._first_connected = asyncio.Event()
        self._state_changed = asyncio.Condition()
        self._dispatch_idle = asyncio.Event()
        self._dispatch_idle.set()
        self._resync_task: asyncio.Task[Any] | None = None
        self._pending_interactions: list[dict[str, Any]] = []
        self._owner_id: str | None = None
        self._owner_generation = 0
        self._runtime_id: str | None = None
        self._durable_session_id: str | None = None
        self._replay_epoch: str | None = None
        self._last_seq = 0
        self.accepted_capabilities = frozenset()

    @property
    def replay_watermark(self) -> tuple[str, int] | None:
        if self._replay_epoch is None:
            return None
        return self._replay_epoch, self._last_seq

    @property
    def owner_id(self) -> str | None:
        return self._owner_id

    @property
    def owner_generation(self) -> int:
        return self._owner_generation

    @property
    def runtime_id(self) -> str | None:
        return self._runtime_id

    @property
    def durable_session_id(self) -> str | None:
        return self._durable_session_id

    async def start(self) -> None:
        if self._runner is not None:
            await self._wait_first_connection()
            return
        self._runner = asyncio.create_task(self._run(), name="live-runtime-client")
        await self._wait_first_connection()

    async def _wait_first_connection(self) -> None:
        assert self._runner is not None
        waiter = asyncio.create_task(self._first_connected.wait())
        try:
            done, _ = await asyncio.wait(
                {waiter, self._runner}, return_when=asyncio.FIRST_COMPLETED
            )
            if self._runner in done and not self._first_connected.is_set():
                await self.wait_closed()
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)

    async def request(self, payload: Mapping[str, Any]) -> Any:
        return await self._request("control.request", dict(payload))

    async def respond_to_interaction(self, payload: Mapping[str, Any]) -> Any:
        if "interaction.respond" not in self.accepted_capabilities:
            raise LiveRuntimeProtocolError("interaction response is not authorized")
        return await self._request(
            "interaction.respond", validate_interaction_response(payload)
        )

    async def _request(self, kind: str, payload: dict[str, Any]) -> Any:
        if self._closing or self._runner is None:
            raise LiveRuntimeClosed("live runtime client is not running")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        item = _OutboundRequest(uuid.uuid4().hex, kind, payload, future)
        try:
            self._outbound.put_nowait(item)
        except asyncio.QueueFull as exc:
            raise LiveRuntimeBackpressure("live runtime outbound queue is full") from exc
        try:
            return await future
        except asyncio.CancelledError:
            future.cancel()
            raise

    def register_local_message_id(self, message_id: str) -> None:
        if self._on_local_message_id is not None:
            self._on_local_message_id(message_id)

    async def wait_for_sequence(self, seq: int) -> None:
        async with self._state_changed:
            await self._state_changed.wait_for(
                lambda: self._last_seq >= seq or self._closing or self._fatal is not None
            )
        self._raise_fatal()
        if self._closing and self._last_seq < seq:
            raise LiveRuntimeClosed("live runtime client closed before sequence")

    async def wait_for_generation(self, generation: int) -> None:
        async with self._state_changed:
            await self._state_changed.wait_for(
                lambda: self._owner_generation >= generation
                or self._closing
                or self._fatal is not None
            )
        self._raise_fatal()
        if self._closing and self._owner_generation < generation:
            raise LiveRuntimeClosed("live runtime client closed before generation")

    async def wait_closed(self) -> None:
        runner = self._runner
        if runner is not None and runner is not asyncio.current_task():
            await asyncio.gather(runner, return_exceptions=True)
        self._raise_fatal()

    async def close(self) -> None:
        if self._closing:
            if self._runner is not None:
                await asyncio.gather(self._runner, return_exceptions=True)
            return
        self._closing = True
        if self._on_close is not None and not self._close_hook_called:
            self._close_hook_called = True
            try:
                await asyncio.wait_for(
                    _maybe_await(self._on_close()),
                    timeout=self._close_hook_timeout,
                )
            except (Exception, asyncio.CancelledError):
                # Presentation teardown must never strand the authenticated
                # runtime transport or its pending request futures.
                pass
        connection = self._connection
        if connection is not None:
            await connection.close()
        if self._runner is not None:
            self._runner.cancel()
            await asyncio.gather(self._runner, return_exceptions=True)
        self._fail_all(LiveRuntimeClosed("live runtime client closed"))
        self._drain_outbound(LiveRuntimeClosed("live runtime client closed"))
        await self._notify_state()
        # Let tasks awaiting terminal request futures observe them before a
        # clean-shutdown caller resumes.
        await asyncio.sleep(0)

    def _raise_fatal(self) -> None:
        if self._fatal is not None:
            raise self._fatal

    async def _run(self) -> None:
        try:
            while not self._closing:
                owner = await _maybe_await(self._owner_lookup(self.conversation_key))
                if owner is None:
                    await asyncio.sleep(self._reconnect_delay)
                    continue
                if owner.generation < self._owner_generation:
                    raise OwnerGenerationFenceError(
                        "runtime owner generation moved backwards"
                    )
                if (
                    owner.generation == self._owner_generation
                    and self._owner_id is not None
                    and owner.owner_id != self._owner_id
                ):
                    raise OwnerGenerationFenceError(
                        "runtime owner changed without a generation advance"
                    )
                try:
                    await self._run_connection(owner)
                except asyncio.CancelledError:
                    raise
                except (OwnerGenerationFenceError, LiveRuntimeProtocolError):
                    raise
                except Exception:
                    self._fail_all(
                        UnknownExecutionOutcome(
                            "runtime disconnected after request transmission"
                        )
                    )
                    await self._drop_connection()
                    if not self._closing:
                        await asyncio.sleep(self._reconnect_delay)
        except asyncio.CancelledError:
            pass
        except BaseException as exc:
            self._fatal = exc
        finally:
            connection = self._connection
            self._connection = None
            if connection is not None:
                await connection.close()
            if self._fatal is not None:
                self._fail_all(self._fatal)
                self._drain_outbound(self._fatal)
            await self._notify_state()

    async def _drop_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None:
            await connection.close()
        # Undelivered frames belong to the superseded connection. The next
        # hello replays from the last callback-delivered watermark instead.
        while True:
            try:
                self._inbound.get_nowait()
            except asyncio.QueueEmpty:
                return

    async def _run_connection(self, owner: RuntimeOwner) -> None:
        connection = await _maybe_await(self._connector(owner))
        self._connection = connection
        hello_kwargs: dict[str, Any] = {}
        if self._replay_epoch is not None:
            hello_kwargs = {
                "replay_epoch": self._replay_epoch,
                "replay_seq": self._last_seq,
                "replay_runtime_id": self._runtime_id,
            }
        await connection.send(
            frontend_hello(
                client_id=self.client_id,
                principal=self.principal,
                surface=self.surface,
                requested_capabilities=self.requested_capabilities,
                durable_root=self.durable_root,
                **hello_kwargs,
            )
        )
        acknowledgement = await connection.recv()
        self._accept_hello(owner, acknowledgement)
        if self._resync_task is not None:
            task = self._resync_task
            self._resync_task = None
            await task
        pending_interactions = self._pending_interactions
        self._pending_interactions = []
        for interaction in pending_interactions:
            if self._on_pending_interaction is None:
                raise LiveRuntimeProtocolError(
                    "pending interaction has no presentation callback"
                )
            await _maybe_await(self._on_pending_interaction(interaction))
        await self._notify_state()
        self._first_connected.set()

        writer = asyncio.create_task(self._writer(connection))
        reader = asyncio.create_task(self._reader(connection))
        dispatcher = asyncio.create_task(self._dispatcher())
        tasks = {writer, reader, dispatcher}
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        if dispatcher in pending and not self._dispatch_idle.is_set():
            try:
                await asyncio.wait_for(
                    self._dispatch_idle.wait(), timeout=self._delivery_drain_timeout
                )
            except TimeoutError:
                pass
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exception = task.exception()
            if exception is not None:
                raise exception
        raise EOFError("runtime connection stopped")

    def _accept_hello(self, owner: RuntimeOwner, frame: Any) -> None:
        if (
            not isinstance(frame, dict)
            or frame.get("kind") != "frontend.hello.ok"
            or frame.get("protocol") != LIVE_RUNTIME_PROTOCOL_VERSION
            or frame.get("owner_id") != owner.owner_id
            or frame.get("generation") != owner.generation
        ):
            raise LiveRuntimeProtocolError("invalid frontend hello acknowledgement")
        capabilities = frame.get("accepted_capabilities")
        if (
            not isinstance(capabilities, list)
            or any(
                not isinstance(capability, str)
                or capability not in self.requested_capabilities
                for capability in capabilities
            )
            or len(set(capabilities)) != len(capabilities)
        ):
            raise LiveRuntimeProtocolError("invalid accepted capabilities")
        runtime_id = frame.get("runtime_id")
        durable_session_id = frame.get("durable_session_id")
        replay_epoch = frame.get("replay_epoch")
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (runtime_id, durable_session_id, replay_epoch)
        ):
            raise LiveRuntimeProtocolError("invalid runtime acknowledgement identity")
        if owner.generation < self._owner_generation:
            raise OwnerGenerationFenceError("runtime owner generation moved backwards")
        runtime_changed = self._runtime_id is not None and self._runtime_id != runtime_id
        self._owner_id = owner.owner_id
        self._owner_generation = owner.generation
        self._runtime_id = runtime_id
        self._durable_session_id = durable_session_id
        self.accepted_capabilities = frozenset(capabilities)
        pending_interactions = frame.get("pending_interactions", [])
        if not isinstance(pending_interactions, list):
            raise LiveRuntimeProtocolError("invalid pending interactions")
        if pending_interactions and "interaction.respond" not in capabilities:
            raise LiveRuntimeProtocolError(
                "observer acknowledgement contains pending interactions"
            )
        normalized_pending = []
        pending_ids = set()
        for interaction in pending_interactions:
            if not isinstance(interaction, dict):
                raise LiveRuntimeProtocolError("invalid pending interaction")
            interaction_type = interaction.get("interaction_type")
            request_id = interaction.get("request_id")
            payload = interaction.get("payload")
            if (
                interaction_type not in {"approval", "clarification"}
                or not isinstance(request_id, str)
                or not request_id.strip()
                or request_id in pending_ids
                or not isinstance(payload, dict)
                or payload.get("request_id") != request_id
            ):
                raise LiveRuntimeProtocolError("invalid pending interaction")
            pending_ids.add(request_id)
            normalized_pending.append(
                {
                    "interaction_type": interaction_type,
                    "request_id": request_id,
                    "payload": dict(payload),
                }
            )
        self._pending_interactions = normalized_pending
        epoch_changed = self._replay_epoch != replay_epoch
        replay_identity_changed = epoch_changed or runtime_changed
        if replay_identity_changed:
            self._replay_epoch = replay_epoch
            self._last_seq = 0
        replay_seq = frame.get("replay_seq")
        if (
            isinstance(replay_seq, bool)
            or not isinstance(replay_seq, int)
            or replay_seq < self._last_seq
        ):
            raise LiveRuntimeProtocolError("invalid replay baseline")
        if replay_identity_changed:
            self._last_seq = replay_seq
        if frame.get("replay_truncated") is True:
            snapshot = frame.get("resync_snapshot")
            latest_assistant = (
                snapshot.get("latest_assistant") if isinstance(snapshot, dict) else None
            )
            if (
                not isinstance(snapshot, dict)
                or (
                    latest_assistant is not None
                    and (
                        not isinstance(latest_assistant, dict)
                        or not isinstance(latest_assistant.get("text"), str)
                        or not latest_assistant["text"]
                    )
                )
            ):
                raise LiveRuntimeProtocolError("invalid presentation resync snapshot")
            if not replay_identity_changed:
                self._last_seq = replay_seq
            self._signal_resync(snapshot)
        elif runtime_changed:
            self._signal_resync()
        elif not epoch_changed and replay_seq != self._last_seq:
            raise LiveRuntimeProtocolError("unexpected replay baseline")

    async def _writer(self, connection: AsyncRuntimeConnection) -> None:
        while True:
            item = await self._outbound.get()
            if item.future.cancelled():
                continue
            self._pending[item.request_id] = item.future
            try:
                await connection.send(
                    {
                        "kind": item.kind,
                        "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
                        "request_id": item.request_id,
                        "payload": item.payload,
                    }
                )
            except BaseException:
                raise

    async def _reader(self, connection: AsyncRuntimeConnection) -> None:
        while True:
            frame = await connection.recv()
            try:
                self._inbound.put_nowait(frame)
            except asyncio.QueueFull as exc:
                self._signal_resync()
                raise LiveRuntimeBackpressure(
                    "live runtime inbound queue is full"
                ) from exc

    async def _dispatcher(self) -> None:
        while True:
            frame = await self._inbound.get()
            self._dispatch_idle.clear()
            try:
                kind = frame.get("kind") if isinstance(frame, dict) else None
                if kind == "control.response":
                    self._dispatch_response(frame)
                elif kind == "runtime.event":
                    await self._dispatch_event(frame)
                else:
                    raise LiveRuntimeProtocolError("invalid runtime frame")
            finally:
                self._dispatch_idle.set()

    def _dispatch_response(self, frame: dict[str, Any]) -> None:
        request_id = frame.get("request_id")
        future = self._pending.pop(request_id, None)
        if future is None or future.done():
            return
        if ("result" in frame) == ("error" in frame):
            raise LiveRuntimeProtocolError("invalid control response")
        if "error" in frame:
            future.set_exception(LiveRuntimeClientError(str(frame["error"])))
        else:
            future.set_result(frame["result"])

    async def _dispatch_event(self, frame: dict[str, Any]) -> None:
        event = validate_runtime_event(frame)
        if (
            event["runtime_id"] != self._runtime_id
            or event["durable_session_id"] != self._durable_session_id
        ):
            raise LiveRuntimeProtocolError("runtime event identity mismatch")
        if event["replay_epoch"] != self._replay_epoch:
            self._signal_resync()
            raise LiveRuntimeProtocolError("runtime replay epoch mismatch")
        seq = event["seq"]
        if seq <= self._last_seq:
            return
        if seq != self._last_seq + 1:
            self._signal_resync()
            raise LiveRuntimeProtocolError("runtime event sequence gap")
        if self._on_event is not None:
            await _maybe_await(self._on_event(event))
        self._last_seq = seq
        await self._notify_state()

    def _signal_resync(self, snapshot: dict[str, Any] | None = None) -> None:
        if self._on_resync_required is None:
            return
        signal: dict[str, Any] = {
            "durable_session_id": str(self._durable_session_id),
            "runtime_id": str(self._runtime_id),
            "replay_epoch": str(self._replay_epoch),
        }
        if snapshot is not None:
            signal["snapshot"] = snapshot
        result = self._on_resync_required(signal)
        if inspect.isawaitable(result):
            self._resync_task = asyncio.create_task(result)

    def _fail_all(self, error: BaseException) -> None:
        pending = self._pending
        self._pending = {}
        for future in pending.values():
            if not future.done():
                future.set_exception(error)

    def _drain_outbound(self, error: BaseException) -> None:
        while True:
            try:
                item = self._outbound.get_nowait()
            except asyncio.QueueEmpty:
                return
            if not item.future.done():
                item.future.set_exception(error)

    async def _notify_state(self) -> None:
        async with self._state_changed:
            self._state_changed.notify_all()
