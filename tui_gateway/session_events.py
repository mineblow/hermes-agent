"""Thread-isolated event fan-out for clients attached to one live session."""

from __future__ import annotations

import copy
import json
import logging
import queue
import threading
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Callable, Optional

_log = logging.getLogger(__name__)

ATTACHMENT_QUEUE_MAX = 256
_WORKER_JOIN_TIMEOUT_SECONDS = 11.0
_DELIVERY_WAIT_TIMEOUT_SECONDS = 11.0

_OBSERVE_CAPABILITIES = frozenset({"observe"})
_CONTROL_CAPABILITIES = frozenset({
    "observe",
    "prompt.submit",
    "session.steer",
    "session.interrupt",
    "approval.respond",
    "clarify.respond",
    "ui.respond",
})
SUPPORTED_CAPABILITIES = _CONTROL_CAPABILITIES


class AttachmentMode(str, Enum):
    """Authorization level granted to an attached client."""

    OBSERVE = "observe"
    CONTROL = "control"

    @classmethod
    def parse(cls, value: str | AttachmentMode) -> AttachmentMode:
        if isinstance(value, cls):
            return value
        try:
            return cls(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid attachment mode: {value!r}") from exc

    @property
    def capabilities(self) -> frozenset[str]:
        if self is AttachmentMode.CONTROL:
            return _CONTROL_CAPABILITIES
        return _OBSERVE_CAPABILITIES


ATTACHMENT_MODES = MappingProxyType({
    mode.value: mode.capabilities for mode in AttachmentMode
})


@dataclass(frozen=True)
class AttachmentSnapshot:
    """Immutable, transport-free attachment metadata."""

    client_id: str
    mode: AttachmentMode
    capabilities: frozenset[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "capabilities", frozenset(self.capabilities))


@dataclass
class _QueuedFrame:
    frame: dict
    delivered: Optional[threading.Event] = None
    succeeded: bool = False


@dataclass
class _Attachment:
    transport: object
    snapshot: AttachmentSnapshot
    events: queue.Queue[_QueuedFrame]
    stopped: threading.Event
    worker: Optional[threading.Thread] = None


class SessionEventHub:
    """Publish ordered event copies to independently drained transports."""

    def __init__(
        self,
        *,
        on_detach: Optional[Callable[[object], None]] = None,
    ) -> None:
        self._on_detach = on_detach
        self._registry_lock = threading.Lock()
        self._publication_lock = threading.Lock()
        self._attachments: dict[int, _Attachment] = {}
        self._clients: dict[str, int] = {}
        self._retiring: dict[int, _Attachment] = {}
        self._closed = False

    def attach(
        self,
        transport: object,
        *,
        client_id: str,
        mode: AttachmentMode,
        capabilities: frozenset[str] | set[str] | None = None,
    ) -> AttachmentSnapshot:
        """Attach or reconnect one stable client to this live runtime."""
        parsed_mode = AttachmentMode.parse(mode)
        granted = parsed_mode.capabilities
        if capabilities is not None:
            granted = granted.intersection(capabilities)
        snapshot = AttachmentSnapshot(
            client_id=str(client_id),
            mode=parsed_mode,
            capabilities=granted,
        )
        key = id(transport)
        replaced: _Attachment | None = None
        with self._publication_lock:
            with self._registry_lock:
                if self._closed:
                    raise RuntimeError("session event hub is closed")
                if key in self._retiring:
                    raise RuntimeError(
                        "transport attachment cleanup is still in progress"
                    )

                previous_key = self._clients.get(snapshot.client_id)
                if previous_key is not None and previous_key != key:
                    previous = self._attachments.pop(previous_key, None)
                    if previous is not None:
                        previous.stopped.set()
                        self._retiring[previous_key] = previous
                        replaced = previous

                current = self._attachments.get(key)
                if current is not None and current.transport is transport:
                    previous_client_id = current.snapshot.client_id
                    if self._clients.get(previous_client_id) == key:
                        self._clients.pop(previous_client_id, None)
                    current.snapshot = snapshot
                    self._clients[snapshot.client_id] = key
                else:
                    attachment = _Attachment(
                        transport=transport,
                        snapshot=snapshot,
                        events=queue.Queue(maxsize=ATTACHMENT_QUEUE_MAX),
                        stopped=threading.Event(),
                    )
                    worker = threading.Thread(
                        target=self._write_events,
                        args=(attachment,),
                        name=f"session-event-writer-{client_id}",
                        daemon=True,
                    )
                    attachment.worker = worker
                    self._attachments[key] = attachment
                    self._clients[snapshot.client_id] = key
                    worker.start()

        if replaced is not None:
            # Stable reconnect must not wait for a stale transport.write(). The
            # retired writer owns generation-safe cleanup when it exits.
            self._finish_detach(replaced, join=False)
        return snapshot

    def detach(self, transport: object) -> bool:
        """Remove one transport and stop its writer worker."""
        with self._publication_lock:
            attachment = self._remove_registered(transport)
        if attachment is None:
            return False
        if not self._finish_detach(attachment, join=True):
            raise RuntimeError("session attachment writer did not stop")
        return True

    def has_transport(self, transport: object) -> bool:
        with self._registry_lock:
            attachment = self._attachments.get(id(transport))
            return attachment is not None and attachment.transport is transport

    def require(self, transport: object, capability: str) -> None:
        """Raise when *transport* is detached or lacks *capability*."""
        with self._registry_lock:
            attachment = self._attachments.get(id(transport))
            if attachment is None or attachment.transport is not transport:
                raise PermissionError("transport is not attached")
            capabilities = attachment.snapshot.capabilities
        if capability not in capabilities:
            raise PermissionError(f"attachment requires capability: {capability}")

    def publish(
        self,
        frame: dict,
        *,
        prepare: Optional[Callable[[dict], None]] = None,
        wait_for_delivery: bool = False,
    ) -> bool:
        """Prepare once, then enqueue private copies in one total order.

        ``prepare`` runs while the publication lock is held, before subscriber
        snapshots are cloned.  Callers use it for sequence allocation and replay
        recording so the stamped order and every subscriber's delivery order
        cannot diverge under concurrent emitters.
        """
        # Normalize before taking the publication lock. Besides proving the
        # protocol frame is JSON-safe before any subscriber sees it, this keeps
        # custom mapping/value hooks outside the hub's lock and leaves only
        # plain built-ins for the per-subscriber deepcopy below.
        normalized = json.loads(json.dumps(frame, ensure_ascii=False))
        overflowed: list[_Attachment] = []
        queued: list[_QueuedFrame] = []
        enqueued = False
        with self._publication_lock:
            with self._registry_lock:
                if self._closed:
                    return False
                attachments = list(self._attachments.values())
            if prepare is not None:
                prepare(normalized)

            for attachment in attachments:
                if attachment.stopped.is_set():
                    continue
                try:
                    item = _QueuedFrame(
                        frame=copy.deepcopy(normalized),
                        delivered=threading.Event() if wait_for_delivery else None,
                    )
                    attachment.events.put_nowait(item)
                    queued.append(item)
                    enqueued = True
                except queue.Full:
                    removed = self._remove_registered(attachment.transport)
                    if removed is not None:
                        overflowed.append(removed)

        for attachment in overflowed:
            self._finish_detach(attachment, join=False)
        if wait_for_delivery:
            for item in queued:
                if item.delivered is None or not item.delivered.wait(
                    timeout=_DELIVERY_WAIT_TIMEOUT_SECONDS
                ):
                    return False
                if not item.succeeded:
                    return False
        return enqueued

    def snapshots(self) -> list[dict]:
        """Return sanitized attachment metadata with no transport internals."""
        with self._registry_lock:
            snapshots = [item.snapshot for item in self._attachments.values()]
        return [
            {
                "client_id": snapshot.client_id,
                "mode": snapshot.mode.value,
                "capabilities": sorted(snapshot.capabilities),
            }
            for snapshot in snapshots
        ]

    def count(self) -> int:
        with self._registry_lock:
            return len(self._attachments)

    def close(self) -> None:
        """Idempotently detach all clients and join all writer workers.

        Raises ``RuntimeError`` rather than silently claiming success when a
        transport write exceeds the bounded shutdown interval. Cleanup for that
        attachment remains deferred until its writer actually exits.
        """
        with self._publication_lock:
            with self._registry_lock:
                if self._closed:
                    attachments = []
                else:
                    self._closed = True
                    attachments = list(self._attachments.values())
                    self._attachments.clear()
                    self._clients.clear()
                    for attachment in attachments:
                        attachment.stopped.set()
                        self._retiring[id(attachment.transport)] = attachment

                # A prior close may have timed out while a transport.write()
                # was still in flight. Keep those workers discoverable so a
                # repeated close can re-check them without spawning an
                # unbounded cleanup waiter.
                for attachment in self._retiring.values():
                    if attachment not in attachments:
                        attachments.append(attachment)

        failed = False
        for attachment in attachments:
            failed = not self._finish_detach(attachment, join=True) or failed
        if failed:
            raise RuntimeError("one or more session attachment writers did not stop")

    def _write_events(self, attachment: _Attachment) -> None:
        try:
            while not attachment.stopped.is_set():
                try:
                    item = attachment.events.get(timeout=0.05)
                except queue.Empty:
                    continue
                if attachment.stopped.is_set():
                    if item.delivered is not None:
                        item.delivered.set()
                    return
                try:
                    write_and_wait = getattr(
                        attachment.transport, "write_and_wait", None
                    )
                    if item.delivered is not None and callable(write_and_wait):
                        success = write_and_wait(item.frame)
                    else:
                        success = attachment.transport.write(item.frame)  # type: ignore[attr-defined]
                    if success is False:
                        raise ConnectionError("transport write returned false")
                    item.succeeded = True
                except Exception as exc:
                    _log.debug("session attachment writer failed: %s", exc)
                    self._detach_failed_attachment(attachment)
                    return
                finally:
                    if item.delivered is not None:
                        item.delivered.set()
        finally:
            # The writer itself owns deferred cleanup. This avoids creating a
            # second thread that waits forever when transport.write() is
            # permanently blocked. _complete_cleanup atomically claims the
            # retiring generation, so a concurrent detach/close can also call
            # it without invoking the callback twice.
            self._complete_cleanup(attachment.transport)

    def _detach_failed_attachment(self, attachment: _Attachment) -> None:
        with self._publication_lock:
            removed = self._remove_registered(attachment.transport)
        if removed is not None:
            self._finish_detach(removed, join=False)

    def _remove_registered(self, transport: object) -> Optional[_Attachment]:
        """Remove under the registry lock; caller serializes publications."""
        with self._registry_lock:
            key = id(transport)
            attachment = self._attachments.get(key)
            if attachment is None or attachment.transport is not transport:
                return None
            del self._attachments[key]
            client_id = attachment.snapshot.client_id
            if self._clients.get(client_id) == key:
                del self._clients[client_id]
            attachment.stopped.set()
            self._retiring[key] = attachment
            return attachment

    def _finish_detach(self, attachment: _Attachment, *, join: bool) -> bool:
        worker = attachment.worker
        if worker is None or worker is threading.current_thread():
            self._complete_cleanup(attachment.transport)
            return True
        if not join:
            return True
        worker.join(timeout=_WORKER_JOIN_TIMEOUT_SECONDS)
        if worker.is_alive():
            return False
        self._complete_cleanup(attachment.transport)
        return True

    def _complete_cleanup(self, transport: object) -> None:
        with self._registry_lock:
            key = id(transport)
            attachment = self._retiring.get(key)
            if attachment is None or attachment.transport is not transport:
                return
            del self._retiring[key]
        try:
            self._run_cleanup(transport)
        except Exception:
            # _run_cleanup already logs user callback failures. The retiring
            # generation is still complete and must not block reattachment.
            pass

    def _run_cleanup(self, transport: object) -> None:
        if self._on_detach is None:
            return
        try:
            self._on_detach(transport)
        except Exception:
            _log.exception("session attachment cleanup failed")
