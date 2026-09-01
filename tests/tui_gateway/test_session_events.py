"""Tests for the per-session attachment event hub."""

from __future__ import annotations

import threading
from dataclasses import FrozenInstanceError

import pytest

from tui_gateway.session_events import (
    ATTACHMENT_QUEUE_MAX,
    AttachmentMode,
    AttachmentSnapshot,
    SessionEventHub,
)


class RecordingTransport:
    def __init__(self) -> None:
        self.frames: list[dict] = []
        self.written = threading.Event()
        self._condition = threading.Condition()

    def write(self, frame: dict) -> bool:
        with self._condition:
            self.frames.append(frame)
            self._condition.notify_all()
        self.written.set()
        return True

    def wait_for_count(self, count: int) -> None:
        with self._condition:
            assert self._condition.wait_for(
                lambda: len(self.frames) >= count,
                timeout=5,
            ), f"transport received fewer than {count} frames"


class BlockingTransport(RecordingTransport):
    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()

    def write(self, frame: dict) -> bool:
        self.written.set()
        assert self.release.wait(timeout=5), "blocked writer was not released"
        return super().write(frame)


def _wait(event: threading.Event, message: str) -> None:
    assert event.wait(timeout=5), message


def test_attachment_modes_parse_and_reject_invalid_values():
    assert AttachmentMode.parse("observe") is AttachmentMode.OBSERVE
    assert AttachmentMode.parse("control") is AttachmentMode.CONTROL

    with pytest.raises(ValueError, match="invalid attachment mode"):
        AttachmentMode.parse("admin")


def test_capabilities_are_immutable_and_match_protocol_contract():
    assert AttachmentMode.OBSERVE.capabilities == frozenset({"observe"})
    assert AttachmentMode.CONTROL.capabilities == frozenset({
        "observe",
        "prompt.submit",
        "session.steer",
        "session.interrupt",
        "approval.respond",
        "clarify.respond",
        "ui.respond",
    })
    assert isinstance(AttachmentMode.CONTROL.capabilities, frozenset)

    supplied_capabilities = {"observe"}
    snapshot = AttachmentSnapshot(
        client_id="client",
        mode=AttachmentMode.OBSERVE,
        capabilities=supplied_capabilities,  # type: ignore[arg-type]
    )
    supplied_capabilities.add("mutated")
    assert snapshot.capabilities == frozenset({"observe"})
    with pytest.raises(FrozenInstanceError):
        snapshot.client_id = "changed"  # type: ignore[misc]


def test_attach_is_idempotent_and_updates_mode_without_duplicate_delivery():
    hub = SessionEventHub()
    transport = RecordingTransport()
    try:
        first = hub.attach(
            transport,
            client_id="client-a",
            mode=AttachmentMode.OBSERVE,
        )
        second = hub.attach(
            transport,
            client_id="client-a",
            mode=AttachmentMode.CONTROL,
        )

        assert first.mode is AttachmentMode.OBSERVE
        assert second.mode is AttachmentMode.CONTROL
        assert hub.count() == 1
        hub.require(transport, "prompt.submit")
        assert hub.publish({"event": 1}) is True
        _wait(transport.written, "updated attachment did not receive an event")
        assert transport.frames == [{"event": 1}]
    finally:
        hub.close()


def test_require_rejects_missing_transport_or_capability():
    hub = SessionEventHub()
    observer = RecordingTransport()
    stranger = RecordingTransport()
    try:
        hub.attach(observer, client_id="observer", mode=AttachmentMode.OBSERVE)

        with pytest.raises(PermissionError, match="prompt.submit"):
            hub.require(observer, "prompt.submit")
        with pytest.raises(PermissionError, match="not attached"):
            hub.require(stranger, "observe")
    finally:
        hub.close()


def test_publish_copies_one_frame_per_subscriber_and_returns_before_delivery():
    hub = SessionEventHub()
    blocked = BlockingTransport()
    recording = RecordingTransport()
    frame = {"payload": {"items": [1]}}
    try:
        hub.attach(blocked, client_id="blocked", mode=AttachmentMode.OBSERVE)
        hub.attach(recording, client_id="recording", mode=AttachmentMode.OBSERVE)

        assert hub.publish(frame) is True
        _wait(blocked.written, "blocking writer was not entered")
        frame["payload"]["items"].append(2)
        blocked.release.set()
        _wait(recording.written, "recording transport did not receive frame")
        blocked.wait_for_count(1)

        assert recording.frames == [{"payload": {"items": [1]}}]
        assert blocked.frames == [{"payload": {"items": [1]}}]
        assert recording.frames[0] is not blocked.frames[0]
        recording.frames[0]["payload"]["items"].append(3)
        assert blocked.frames[0] == {"payload": {"items": [1]}}
    finally:
        blocked.release.set()
        hub.close()


def test_publish_can_wait_until_every_transport_write_completes():
    hub = SessionEventHub()
    blocked = BlockingTransport()
    completed = threading.Event()

    def publish() -> None:
        assert hub.publish({"event": "resolved"}, wait_for_delivery=True) is True
        completed.set()

    try:
        hub.attach(blocked, client_id="blocked", mode=AttachmentMode.OBSERVE)
        worker = threading.Thread(target=publish)
        worker.start()
        _wait(blocked.written, "blocking writer was not entered")
        assert not completed.is_set()
        blocked.release.set()
        _wait(completed, "synchronous publication did not acknowledge delivery")
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert blocked.frames == [{"event": "resolved"}]
    finally:
        blocked.release.set()
        hub.close()


def test_concurrent_publishers_have_one_identical_delivery_order():
    hub = SessionEventHub()
    left = RecordingTransport()
    right = RecordingTransport()
    start = threading.Barrier(3)
    publication_count = 40

    def publish(source: str) -> None:
        start.wait(timeout=5)
        for index in range(publication_count):
            assert hub.publish({"source": source, "index": index})

    try:
        hub.attach(left, client_id="left", mode=AttachmentMode.OBSERVE)
        hub.attach(right, client_id="right", mode=AttachmentMode.OBSERVE)
        workers = [
            threading.Thread(target=publish, args=(source,)) for source in ("a", "b")
        ]
        for worker in workers:
            worker.start()
        start.wait(timeout=5)
        for worker in workers:
            worker.join(timeout=5)
            assert not worker.is_alive()

        left.wait_for_count(publication_count * 2)
        right.wait_for_count(publication_count * 2)

        assert left.frames == right.frames
        assert len(left.frames) == publication_count * 2
        for source in ("a", "b"):
            assert [
                frame["index"] for frame in left.frames if frame["source"] == source
            ] == list(range(publication_count))
    finally:
        hub.close()


def test_prepare_runs_inside_the_total_publication_order_before_fanout():
    hub = SessionEventHub()
    left = RecordingTransport()
    right = RecordingTransport()
    start = threading.Barrier(3)
    next_sequence = 0

    def prepare(frame: dict) -> None:
        nonlocal next_sequence
        next_sequence += 1
        frame["seq"] = next_sequence

    def publish(source: str) -> None:
        start.wait(timeout=5)
        for index in range(20):
            assert hub.publish(
                {"source": source, "index": index},
                prepare=prepare,
            )

    try:
        hub.attach(left, client_id="left", mode=AttachmentMode.OBSERVE)
        hub.attach(right, client_id="right", mode=AttachmentMode.OBSERVE)
        workers = [
            threading.Thread(target=publish, args=(source,)) for source in ("a", "b")
        ]
        for worker in workers:
            worker.start()
        start.wait(timeout=5)
        for worker in workers:
            worker.join(timeout=5)
            assert not worker.is_alive()

        left.wait_for_count(40)
        right.wait_for_count(40)

        assert left.frames == right.frames
        assert [frame["seq"] for frame in left.frames] == list(range(1, 41))
    finally:
        hub.close()


def test_reconnect_does_not_wait_for_blocked_stale_writer():
    hub = SessionEventHub()
    stale = BlockingTransport()
    replacement = RecordingTransport()
    attach_done = threading.Event()
    attach_error = []
    try:
        hub.attach(stale, client_id="stable-blocked", mode=AttachmentMode.OBSERVE)
        assert hub.publish({"event": "block-stale"})
        _wait(stale.written, "stale transport did not enter write")

        def reconnect() -> None:
            try:
                hub.attach(
                    replacement,
                    client_id="stable-blocked",
                    mode=AttachmentMode.CONTROL,
                )
            except Exception as exc:
                attach_error.append(exc)
            finally:
                attach_done.set()

        worker = threading.Thread(target=reconnect)
        worker.start()
        assert attach_done.wait(timeout=1), "reconnect waited for stale writer shutdown"
        assert attach_error == []
        assert hub.has_transport(replacement)
        stale.release.set()
        worker.join(timeout=5)
    finally:
        stale.release.set()
        hub.close()


def test_reconnect_replaces_transport_by_stable_client_id():
    cleaned = []
    cleanup_done = threading.Event()

    def cleanup(transport: object) -> None:
        cleaned.append(transport)
        cleanup_done.set()

    hub = SessionEventHub(on_detach=cleanup)
    stale = RecordingTransport()
    replacement = RecordingTransport()
    try:
        hub.attach(stale, client_id="stable-window", mode=AttachmentMode.OBSERVE)
        hub.attach(replacement, client_id="stable-window", mode=AttachmentMode.CONTROL)

        assert hub.count() == 1
        assert hub.has_transport(stale) is False
        assert hub.has_transport(replacement) is True
        _wait(cleanup_done, "stale reconnect generation was not cleaned up")
        assert cleaned == [stale]
        assert hub.publish({"event": "after-reconnect"})
        replacement.wait_for_count(1)
        assert stale.frames == []
        assert replacement.frames == [{"event": "after-reconnect"}]
    finally:
        hub.close()


def test_failed_writer_detaches_once_without_affecting_other_subscriber():
    cleanup_calls: list[object] = []
    cleaned = threading.Event()

    def cleanup(transport: object) -> None:
        cleanup_calls.append(transport)
        cleaned.set()

    class FailedTransport:
        def write(self, frame: dict) -> bool:
            raise RuntimeError("peer failed")

    hub = SessionEventHub(on_detach=cleanup)
    failed = FailedTransport()
    healthy = RecordingTransport()
    try:
        hub.attach(failed, client_id="failed", mode=AttachmentMode.OBSERVE)
        hub.attach(healthy, client_id="healthy", mode=AttachmentMode.OBSERVE)
        assert hub.publish({"event": 1})

        _wait(cleaned, "failed writer was not cleaned up")
        _wait(healthy.written, "healthy writer was affected by peer failure")
        assert not hub.has_transport(failed)
        assert hub.has_transport(healthy)
        assert healthy.frames == [{"event": 1}]

        hub.detach(failed)
        hub.close()
        assert cleanup_calls.count(failed) == 1
    finally:
        hub.close()


def test_queue_overflow_detaches_only_slow_subscriber(monkeypatch):
    monkeypatch.setattr("tui_gateway.session_events.ATTACHMENT_QUEUE_MAX", 1)
    cleanup_calls: list[object] = []
    cleaned = threading.Event()

    def cleanup(transport: object) -> None:
        cleanup_calls.append(transport)
        cleaned.set()

    hub = SessionEventHub(on_detach=cleanup)
    slow = BlockingTransport()
    healthy = RecordingTransport()
    try:
        hub.attach(slow, client_id="slow", mode=AttachmentMode.OBSERVE)
        hub.attach(healthy, client_id="healthy", mode=AttachmentMode.OBSERVE)
        assert hub.publish({"event": 1})
        _wait(slow.written, "slow writer did not consume the first queue item")
        healthy.wait_for_count(1)
        assert hub.publish({"event": 2})
        healthy.wait_for_count(2)
        assert hub.publish({"event": 3})

        # Cleanup is deferred until the in-flight write exits, and a stale
        # detach generation cannot race a reattach of the same transport.
        with pytest.raises(RuntimeError, match="cleanup is still in progress"):
            hub.attach(slow, client_id="slow-again", mode=AttachmentMode.OBSERVE)
        slow.release.set()
        _wait(cleaned, "overflowed subscriber was not cleaned up")
        healthy.wait_for_count(3)

        assert cleanup_calls == [slow]
        assert not hub.has_transport(slow)
        assert hub.has_transport(healthy)
        assert healthy.frames == [{"event": 1}, {"event": 2}, {"event": 3}]

        hub.attach(slow, client_id="slow-again", mode=AttachmentMode.OBSERVE)
        assert hub.has_transport(slow)
        assert hub.publish({"event": 4})
        slow.wait_for_count(2)
        healthy.wait_for_count(4)
        assert slow.frames == [{"event": 1}, {"event": 4}]
        assert healthy.frames == [
            {"event": 1},
            {"event": 2},
            {"event": 3},
            {"event": 4},
        ]
    finally:
        slow.release.set()
        hub.close()


def test_detach_reports_a_writer_that_does_not_stop(monkeypatch):
    monkeypatch.setattr(
        "tui_gateway.session_events._WORKER_JOIN_TIMEOUT_SECONDS",
        0.05,
    )
    cleaned = threading.Event()
    hub = SessionEventHub(on_detach=lambda _transport: cleaned.set())
    blocked = BlockingTransport()
    hub.attach(blocked, client_id="blocked", mode=AttachmentMode.OBSERVE)
    assert hub.publish({"event": 1})
    _wait(blocked.written, "blocking writer was not entered")

    with pytest.raises(RuntimeError, match="writer did not stop"):
        hub.detach(blocked)
    assert not cleaned.is_set()
    with pytest.raises(RuntimeError, match="cleanup is still in progress"):
        hub.attach(blocked, client_id="blocked", mode=AttachmentMode.OBSERVE)

    blocked.release.set()
    _wait(cleaned, "deferred cleanup did not run after writer exit")
    hub.close()


def test_frame_normalization_runs_custom_hooks_outside_publication_lock():
    hub = SessionEventHub()
    transport = RecordingTransport()
    deepcopy_called = False

    class ReentrantFrame(dict):
        def __deepcopy__(self, memo):
            nonlocal deepcopy_called
            deepcopy_called = True
            hub.publish({"reentrant": True})
            return dict(self)

    try:
        hub.attach(transport, client_id="plain-json", mode=AttachmentMode.OBSERVE)
        assert hub.publish(ReentrantFrame({"event": 1}))
        transport.wait_for_count(1)

        assert deepcopy_called is False
        assert transport.frames == [{"event": 1}]
    finally:
        hub.close()


def test_invalid_frame_fails_before_any_subscriber_receives_it():
    hub = SessionEventHub()
    left = RecordingTransport()
    right = RecordingTransport()
    try:
        hub.attach(left, client_id="left", mode=AttachmentMode.OBSERVE)
        hub.attach(right, client_id="right", mode=AttachmentMode.OBSERVE)

        with pytest.raises(TypeError):
            hub.publish({"not-json": object()})

        assert left.frames == []
        assert right.frames == []
        assert hub.publish({"valid": True})
        left.wait_for_count(1)
        right.wait_for_count(1)
    finally:
        hub.close()


def test_detach_and_close_stop_workers_and_are_idempotent():
    hub = SessionEventHub()
    first = RecordingTransport()
    second = RecordingTransport()
    hub.attach(first, client_id="worker-cleanup-first", mode=AttachmentMode.OBSERVE)
    hub.attach(second, client_id="worker-cleanup-second", mode=AttachmentMode.CONTROL)

    assert hub.detach(first) is True
    assert hub.detach(first) is False
    hub.close()
    hub.close()

    assert hub.count() == 0
    assert hub.publish({"ignored": True}) is False
    assert not any(
        thread.is_alive()
        and thread.name.startswith("session-event-writer-worker-cleanup")
        for thread in threading.enumerate()
    )


def test_writes_do_not_hold_registry_or_publication_locks():
    hub = SessionEventHub()
    reentered = threading.Event()

    class ReentrantTransport:
        def write(self, frame: dict) -> bool:
            assert hub.has_transport(self)
            if frame == {"event": 1}:
                assert hub.publish({"event": 2})
                reentered.set()
            return True

    transport = ReentrantTransport()
    try:
        hub.attach(transport, client_id="reentrant", mode=AttachmentMode.CONTROL)
        assert hub.publish({"event": 1})
        _wait(reentered, "transport write blocked on a hub lock")
    finally:
        hub.close()


def test_capability_lookup_is_bound_to_attached_transport():
    hub = SessionEventHub()
    observer = RecordingTransport()
    controller = RecordingTransport()
    stranger = RecordingTransport()
    try:
        hub.attach(observer, client_id="observer", mode=AttachmentMode.OBSERVE)
        hub.attach(controller, client_id="controller", mode=AttachmentMode.CONTROL)

        observer_capabilities = hub.require(observer, "observe")
        controller_capabilities = hub.require(controller, "prompt.submit")
        assert observer_capabilities is None
        assert controller_capabilities is None
        with pytest.raises(PermissionError, match="requires capability"):
            hub.require(observer, "prompt.submit")
        with pytest.raises(PermissionError, match="not attached"):
            hub.require(stranger, "observe")
    finally:
        hub.close()


def test_snapshots_are_sanitized_plain_metadata():
    hub = SessionEventHub()

    class CredentialTransport(RecordingTransport):
        token = "secret-token"

    transport = CredentialTransport()
    try:
        hub.attach(transport, client_id="safe-id", mode=AttachmentMode.CONTROL)

        snapshots = hub.snapshots()

        assert snapshots == [
            {
                "client_id": "safe-id",
                "mode": "control",
                "capabilities": sorted(AttachmentMode.CONTROL.capabilities),
            }
        ]
        assert transport not in snapshots[0].values()
        assert "secret-token" not in repr(snapshots)
    finally:
        hub.close()


def test_queue_bound_is_a_fixed_positive_module_constant():
    assert isinstance(ATTACHMENT_QUEUE_MAX, int)
    assert ATTACHMENT_QUEUE_MAX > 0
