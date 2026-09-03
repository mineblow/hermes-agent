"""Tests for tui_gateway.event_replay — per-session event seq + replay ring."""

import threading

import pytest

from tui_gateway import event_replay
from tui_gateway.event_replay import (
    latest_seq,
    reset_replay_state,
    events_since,
    replay_stats,
)


@pytest.fixture(autouse=True)
def _clean():
    reset_replay_state()
    yield
    reset_replay_state()


def _frame(sid, etype="message.delta"):
    return {
        "jsonrpc": "2.0",
        "method": "event",
        "params": {"type": etype, "session_id": sid, "payload": {}},
    }


def test_stamp_adds_monotonic_seq_per_session():
    f1 = _frame("s1")
    f2 = _frame("s1")
    other = _frame("s2")

    event_replay._stamp_event(f1)
    event_replay._stamp_event(other)
    event_replay._stamp_event(f2)

    assert f1["params"]["seq"] == 1
    assert f1["params"]["epoch"] == event_replay.replay_epoch()
    assert f2["params"]["seq"] == 2  # per-session counter, unaffected by s2
    assert other["params"]["seq"] == 1


def test_stamp_ignores_non_event_and_sessionless_frames():
    rpc = {"jsonrpc": "2.0", "id": 1, "result": {}}
    no_sid = {"jsonrpc": "2.0", "method": "event", "params": {"type": "skin.changed"}}

    event_replay._stamp_event(rpc)
    event_replay._stamp_event(no_sid)

    assert "seq" not in rpc
    assert "seq" not in no_sid["params"]
    assert replay_stats()["events"] == 0


def test_events_since_returns_only_newer_frames_in_order():
    frames = [_frame("s1") for _ in range(5)]
    for f in frames:
        event_replay._stamp_event(f)

    got = events_since("s1", 3)
    assert [e["seq"] for e in got] == [4, 5]
    assert events_since("s1", 0) == [f["params"] for f in frames]
    assert events_since("s1", 99) == []
    assert latest_seq("s1") == 5


def test_events_since_returns_client_dispatchable_event_objects():
    """Cross-language contract: the client's replay loop dispatches an element
    only when it has a TOP-LEVEL ``type`` (json-rpc-gateway.ts fetchReplay:
    ``if (!event?.type) continue``). Returning full JSON-RPC envelopes here
    makes every replayed event silently droppable — the original #94219 bug.
    """
    event_replay._stamp_event(_frame("s1"))
    (event,) = events_since("s1", 0)

    # Bare event object, not an envelope.
    assert event["type"] == "message.delta"
    assert event["session_id"] == "s1"
    assert event["seq"] == 1
    assert "jsonrpc" not in event
    assert "method" not in event
    assert "params" not in event


def test_events_since_returns_defensive_copies():
    frame = _frame("s1")
    frame["params"]["payload"] = {"items": [1]}
    event_replay._stamp_event(frame)

    first = events_since("s1", 0)
    first[0]["payload"]["items"].append(2)
    first[0]["type"] = "mutated"

    second = events_since("s1", 0)
    assert second == [
        {
            "type": "message.delta",
            "session_id": "s1",
            "payload": {"items": [1]},
            "seq": 1,
            "epoch": event_replay.replay_epoch(),
        }
    ]


def test_unknown_session_returns_empty():
    assert events_since("nope", 0) == []
    assert latest_seq("nope") == 0


def test_ring_buffer_is_bounded():
    for i in range(event_replay._REPLAY_BUFFER_MAX + 50):
        event_replay._stamp_event(_frame("s1"))

    stats = replay_stats()
    assert stats["events"] == event_replay._REPLAY_BUFFER_MAX
    # Oldest evicted: last_seen=0 must report truncation via the RPC contract.
    buf = event_replay._replay_buffers["s1"]
    assert buf[0][0] > 1


def test_session_count_bounded_with_fifo_eviction():
    for i in range(event_replay._REPLAY_SESSIONS_MAX + 10):
        event_replay._stamp_event(_frame(f"s{i}"))

    stats = replay_stats()
    assert stats["sessions"] == event_replay._REPLAY_SESSIONS_MAX
    assert events_since("s0", 0) == []  # oldest session fully evicted
    assert latest_seq(f"s{event_replay._REPLAY_SESSIONS_MAX + 9}") == 1


def test_sequence_metadata_is_bounded_by_epoch_rollover():
    initial_epoch = event_replay.replay_epoch()
    for index in range(event_replay._REPLAY_SESSIONS_MAX + 1):
        event_replay._stamp_event(_frame(f"rollover-{index}"))

    stats = replay_stats()
    assert stats["sequence_sessions"] <= event_replay._REPLAY_SESSIONS_MAX
    assert event_replay.replay_epoch() != initial_epoch
    assert all(
        event["epoch"] == event_replay.replay_epoch()
        for buffer in event_replay._replay_buffers.values()
        for _seq, event in buffer
    )


def test_evicted_session_without_new_events_is_separated_by_epoch_rollover():
    initial_epoch = event_replay.replay_epoch()
    event_replay._stamp_event(_frame("old-live"))
    for index in range(event_replay._REPLAY_SESSIONS_MAX):
        event_replay._stamp_event(_frame(f"new-{index}"))

    assert events_since("old-live", 0) == []
    assert latest_seq("old-live") == 0
    assert event_replay.replay_epoch() != initial_epoch
    assert event_replay.is_truncated("old-live", 0) is False


def test_sequence_restart_after_eviction_always_changes_epoch():
    first = _frame("old-live")
    event_replay._stamp_event(first)
    assert first["params"]["seq"] == 1
    first_epoch = first["params"]["epoch"]

    for index in range(event_replay._REPLAY_SESSIONS_MAX):
        event_replay._stamp_event(_frame(f"new-{index}"))

    assert events_since("old-live", 0) == []
    resumed = _frame("old-live")
    event_replay._stamp_event(resumed)

    assert resumed["params"]["seq"] == 1
    assert resumed["params"]["epoch"] != first_epoch
    assert [event["seq"] for event in events_since("old-live", 0)] == [1]


def test_release_session_reclaims_counter_and_buffer_state():
    frame = _frame("finished")
    event_replay._stamp_event(frame)
    assert latest_seq("finished") == 1
    initial_epoch = event_replay.replay_epoch()

    event_replay.release_session("finished")

    assert latest_seq("finished") == 0
    assert events_since("finished", 0) == []
    assert event_replay.is_truncated("finished", 0) is False
    assert event_replay.replay_epoch() != initial_epoch

    replacement = _frame("finished")
    event_replay._stamp_event(replacement)
    assert replacement["params"]["seq"] == 1
    assert replacement["params"]["epoch"] != initial_epoch


def test_concurrent_stamping_never_drops_or_duplicates_seq():
    errors = []

    def worker(sid):
        try:
            seen = set()
            for _ in range(200):
                f = _frame(sid)
                event_replay._stamp_event(f)
                seq = f["params"]["seq"]
                assert seq not in seen
                seen.add(seq)
        except AssertionError as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"t{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert replay_stats()["events"] == 8 * 200


def test_truncation_detection_semantics():
    """The RPC handler's truncated flag: gap between last_seen and buffer start."""
    # Overflow the ring so the oldest events are genuinely evicted.
    for _ in range(event_replay._REPLAY_BUFFER_MAX + 10):
        event_replay._stamp_event(_frame("s1"))

    with event_replay._replay_lock:
        oldest = event_replay._replay_buffers["s1"][0][0]

    assert oldest > 1  # eviction happened

    # Client saw everything up to just before the buffer → NOT truncated.
    assert not event_replay.is_truncated("s1", oldest - 1)
    # Client saw seq 5, buffer starts later → truncated.
    assert event_replay.is_truncated("s1", 5)
    # Unknown session: nothing evicted, nothing truncated.
    assert not event_replay.is_truncated("nope", 0)
