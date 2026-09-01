from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from gateway.live_runtime_client import (
    AsyncLiveRuntimeClient,
    LiveRuntimeBackpressure,
    OwnerGenerationFenceError,
    UnknownExecutionOutcome,
)
from hermes_cli.live_runtime_owners import RuntimeOwner


PRINCIPAL = {"provider": "gateway-auth", "subject": "user-1", "authenticated": True}


def _owner(generation: int = 1, owner_id: str | None = None) -> RuntimeOwner:
    return RuntimeOwner(
        conversation_key="durable-root",
        owner_id=owner_id or f"owner-{generation}",
        generation=generation,
        pid=100 + generation,
        process_start_time=1.0,
        endpoint=f"memory://owner-{generation}",
        profile_home="/profile",
        surface="tui",
        started_at=float(generation),
    )


class FakeConnection:
    def __init__(
        self,
        owner: RuntimeOwner,
        *,
        accepted_capabilities=("observe", "prompt.submit"),
        runtime_id="runtime-1",
        replay_epoch="epoch-1",
        replay_truncated=False,
        on_send: Callable | None = None,
    ):
        self.owner = owner
        self.accepted_capabilities = list(accepted_capabilities)
        self.runtime_id = runtime_id
        self.replay_epoch = replay_epoch
        self.replay_truncated = replay_truncated
        self.on_send = on_send
        self.sent = []
        self.inbound = asyncio.Queue()
        self.closed = False

    async def send(self, frame):
        self.sent.append(frame)
        if frame["kind"] == "frontend.hello":
            await self.inbound.put({
                "kind": "frontend.hello.ok",
                "protocol": 1,
                "owner_id": self.owner.owner_id,
                "generation": self.owner.generation,
                "runtime_id": self.runtime_id,
                "durable_session_id": "session-1",
                "replay_epoch": self.replay_epoch,
                "accepted_capabilities": self.accepted_capabilities,
                "replay_truncated": self.replay_truncated,
            })
        if self.on_send is not None:
            result = self.on_send(self, frame)
            if asyncio.iscoroutine(result):
                await result

    async def recv(self):
        item = await self.inbound.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self):
        self.closed = True


async def _client(owner_lookup, connector, **kwargs):
    return AsyncLiveRuntimeClient(
        conversation_key="durable-root",
        durable_root="durable-root",
        client_id="gateway-client-1",
        principal=PRINCIPAL,
        surface="gateway",
        requested_capabilities={"observe", "prompt.submit", "clarify.respond"},
        owner_lookup=owner_lookup,
        connector=connector,
        reconnect_delay=0,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_connect_sends_neutral_hello_and_intersects_capabilities():
    owner = _owner()
    connection = FakeConnection(owner, accepted_capabilities=("observe",))
    client = await _client(lambda _key: owner, lambda _owner: connection)

    try:
        await client.start()
        assert client.accepted_capabilities == frozenset({"observe"})
        assert connection.sent[0]["kind"] == "frontend.hello"
        assert connection.sent[0]["client_id"] == "gateway-client-1"
        assert connection.sent[0]["durable_root"] == "durable-root"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_concurrent_requests_correlate_out_of_order_responses():
    owner = _owner()
    requests = []

    async def on_send(connection, frame):
        if frame["kind"] != "control.request":
            return
        requests.append(frame)
        if len(requests) == 2:
            for request in reversed(requests):
                await connection.inbound.put({
                    "kind": "control.response",
                    "protocol": 1,
                    "request_id": request["request_id"],
                    "result": {"value": request["payload"]["value"]},
                })

    connection = FakeConnection(owner, on_send=on_send)
    client = await _client(lambda _key: owner, lambda _owner: connection)
    try:
        await client.start()
        first, second = await asyncio.gather(
            client.request({"value": "first"}),
            client.request({"value": "second"}),
        )
        assert first == {"value": "first"}
        assert second == {"value": "second"}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_ordered_event_callback_advances_replay_watermark_after_delivery():
    owner = _owner()
    connection = FakeConnection(owner)
    delivered = []
    client = await _client(
        lambda _key: owner,
        lambda _owner: connection,
        on_event=lambda event: delivered.append(event["type"]),
    )
    try:
        await client.start()
        for seq, event_type in ((1, "message.delta"), (2, "tool.start")):
            await connection.inbound.put({
                "kind": "runtime.event",
                "protocol": 1,
                "runtime_id": "runtime-1",
                "durable_session_id": "session-1",
                "replay_epoch": "epoch-1",
                "seq": seq,
                "type": event_type,
                "payload": {},
            })
        await asyncio.wait_for(client.wait_for_sequence(2), timeout=1)
        assert delivered == ["message.delta", "tool.start"]
        assert client.replay_watermark == ("epoch-1", 2)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_reconnect_fences_generation_and_sends_last_replay_watermark():
    owners = [_owner(1), _owner(2)]
    first = FakeConnection(owners[0], replay_epoch="epoch-1")
    second = FakeConnection(owners[1], replay_epoch="epoch-2")
    connections = [first, second]
    current = 0

    def lookup(_key):
        return owners[current]

    async def connect(_owner):
        return connections.pop(0)

    client = await _client(lookup, connect)
    try:
        await client.start()
        for seq in range(1, 8):
            await first.inbound.put({
                "kind": "runtime.event",
                "protocol": 1,
                "runtime_id": "runtime-1",
                "durable_session_id": "session-1",
                "replay_epoch": "epoch-1",
                "seq": seq,
                "type": "message.complete",
                "payload": {},
            })
        await client.wait_for_sequence(7)
        current = 1
        await first.inbound.put(EOFError("owner exited"))
        await asyncio.wait_for(client.wait_for_generation(2), timeout=1)
        assert first.closed is True
        assert second.sent[0]["replay"] == {"epoch": "epoch-1", "seq": 7}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_generation_regression_is_rejected():
    owner = _owner(2)
    connection = FakeConnection(owner)
    client = await _client(lambda _key: owner, lambda _owner: connection)
    try:
        await client.start()
        client._owner_generation = 3
        await connection.inbound.put(EOFError("disconnect"))
        with pytest.raises(OwnerGenerationFenceError):
            await asyncio.wait_for(client.wait_closed(), timeout=1)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_sent_request_is_not_retried_after_unknown_execution_outcome():
    owners = [_owner(1), _owner(2)]
    first = FakeConnection(owners[0])
    second = FakeConnection(owners[1])
    current = 0

    async def drop_after_send(connection, frame):
        nonlocal current
        if frame["kind"] == "control.request":
            current = 1
            await connection.inbound.put(EOFError("lost after send"))

    first.on_send = drop_after_send
    connections = [first, second]
    client = await _client(
        lambda _key: owners[current],
        lambda _owner: connections.pop(0),
    )
    try:
        await client.start()
        with pytest.raises(UnknownExecutionOutcome):
            await client.request({"operation": "mutate"})
        await asyncio.wait_for(client.wait_for_generation(2), timeout=1)
        assert [frame["kind"] for frame in second.sent] == ["frontend.hello"]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_replay_truncation_requests_authoritative_durable_resync():
    owner = _owner()
    connection = FakeConnection(owner, replay_truncated=True)
    resync = []
    client = await _client(
        lambda _key: owner,
        lambda _owner: connection,
        on_resync_required=lambda signal: resync.append(signal),
    )
    try:
        await client.start()
        assert resync == [{
            "durable_session_id": "session-1",
            "runtime_id": "runtime-1",
            "replay_epoch": "epoch-1",
        }]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_outbound_queue_is_bounded_and_close_cancels_waiters():
    owner = _owner()
    release = asyncio.Event()

    async def block_requests(_connection, frame):
        if frame["kind"] == "control.request":
            await release.wait()

    connection = FakeConnection(owner, on_send=block_requests)
    client = await _client(
        lambda _key: owner,
        lambda _owner: connection,
        queue_size=1,
    )
    await client.start()
    first = asyncio.create_task(client.request({"n": 1}))
    await asyncio.sleep(0)
    second = asyncio.create_task(client.request({"n": 2}))
    await asyncio.sleep(0)
    with pytest.raises(LiveRuntimeBackpressure):
        await client.request({"n": 3})

    await client.close()
    assert first.done() and second.done()
    assert connection.closed is True


@pytest.mark.asyncio
async def test_inbound_queue_overflow_signals_durable_resync_without_unbounded_growth():
    owner = _owner()
    callback_started = asyncio.Event()
    release_callback = asyncio.Event()
    resync_required = asyncio.Event()

    async def block_first_event(_event):
        callback_started.set()
        await release_callback.wait()

    connection = FakeConnection(owner)
    client = await _client(
        lambda _key: owner,
        lambda _owner: connection,
        queue_size=1,
        on_event=block_first_event,
        on_resync_required=lambda _signal: resync_required.set(),
    )
    try:
        await client.start()
        for seq in range(1, 4):
            await connection.inbound.put({
                "kind": "runtime.event",
                "protocol": 1,
                "runtime_id": "runtime-1",
                "durable_session_id": "session-1",
                "replay_epoch": "epoch-1",
                "seq": seq,
                "type": "message.delta",
                "payload": {},
            })
            if seq == 1:
                await callback_started.wait()

        await asyncio.wait_for(resync_required.wait(), timeout=1)
        assert client._inbound.qsize() <= 1
    finally:
        release_callback.set()
        await client.close()


@pytest.mark.asyncio
async def test_cancelled_request_waiter_does_not_corrupt_later_correlation():
    owner = _owner()
    first_sent = asyncio.Event()
    first_request = None

    async def on_send(connection, frame):
        nonlocal first_request
        if frame["kind"] != "control.request":
            return
        if first_request is None:
            first_request = frame
            first_sent.set()
            return
        await connection.inbound.put({
            "kind": "control.response",
            "protocol": 1,
            "request_id": frame["request_id"],
            "result": {"status": "still-connected"},
        })

    connection = FakeConnection(owner, on_send=on_send)
    client = await _client(lambda _key: owner, lambda _owner: connection)
    try:
        await client.start()
        cancelled = asyncio.create_task(client.request({"operation": "cancel-me"}))
        await first_sent.wait()
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled

        await connection.inbound.put({
            "kind": "control.response",
            "protocol": 1,
            "request_id": first_request["request_id"],
            "result": {"ignored": True},
        })
        assert await client.request({"operation": "next"}) == {
            "status": "still-connected"
        }
    finally:
        await client.close()
