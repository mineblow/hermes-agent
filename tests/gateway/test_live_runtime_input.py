from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

import gateway.live_runtime_bridge as live_runtime_bridge
from gateway.live_runtime_bridge import (
    AttachmentMode,
    LiveRuntimeAttachment,
    LiveRuntimeBridge,
    LiveRuntimeRoutingKey,
)
from gateway.platforms.base import MessageEvent, Platform
from gateway.session import SessionSource


class RequestClient:
    accepted_capabilities = frozenset({"observe", "prompt.submit"})
    owner_id = "owner-1"
    owner_generation = 2
    runtime_id = "runtime-1"
    durable_session_id = "session-1"
    replay_watermark = ("epoch-1", 0)

    def __init__(self):
        self.requests = []
        self.operations = []

    def register_local_message_id(self, message_id):
        self.operations.append(("register", message_id))

    async def request(self, payload):
        self.operations.append(("request", payload["message_id"]))
        self.requests.append(payload)
        return {"status": "accepted", "message_id": payload["message_id"]}

    async def close(self):
        pass


def _source():
    return SessionSource(
        platform=Platform.DISCORD,
        scope_id="guild-1",
        chat_id="channel-1",
        thread_id="thread-1",
        user_id="user-1",
        user_name="Ethan",
        profile="worker",
    )


def _attachment(*, mode=AttachmentMode.CONTROL, capabilities=None):
    source = _source()
    client = RequestClient()
    if capabilities is not None:
        client.accepted_capabilities = frozenset(capabilities)
    return LiveRuntimeAttachment(
        routing_key=LiveRuntimeRoutingKey.from_source(source, "user-1"),
        stable_client_id="gateway-v1:client",
        principal_id="user-1",
        durable_root="root-1",
        durable_session_id="session-1",
        profile_home="/profiles/worker",
        conversation_key="v1:conversation",
        mode=mode,
        source=source,
        requested_capabilities=frozenset({"observe", "prompt.submit"}),
        accepted_capabilities=client.accepted_capabilities,
        owner_id="owner-1",
        owner_generation=2,
        runtime_id="runtime-1",
        replay_epoch="epoch-1",
        replay_seq=0,
        client=client,
    )


@pytest.mark.asyncio
async def test_native_redelivery_maps_to_same_client_message_id():
    attachment = _attachment()
    first = MessageEvent(text="hello", source=_source(), message_id="native-42")
    retry = MessageEvent(text="hello", source=_source(), message_id="native-42")

    await LiveRuntimeBridge.submit_message(attachment, first)
    await LiveRuntimeBridge.submit_message(attachment, retry)

    first_frame, retry_frame = attachment.client.requests
    assert first_frame["message_id"] == retry_frame["message_id"]
    assert first_frame["message_id"].startswith("gateway-message-v1:")
    assert attachment.client.operations == [
        ("register", first_frame["message_id"]),
        ("request", first_frame["message_id"]),
        ("register", retry_frame["message_id"]),
        ("request", retry_frame["message_id"]),
    ]


@pytest.mark.asyncio
async def test_identical_text_from_distinct_native_messages_executes_as_distinct_turns():
    attachment = _attachment()

    await LiveRuntimeBridge.submit_message(
        attachment,
        MessageEvent(text="same", source=_source(), message_id="native-a"),
    )
    await LiveRuntimeBridge.submit_message(
        attachment,
        MessageEvent(text="same", source=_source(), message_id="native-b"),
    )

    assert attachment.client.requests[0]["message_id"] != (
        attachment.client.requests[1]["message_id"]
    )


@pytest.mark.asyncio
async def test_display_text_attachments_timestamp_and_author_survive_bridge():
    attachment = _attachment()
    event = MessageEvent(
        text="execution text",
        user_id="current-user",
        user_name="Current Speaker",
        source=_source(),
        message_id="native-1",
        media_urls=["attachment://image-1", "attachment://document-2"],
        timestamp=datetime(2026, 9, 1, 12, 0, tzinfo=UTC),
        metadata={
            "live_runtime_display_text": "display text",
            "live_runtime_image_refs": ["attachment://image-1"],
        },
    )

    result = await LiveRuntimeBridge.submit_message(attachment, event)

    frame = attachment.client.requests[0]
    assert result["status"] == "accepted"
    assert frame["text"] == "execution text"
    assert frame["display_text"] == "display text"
    assert frame["attachment_refs"] == [
        "attachment://image-1",
        "attachment://document-2",
    ]
    assert frame["image_refs"] == ["attachment://image-1"]
    assert frame["submitted_at"] == event.timestamp.timestamp()
    assert frame["display_metadata"] == {
        "platform": "discord",
        "profile": "worker",
        "principal_id": "user-1",
        "user_id": "current-user",
        "user_name": "Current Speaker",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "capabilities"),
    [
        (AttachmentMode.OBSERVE, {"observe"}),
        (AttachmentMode.CONTROL, {"observe"}),
    ],
)
async def test_observer_or_unnegotiated_input_rejected_before_scheduler_mutation(
    mode, capabilities
):
    attachment = _attachment(mode=mode, capabilities=capabilities)

    with pytest.raises(PermissionError):
        await LiveRuntimeBridge.submit_message(
            attachment,
            MessageEvent(text="forbidden", source=_source(), message_id="native-1"),
        )

    assert attachment.client.requests == []


@pytest.mark.asyncio
async def test_missing_native_id_is_stable_for_same_event_but_not_distinct_events():
    attachment = _attachment()
    first = MessageEvent(text="same", source=_source())
    second = MessageEvent(text="same", source=_source())

    await LiveRuntimeBridge.submit_message(attachment, first)
    await LiveRuntimeBridge.submit_message(attachment, first)
    await LiveRuntimeBridge.submit_message(attachment, second)

    ids = [request["message_id"] for request in attachment.client.requests]
    assert ids[0] == ids[1]
    assert ids[0] != ids[2]


@pytest.mark.asyncio
async def test_platform_update_id_is_stable_across_redelivery_objects():
    attachment = _attachment()
    first = MessageEvent(text="same", source=_source(), platform_update_id=12345)
    redelivery = MessageEvent(text="same", source=_source(), platform_update_id=12345)

    await LiveRuntimeBridge.submit_message(attachment, first)
    await LiveRuntimeBridge.submit_message(attachment, redelivery)

    ids = [request["message_id"] for request in attachment.client.requests]
    assert ids[0] == ids[1]


@pytest.mark.asyncio
async def test_wedged_scheduler_times_out_and_closes_unknown_outcome(monkeypatch):
    attachment = _attachment()
    closed = False

    async def wedged_request(_frame):
        await asyncio.Event().wait()

    async def close():
        nonlocal closed
        closed = True

    attachment.client.request = wedged_request
    attachment.client.close = close
    monkeypatch.setattr(live_runtime_bridge, "SUBMISSION_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(TimeoutError, match="outcome is unknown"):
        await LiveRuntimeBridge.submit_message(
            attachment,
            MessageEvent(text="hello", source=_source(), message_id="native-1"),
        )

    assert closed is True


@pytest.mark.asyncio
async def test_unknown_outcome_remains_bounded_when_client_close_wedges(monkeypatch):
    attachment = _attachment()
    close_started = False

    async def wedged_request(_frame):
        await asyncio.Event().wait()

    async def wedged_close():
        nonlocal close_started
        close_started = True
        await asyncio.Event().wait()

    attachment.client.request = wedged_request
    attachment.client.close = wedged_close
    monkeypatch.setattr(live_runtime_bridge, "SUBMISSION_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(live_runtime_bridge, "CLOSE_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(TimeoutError, match="outcome is unknown"):
        await asyncio.wait_for(
            LiveRuntimeBridge.submit_message(
                attachment,
                MessageEvent(text="hello", source=_source(), message_id="native-1"),
            ),
            timeout=0.2,
        )

    assert close_started is True
