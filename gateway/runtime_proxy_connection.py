"""Async canonical-input adapter for the authenticated runtime proxy."""

from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import Callable
from typing import Any

from hermes_cli.live_runtime_owners import RuntimeOwner
from hermes_cli.live_runtime_protocol import (
    LIVE_RUNTIME_PROTOCOL_VERSION,
    runtime_event,
    validate_interaction_response,
    validate_runtime_event,
    validate_frontend_hello,
    validate_runtime_input,
)
from tui_gateway.runtime_proxy import RuntimeProxyClient


class RuntimeProxyConnectionError(RuntimeError):
    pass


class RuntimeProxyAsyncConnection:
    """Adapt canonical gateway frames to the existing authenticated proxy IPC.

    Legacy proxy output is normalized into the canonical ordered event envelope
    for :class:`AsyncLiveRuntimeClient`; presentation translation remains above
    this transport adapter.
    """

    _SUPPORTED_CAPABILITIES = frozenset(
        {"observe", "prompt.submit", "interaction.respond"}
    )

    def __init__(
        self,
        *,
        owner: RuntimeOwner,
        durable_session_id: str,
        profile: str | None,
        client_id: str,
        requested_capabilities: frozenset[str],
        proxy_factory: Callable[..., RuntimeProxyClient] = RuntimeProxyClient,
    ) -> None:
        self.owner = owner
        self.durable_session_id = durable_session_id
        self.profile = profile
        self.client_id = client_id
        self.requested_capabilities = requested_capabilities
        self._proxy_factory = proxy_factory
        self._proxy: RuntimeProxyClient | None = None
        self._live_session_id: str | None = None
        self._replay_epoch: str | None = None
        self._received: asyncio.Queue[dict[str, Any] | BaseException] = asyncio.Queue(
            maxsize=64
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False
        self._event_lock = threading.Lock()
        self._initializing = True
        self._pending_events: list[dict[str, Any]] = []
        self._pending_overflow = False
        self._accepted_capabilities: frozenset[str] = frozenset()

    @staticmethod
    def _legacy_capabilities(capabilities: frozenset[str]) -> frozenset[str]:
        legacy = set(capabilities)
        if "interaction.respond" in legacy:
            legacy.remove("interaction.respond")
            legacy.update({"approval.respond", "clarify.respond"})
        return frozenset(legacy)

    def _receive_event(self, frame: dict[str, Any]) -> None:
        """Move a proxy reader-thread event onto the owning asyncio loop."""
        loop = self._loop
        if loop is None or self._closed:
            return
        with self._event_lock:
            if self._initializing:
                if len(self._pending_events) >= self._received.maxsize:
                    self._pending_overflow = True
                else:
                    self._pending_events.append(dict(frame))
                return
        loop.call_soon_threadsafe(self._enqueue_event, dict(frame))

    def _receive_disconnect(self) -> None:
        loop = self._loop
        if loop is not None and not self._closed:
            loop.call_soon_threadsafe(
                self._fail_connection,
                RuntimeProxyConnectionError("runtime proxy disconnected"),
            )

    def _fail_connection(self, error: BaseException) -> None:
        if self._closed:
            return
        while True:
            try:
                self._received.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._received.put_nowait(error)
        asyncio.create_task(self.close())

    def _canonical_event(self, frame: dict[str, Any]) -> dict[str, Any]:
        if frame.get("kind") == "runtime.event":
            return validate_runtime_event(frame)
        event = frame
        if frame.get("method") == "event":
            params = frame.get("params")
            if not isinstance(params, dict):
                raise RuntimeProxyConnectionError("invalid runtime proxy event envelope")
            event = params
        runtime_id = event.get("session_id")
        replay_epoch = event.get("epoch")
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            raise RuntimeProxyConnectionError("runtime proxy event payload is invalid")
        return runtime_event(
            runtime_id=runtime_id,
            durable_session_id=self.durable_session_id,
            replay_epoch=replay_epoch,
            seq=event.get("seq"),
            event_type=event.get("type"),
            payload=payload,
        )

    def _enqueue_event(self, frame: dict[str, Any]) -> None:
        if self._closed:
            return
        try:
            self._received.put_nowait(self._canonical_event(frame))
        except Exception as exc:
            # Malformed input or a stalled downstream consumer fences this
            # connection. AsyncLiveRuntimeClient reconnect/replay owns recovery.
            self._fail_connection(
                RuntimeProxyConnectionError(
                    f"runtime proxy event delivery failed: {exc}"
                )
            )

    async def send(self, frame: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeProxyConnectionError("runtime proxy connection is closed")
        if frame.get("kind") == "frontend.hello":
            await self._hello(frame)
            return
        if frame.get("kind") == "control.request":
            await self._control(frame)
            return
        if frame.get("kind") == "interaction.respond":
            await self._interaction(frame)
            return
        raise RuntimeProxyConnectionError("unsupported canonical runtime frame")

    async def recv(self) -> dict[str, Any]:
        if self._closed and self._received.empty():
            raise EOFError("runtime proxy connection is closed")
        item = await self._received.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        proxy = self._proxy
        self._proxy = None
        if proxy is not None:
            await asyncio.to_thread(proxy.close)

    async def _hello(self, frame: dict[str, Any]) -> None:
        self._loop = asyncio.get_running_loop()
        hello = validate_frontend_hello(frame)
        if self._proxy is not None:
            raise RuntimeProxyConnectionError("duplicate frontend hello")
        if hello["client_id"] != self.client_id:
            raise RuntimeProxyConnectionError("frontend client identity mismatch")
        requested = frozenset(hello["requested_capabilities"])
        if requested != self.requested_capabilities:
            raise RuntimeProxyConnectionError("frontend capability identity mismatch")
        accepted = requested & self._SUPPORTED_CAPABILITIES
        self._accepted_capabilities = accepted
        proxy = self._proxy_factory(
            owner=self.owner,
            client_id=self.client_id,
            negotiated_capabilities=self._legacy_capabilities(accepted),
            on_event=self._receive_event,
            on_disconnect=self._receive_disconnect,
        )
        try:
            await asyncio.to_thread(proxy.connect)
            request_id = uuid.uuid4().hex
            resume = await asyncio.to_thread(
                proxy.request,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "session.resume",
                    "params": {
                        "session_id": self.durable_session_id,
                        "profile": self.profile,
                        "attachment_mode": (
                            "control"
                            if accepted & {"prompt.submit", "interaction.respond"}
                            else "observe"
                        ),
                        "omit_messages": True,
                    },
                },
            )
            if "error" in resume or not isinstance(resume.get("result"), dict):
                raise RuntimeProxyConnectionError(
                    f"runtime proxy resume failed: {resume.get('error')}"
                )
            result = resume["result"]
            live_session_id = result.get("session_id")
            if not isinstance(live_session_id, str) or not live_session_id.strip():
                raise RuntimeProxyConnectionError(
                    "runtime proxy resume returned no live session"
                )
            pending_interactions = []
            if "interaction.respond" in accepted:
                pending_ids = set()
                for plural_field, singular_field, interaction_type in (
                    ("pending_approvals", "pending_approval", "approval"),
                    (
                        "pending_clarifications",
                        "pending_clarify",
                        "clarification",
                    ),
                ):
                    pending_items = result.get(plural_field)
                    if pending_items is None:
                        pending = result.get(singular_field)
                        pending_items = [] if pending is None else [pending]
                    if not isinstance(pending_items, list):
                        raise RuntimeProxyConnectionError(
                            "invalid pending runtime interactions"
                        )
                    for pending in pending_items:
                        if not isinstance(pending, dict):
                            raise RuntimeProxyConnectionError(
                                "invalid pending runtime interaction"
                            )
                        interaction_id = pending.get("request_id")
                        if (
                            not isinstance(interaction_id, str)
                            or not interaction_id.strip()
                            or interaction_id in pending_ids
                        ):
                            raise RuntimeProxyConnectionError(
                                "invalid pending runtime interaction identity"
                            )
                        pending_ids.add(interaction_id)
                        pending_interactions.append(
                            {
                                "interaction_type": interaction_type,
                                "request_id": interaction_id,
                                "payload": dict(pending),
                            }
                        )
            replay = hello.get("replay")
            since = replay["seq"] if isinstance(replay, dict) else 0
            replay_response = await asyncio.to_thread(
                proxy.request,
                {
                    "jsonrpc": "2.0",
                    "id": uuid.uuid4().hex,
                    "method": "session.events.since",
                    "params": {
                        "session_id": live_session_id,
                        "last_seen_seq": since,
                    },
                },
            )
            if "error" in replay_response or not isinstance(
                replay_response.get("result"), dict
            ):
                raise RuntimeProxyConnectionError(
                    f"runtime proxy replay failed: {replay_response.get('error')}"
                )
            replay_result = replay_response["result"]
            replay_epoch = replay_result.get("epoch")
            if not isinstance(replay_epoch, str) or not replay_epoch.strip():
                raise RuntimeProxyConnectionError("runtime proxy replay returned no epoch")
            if isinstance(replay, dict) and replay["epoch"] != replay_epoch:
                since = 0
                replay_response = await asyncio.to_thread(
                    proxy.request,
                    {
                        "jsonrpc": "2.0",
                        "id": uuid.uuid4().hex,
                        "method": "session.events.since",
                        "params": {
                            "session_id": live_session_id,
                            "last_seen_seq": 0,
                        },
                    },
                )
                if "error" in replay_response or not isinstance(
                    replay_response.get("result"), dict
                ):
                    raise RuntimeProxyConnectionError(
                        "runtime proxy replay restart failed"
                    )
                replay_result = replay_response["result"]
                replay_epoch = replay_result.get("epoch")
        except BaseException:
            await asyncio.to_thread(proxy.close)
            raise
        self._proxy = proxy
        self._live_session_id = live_session_id
        self._replay_epoch = replay_epoch
        events = replay_result.get("events")
        latest_seq = replay_result.get("latest_seq")
        truncated = replay_result.get("truncated") is True
        if (
            not isinstance(events, list)
            or isinstance(latest_seq, bool)
            or not isinstance(latest_seq, int)
            or latest_seq < 0
        ):
            await self.close()
            raise RuntimeProxyConnectionError("invalid runtime proxy replay result")
        with self._event_lock:
            pending = self._pending_events
            pending_overflow = self._pending_overflow
            self._pending_events = []
            self._pending_overflow = False
        if pending_overflow:
            await self.close()
            raise RuntimeProxyConnectionError(
                "runtime proxy event buffer overflowed during replay"
            )
        canonical_pending = [self._canonical_event(item) for item in pending]
        canonical_replay = [self._canonical_event(item) for item in events]
        merged_by_seq = {
            item["seq"]: item
            for item in canonical_replay + canonical_pending
            if item["runtime_id"] == live_session_id
            and item["replay_epoch"] == replay_epoch
        }
        if replay is None:
            # A fresh platform attachment observes from this authoritative
            # snapshot forward. Replaying retained rows here would resend old
            # assistant completions and peer-user prompts to the platform.
            truncated = False
            baseline_seq = latest_seq
            ordered_events = [
                item
                for seq, item in sorted(merged_by_seq.items())
                if seq > baseline_seq
            ]
        elif truncated:
            baseline_seq = max([latest_seq, *merged_by_seq.keys()], default=latest_seq)
            ordered_events = [
                item
                for seq, item in sorted(merged_by_seq.items())
                if seq > baseline_seq
            ]
        else:
            ordered_events = [
                item
                for seq, item in sorted(merged_by_seq.items())
                if seq > since
            ]
            baseline_seq = since
        if len(ordered_events) + 1 > self._received.maxsize:
            truncated = True
            baseline_seq = max([latest_seq, *merged_by_seq.keys()], default=latest_seq)
            ordered_events = []
        resync_snapshot = None
        if truncated:
            snapshot_response = await asyncio.to_thread(
                proxy.request,
                {
                    "jsonrpc": "2.0",
                    "id": uuid.uuid4().hex,
                    "method": "session.presentation.snapshot",
                    "params": {"session_id": live_session_id},
                },
            )
            snapshot = snapshot_response.get("result")
            if "error" in snapshot_response or not isinstance(snapshot, dict):
                await self.close()
                raise RuntimeProxyConnectionError(
                    f"runtime presentation resync failed: {snapshot_response.get('error')}"
                )
            latest_assistant = snapshot.get("latest_assistant")
            completion_seq = (
                latest_assistant.get("completion_seq")
                if isinstance(latest_assistant, dict)
                else None
            )
            if (
                snapshot.get("session_id") != live_session_id
                or snapshot.get("reconcilable") is not True
                or (
                    latest_assistant is not None
                    and (
                        not isinstance(latest_assistant.get("text"), str)
                        or not latest_assistant["text"]
                        or isinstance(completion_seq, bool)
                        or not isinstance(completion_seq, int)
                        or completion_seq < 1
                        or completion_seq > baseline_seq
                    )
                )
            ):
                await self.close()
                raise RuntimeProxyConnectionError(
                    "invalid or unreconcilable runtime presentation snapshot"
                )
            if latest_assistant is not None and completion_seq <= since:
                latest_assistant = None
            resync_snapshot = {
                "latest_assistant": latest_assistant,
            }
        with self._event_lock:
            late_pending = self._pending_events
            late_overflow = self._pending_overflow
            self._pending_events = []
            self._pending_overflow = False
            self._initializing = False
        if late_overflow:
            await self.close()
            raise RuntimeProxyConnectionError(
                "runtime proxy event buffer overflowed during resync"
            )
        ordered_by_seq = {item["seq"]: item for item in ordered_events}
        for item in (self._canonical_event(raw) for raw in late_pending):
            if (
                item["runtime_id"] == live_session_id
                and item["replay_epoch"] == replay_epoch
                and item["seq"] > baseline_seq
            ):
                ordered_by_seq[item["seq"]] = item
        ordered_events = [ordered_by_seq[seq] for seq in sorted(ordered_by_seq)]
        if len(ordered_events) + 1 > self._received.maxsize:
            await self.close()
            raise RuntimeProxyConnectionError(
                "runtime proxy event buffer overflowed before hello acknowledgement"
            )
        acknowledgement = {
            "kind": "frontend.hello.ok",
            "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
            "owner_id": self.owner.owner_id,
            "generation": self.owner.generation,
            "accepted_capabilities": sorted(accepted),
            "runtime_id": live_session_id,
            "durable_session_id": self.durable_session_id,
            "replay_epoch": self._replay_epoch,
            "replay_truncated": truncated,
            "replay_seq": baseline_seq,
        }
        if resync_snapshot is not None:
            acknowledgement["resync_snapshot"] = resync_snapshot
        if pending_interactions:
            acknowledgement["pending_interactions"] = pending_interactions
        await self._received.put(acknowledgement)
        for event in ordered_events:
            await self._received.put(event)

    async def _control(self, frame: dict[str, Any]) -> None:
        if self._proxy is None or self._live_session_id is None:
            raise RuntimeProxyConnectionError("frontend hello is required")
        request_id = frame.get("request_id")
        payload = validate_runtime_input(frame.get("payload"))
        params = {
            "session_id": self._live_session_id,
            "text": payload["text"],
            "client_message_id": payload["message_id"],
            "queued": payload["busy_policy"] == "queue",
        }
        for key in (
            "display_text",
            "submitted_at",
            "attachment_refs",
            "display_metadata",
        ):
            if key in payload:
                params[key] = payload[key]
        if payload.get("image_refs"):
            params["image_paths"] = list(payload["image_refs"])
        response = await asyncio.to_thread(
            self._proxy.request,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "prompt.submit",
                "params": params,
            },
        )
        translated = {
            "kind": "control.response",
            "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
            "request_id": request_id,
        }
        if "error" in response:
            translated["error"] = response["error"]
        else:
            translated["result"] = response.get("result")
        await self._received.put(translated)

    async def _interaction(self, frame: dict[str, Any]) -> None:
        if self._proxy is None or self._live_session_id is None:
            raise RuntimeProxyConnectionError("frontend hello is required")
        if "interaction.respond" not in self._accepted_capabilities:
            raise RuntimeProxyConnectionError("interaction response is not authorized")
        request_id = frame.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise RuntimeProxyConnectionError("invalid interaction request identity")
        payload = validate_interaction_response(frame.get("payload"))
        params = {
            "session_id": self._live_session_id,
            "request_id": payload["request_id"],
        }
        if payload["interaction_type"] == "approval":
            method = "approval.respond"
            params["choice"] = payload["choice"]
            if "reason" in payload:
                params["reason"] = payload["reason"]
        else:
            method = "clarify.respond"
            params["answer"] = payload["answer"]
            if "question_id" in payload:
                params["question_id"] = payload["question_id"]
        response = await asyncio.to_thread(
            self._proxy.request,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            },
        )
        translated = {
            "kind": "control.response",
            "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
            "request_id": request_id,
        }
        if "error" in response:
            translated["error"] = response["error"]
        else:
            translated["result"] = response.get("result")
        await self._received.put(translated)
