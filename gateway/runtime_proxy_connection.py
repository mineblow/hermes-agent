"""Async canonical-input adapter for the authenticated runtime proxy."""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import Callable
from typing import Any

from hermes_cli.live_runtime_owners import RuntimeOwner
from hermes_cli.live_runtime_protocol import (
    LIVE_RUNTIME_PROTOCOL_VERSION,
    validate_frontend_hello,
    validate_runtime_input,
)
from tui_gateway.runtime_proxy import RuntimeProxyClient


class RuntimeProxyConnectionError(RuntimeError):
    pass


class RuntimeProxyAsyncConnection:
    """Adapt canonical gateway frames to the existing authenticated proxy IPC.

    Runtime output is intentionally ignored here. Task 9 owns ordered event
    translation into gateway stream events; this class only establishes the
    attachment and routes scheduler input/results.
    """

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
        self._received: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._closed = False

    @staticmethod
    def _legacy_capabilities(capabilities: frozenset[str]) -> frozenset[str]:
        return frozenset(
            "ui.respond" if capability == "interaction.respond" else capability
            for capability in capabilities
        )

    def _ignore_event(self, _frame: dict[str, Any]) -> None:
        # Task 9 adds ordered output translation. Existing owner subscribers
        # still receive these events; dropping only this gateway copy is safe.
        return

    async def send(self, frame: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeProxyConnectionError("runtime proxy connection is closed")
        if frame.get("kind") == "frontend.hello":
            await self._hello(frame)
            return
        if frame.get("kind") == "control.request":
            await self._control(frame)
            return
        raise RuntimeProxyConnectionError("unsupported canonical runtime frame")

    async def recv(self) -> dict[str, Any]:
        if self._closed and self._received.empty():
            raise EOFError("runtime proxy connection is closed")
        return await self._received.get()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        proxy = self._proxy
        self._proxy = None
        if proxy is not None:
            await asyncio.to_thread(proxy.close)

    async def _hello(self, frame: dict[str, Any]) -> None:
        hello = validate_frontend_hello(frame)
        if self._proxy is not None:
            raise RuntimeProxyConnectionError("duplicate frontend hello")
        if hello["client_id"] != self.client_id:
            raise RuntimeProxyConnectionError("frontend client identity mismatch")
        requested = frozenset(hello["requested_capabilities"])
        if requested != self.requested_capabilities:
            raise RuntimeProxyConnectionError("frontend capability identity mismatch")
        proxy = self._proxy_factory(
            owner=self.owner,
            client_id=self.client_id,
            negotiated_capabilities=self._legacy_capabilities(requested),
            on_event=self._ignore_event,
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
                        "attachment_mode": "control",
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
        except BaseException:
            await asyncio.to_thread(proxy.close)
            raise
        self._proxy = proxy
        self._live_session_id = live_session_id
        epoch_material = (
            f"{self.owner.owner_id}\0{self.owner.generation}\0{live_session_id}"
        ).encode("utf-8")
        self._replay_epoch = hashlib.sha256(epoch_material).hexdigest()
        await self._received.put(
            {
                "kind": "frontend.hello.ok",
                "protocol": LIVE_RUNTIME_PROTOCOL_VERSION,
                "owner_id": self.owner.owner_id,
                "generation": self.owner.generation,
                "accepted_capabilities": sorted(requested),
                "runtime_id": live_session_id,
                "durable_session_id": self.durable_session_id,
                "replay_epoch": self._replay_epoch,
                "replay_truncated": "replay" in hello,
            }
        )

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
