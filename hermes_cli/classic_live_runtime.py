"""Presentation-neutral lifecycle helpers for the classic interactive CLI."""

from __future__ import annotations

import asyncio
import inspect
import threading
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from hermes_cli.live_runtime_protocol import validate_runtime_event


_REQUIRED_CAPABILITIES = frozenset(
    {"observe", "prompt.submit", "interaction.respond"}
)


def build_classic_live_runtime_frontend(
    *,
    durable_session_id: str,
    profile: str | None,
    profile_home: str | Path,
    on_row: Callable[[dict[str, Any]], Any] | None = None,
    client_id: str | None = None,
    client_factory: Callable[..., Any] | None = None,
    connection_factory: Callable[..., Any] | None = None,
    owner_lookup: Callable[..., Any] | None = None,
) -> "ClassicLiveRuntimeFrontend":
    """Build a classic frontend against the profile-scoped canonical owner."""
    from gateway.live_runtime_client import AsyncLiveRuntimeClient
    from gateway.runtime_proxy_connection import RuntimeProxyAsyncConnection
    from hermes_cli.live_runtime_owners import lookup_runtime_owner
    from tui_gateway.runtime_proxy import _profile_scoped_conversation_key

    resolved_client_id = client_id or f"classic-cli:{uuid.uuid4()}"
    resolved_client_factory = client_factory or AsyncLiveRuntimeClient
    resolved_connection_factory = connection_factory or RuntimeProxyAsyncConnection
    resolved_owner_lookup = owner_lookup or lookup_runtime_owner
    scoped_key = _profile_scoped_conversation_key(durable_session_id, profile_home)
    holder: dict[str, ClassicLiveRuntimeFrontend] = {}

    def find_owner(key: str):
        return resolved_owner_lookup(key, registry_home=profile_home)

    def connect(owner):
        return resolved_connection_factory(
            owner=owner,
            durable_session_id=durable_session_id,
            profile=profile,
            client_id=resolved_client_id,
            requested_capabilities=_REQUIRED_CAPABILITIES,
        )

    client = resolved_client_factory(
        conversation_key=scoped_key,
        durable_root=durable_session_id,
        client_id=resolved_client_id,
        principal={
            "authenticated": True,
            "provider": "classic-cli",
            "subject": resolved_client_id,
        },
        surface="classic-cli",
        requested_capabilities=set(_REQUIRED_CAPABILITIES),
        owner_lookup=find_owner,
        connector=connect,
        on_event=lambda event: holder["frontend"].on_event(event),
        on_pending_interaction=lambda interaction: holder[
            "frontend"
        ].on_pending_interaction(interaction),
        on_local_message_id=lambda message_id: holder[
            "frontend"
        ].remember_local_message_id(message_id),
    )
    frontend = ClassicLiveRuntimeFrontend(
        client=client,
        client_id=resolved_client_id,
        on_row=on_row,
    )
    holder["frontend"] = frontend
    return frontend


class ClassicLiveRuntimeController:
    """Run the async classic frontend behind the synchronous prompt loop."""

    def __init__(self, frontend: Any, *, request_timeout: float = 30 * 60) -> None:
        self.frontend = frontend
        self.request_timeout = request_timeout
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None

    @property
    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.is_alive:
            return
        self._ready.clear()
        self._startup_error = None
        self._thread = threading.Thread(
            target=self._run,
            name="classic-live-runtime",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=self.request_timeout):
            raise TimeoutError("classic live runtime did not start")
        if self._startup_error is not None:
            raise RuntimeError("classic live runtime failed to start") from self._startup_error

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        started = False
        try:
            loop.run_until_complete(self.frontend.start())
            started = True
        except BaseException as exc:
            self._startup_error = exc
        finally:
            self._ready.set()
        try:
            if started:
                loop.run_forever()
        finally:
            if started:
                loop.run_until_complete(self.frontend.close())
            loop.close()
            self._loop = None

    def _call(self, awaitable: Any) -> Any:
        loop = self._loop
        if loop is None or not self.is_alive:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise RuntimeError("classic live runtime is not running")
        future = asyncio.run_coroutine_threadsafe(awaitable, loop)
        return future.result(timeout=self.request_timeout)

    def submit(self, text: str, **kwargs: Any) -> Any:
        return self._call(self.frontend.submit(text, **kwargs))

    def respond_to_interaction(self, **kwargs: Any) -> Any:
        return self._call(self.frontend.respond_to_interaction(**kwargs))

    def close(self) -> None:
        loop = self._loop
        thread = self._thread
        if loop is not None and thread is not None and thread.is_alive():
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=self.request_timeout)
            if thread.is_alive():
                raise TimeoutError("classic live runtime did not stop")
        self._thread = None


class ClassicLiveRuntimeFrontend:
    """Attach the classic CLI presentation to one canonical live runtime."""

    surface = "classic-cli"

    def __init__(
        self,
        *,
        client: Any,
        client_id: str,
        on_row: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        if not isinstance(client_id, str) or not client_id.strip():
            raise ValueError("client_id must be a non-empty string")
        self.client = client
        self.client_id = client_id
        self._on_row = on_row
        self._local_message_ids: set[str] = set()

    async def start(self) -> None:
        await self.client.start()

    async def close(self) -> None:
        await self.client.close()

    def on_pending_interaction(self, interaction: Mapping[str, Any]) -> None:
        if self._on_row is not None:
            self._on_row({"kind": "interaction", **dict(interaction)})

    async def respond_to_interaction(
        self,
        *,
        interaction_type: str,
        request_id: str,
        choice: str | None = None,
        answer: str | None = None,
        question_id: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "interaction_type": interaction_type,
            "request_id": request_id,
        }
        if choice is not None:
            payload["choice"] = choice
        if answer is not None:
            payload["answer"] = answer
        if question_id is not None:
            payload["question_id"] = question_id
        return await self.client.respond_to_interaction(payload)

    def remember_local_message_id(self, message_id: str) -> None:
        if message_id:
            self._local_message_ids.add(message_id)

    async def submit(
        self,
        text: str,
        *,
        submitted_at: float,
        message_id: str,
        busy_policy: str = "interrupt",
    ) -> Any:
        self.remember_local_message_id(message_id)
        registered = self.client.register_local_message_id(message_id)
        if inspect.isawaitable(registered):
            await registered
        return await self.client.request(
            {
                "busy_policy": busy_policy,
                "display_text": text,
                "message_id": message_id,
                "submitted_at": submitted_at,
                "text": text,
            }
        )

    def on_event(self, frame: Mapping[str, Any]) -> None:
        event = validate_runtime_event(frame)
        event_type = event["type"]
        payload = event["payload"]
        row: dict[str, Any] | None = None
        if event_type == "message.user":
            message_id = payload.get("message_id")
            if message_id in self._local_message_ids:
                self._local_message_ids.discard(message_id)
                return
            metadata = payload.get("display_metadata", {})
            row = {
                "kind": "peer-user",
                "message_id": message_id,
                "surface": metadata.get("surface"),
                "text": payload.get("text", ""),
                "timestamp": payload.get("timestamp"),
                "user_name": metadata.get("user_name"),
            }
        elif event_type == "message.complete":
            row = {
                "kind": "assistant",
                "status": payload.get("status"),
                "text": payload.get("text", ""),
            }
        if row is not None and self._on_row is not None:
            self._on_row(row)
