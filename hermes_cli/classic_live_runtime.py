"""Presentation-neutral lifecycle helpers for the classic interactive CLI."""

from __future__ import annotations

import asyncio
import inspect
import queue
import threading
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from hermes_cli.live_runtime_protocol import runtime_input, validate_runtime_event


_REQUIRED_CAPABILITIES = frozenset(
    {"observe", "prompt.submit", "interaction.respond"}
)


def build_classic_live_runtime_frontend(
    *,
    durable_session_id: str,
    conversation_key: str,
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
    scoped_key = _profile_scoped_conversation_key(conversation_key, profile_home)
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


class _InProcessGatewayTransport:
    def __init__(self) -> None:
        self.connection_id = f"classic-cli-bootstrap:{uuid.uuid4()}"
        self.client_id = self.connection_id
        self.auth_identity = {
            "authenticated": True,
            "provider": "classic-cli",
            "subject": self.client_id,
        }
        self.closed = False
        self.responses: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=8)

    def write(self, frame: dict[str, Any]) -> bool:
        if self.closed:
            return False
        if "id" not in frame:
            return True
        try:
            self.responses.put_nowait(dict(frame))
        except queue.Full:
            self.closed = True
            return False
        return True

    def close(self) -> None:
        self.closed = True


class InProcessGatewayClient:
    """Synchronous RPC bootstrap against the canonical in-process gateway."""

    def __init__(
        self,
        *,
        dispatch: Callable[[dict[str, Any], Any], dict[str, Any] | None],
        detach: Callable[[Any], Any],
        timeout: float = 30 * 60,
    ) -> None:
        self.dispatch = dispatch
        self.detach = detach
        self.timeout = timeout
        self.transport = _InProcessGatewayTransport()
        self._request_lock = threading.Lock()
        self._closed = False

    def request(self, method: str, params: Mapping[str, Any]) -> Any:
        if self._closed:
            raise RuntimeError("in-process gateway client is closed")
        request_id = uuid.uuid4().hex
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": dict(params),
        }
        with self._request_lock:
            response = self.dispatch(request, self.transport)
            if response is None:
                try:
                    response = self.transport.responses.get(timeout=self.timeout)
                except queue.Empty as exc:
                    raise TimeoutError(
                        f"gateway request timed out: {method}"
                    ) from exc
            if response.get("id") != request_id:
                raise RuntimeError("gateway response identity mismatch")
            error = response.get("error")
            if error is not None:
                message = error.get("message") if isinstance(error, dict) else error
                raise RuntimeError(f"gateway request failed: {message}")
            if "result" not in response:
                raise RuntimeError("gateway response has no result")
            return response["result"]

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.detach(self.transport)
        finally:
            self.transport.close()


def build_in_process_gateway_client(*, timeout: float = 30 * 60) -> InProcessGatewayClient:
    from tui_gateway.server import _close_sessions_for_transport, dispatch

    return InProcessGatewayClient(
        dispatch=lambda request, transport: dispatch(request, transport),
        detach=lambda transport: _close_sessions_for_transport(
            transport, end_reason="classic_cli_bootstrap_close"
        ),
        timeout=timeout,
    )


class ClassicLiveRuntimeSession:
    """Own the classic frontend's create/resume and proxy attachment lifecycle."""

    def __init__(
        self,
        *,
        gateway: Any,
        profile: str | None,
        profile_home: str | Path,
        on_row: Callable[[dict[str, Any]], Any] | None = None,
        conversation_key_resolver: Callable[[str], str] | None = None,
        turn_timeout: float = 30 * 60,
        frontend_factory: Callable[..., Any] = build_classic_live_runtime_frontend,
        controller_factory: Callable[[Any], Any] | None = None,
    ) -> None:
        self.gateway = gateway
        self.profile = profile
        self.profile_home = profile_home
        self.on_row = on_row
        self.conversation_key_resolver = conversation_key_resolver
        self.turn_timeout = turn_timeout
        self.frontend_factory = frontend_factory
        self.controller_factory = controller_factory
        self.controller: Any | None = None
        self._terminal_rows: queue.Queue[dict[str, Any]] = queue.Queue()

    @staticmethod
    def _identity(result: Mapping[str, Any]) -> dict[str, str]:
        live_session_id = result.get("session_id")
        durable_session_id = result.get("stored_session_id")
        if not isinstance(live_session_id, str) or not live_session_id:
            raise RuntimeError("gateway returned no live session identity")
        if not isinstance(durable_session_id, str) or not durable_session_id:
            raise RuntimeError("gateway returned no durable session identity")
        return {
            "conversation_key": durable_session_id,
            "durable_session_id": durable_session_id,
            "live_session_id": live_session_id,
        }

    def _handle_row(self, row: dict[str, Any]) -> None:
        if row.get("kind") in {"assistant", "error"}:
            self._terminal_rows.put(dict(row))
        if self.on_row is not None:
            self.on_row(row)

    def _start_frontend(self, identity: dict[str, str]) -> None:
        frontend = self.frontend_factory(
            durable_session_id=identity["durable_session_id"],
            conversation_key=identity["conversation_key"],
            profile=self.profile,
            profile_home=self.profile_home,
            on_row=self._handle_row,
        )
        controller_factory = self.controller_factory or ClassicLiveRuntimeController
        self.controller = controller_factory(frontend)
        self.controller.start()

    def start_new(self, *, cols: int, cwd: str) -> dict[str, str]:
        result = self.gateway.request(
            "session.create",
            {
                "attachment_mode": "control",
                "cols": cols,
                "cwd": cwd,
                "profile": self.profile,
                "source": "classic-cli",
            },
        )
        identity = self._identity(result)
        self._start_frontend(identity)
        return identity

    def start_resume(self, session_id: str, *, cols: int) -> dict[str, str]:
        result = self.gateway.request(
            "session.resume",
            {
                "attachment_mode": "control",
                "cols": cols,
                "omit_messages": True,
                "profile": self.profile,
                "session_id": session_id,
                "source": "classic-cli",
            },
        )
        identity = self._identity(result)
        if self.conversation_key_resolver is None:
            raise RuntimeError("resume requires a compression-root resolver")
        conversation_key = self.conversation_key_resolver(
            identity["durable_session_id"]
        )
        if not isinstance(conversation_key, str) or not conversation_key:
            raise RuntimeError("resume returned no compression-root identity")
        identity["conversation_key"] = conversation_key
        self._start_frontend(identity)
        return identity

    def submit(self, text: str, **kwargs: Any) -> Any:
        if self.controller is None:
            raise RuntimeError("classic live runtime session is not started")
        return self.controller.submit(text, **kwargs)

    def _clear_terminal_rows(self) -> None:
        while True:
            try:
                self._terminal_rows.get_nowait()
            except queue.Empty:
                return

    def start_turn(self, text: str, **kwargs: Any) -> Any:
        self._clear_terminal_rows()
        return self.submit(text, **kwargs)

    def wait_for_terminal(self, *, timeout: float | None = None) -> dict[str, Any]:
        try:
            terminal = self._terminal_rows.get(
                timeout=self.turn_timeout if timeout is None else timeout
            )
        except queue.Empty as exc:
            raise TimeoutError("classic live runtime turn did not complete") from exc
        if terminal.get("kind") == "error":
            raise RuntimeError(
                str(terminal.get("message") or "classic live runtime turn failed")
            )
        return terminal

    def submit_and_wait(self, text: str, **kwargs: Any) -> dict[str, Any]:
        self.start_turn(text, **kwargs)
        return self.wait_for_terminal()

    def respond_to_interaction(self, **kwargs: Any) -> Any:
        if self.controller is None:
            raise RuntimeError("classic live runtime session is not started")
        return self.controller.respond_to_interaction(**kwargs)

    def close(self) -> None:
        if self.controller is not None:
            self.controller.close()
            self.controller = None
        self.gateway.close()


class ClassicLiveRuntimeController:
    """Run the async classic frontend behind the synchronous prompt loop."""

    def __init__(
        self,
        frontend: Any,
        *,
        request_timeout: float = 30 * 60,
        interaction_timeout: float = 30,
    ) -> None:
        self.frontend = frontend
        self.request_timeout = request_timeout
        self.interaction_timeout = interaction_timeout
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

    def _call(self, awaitable: Any, *, timeout: float | None = None) -> Any:
        loop = self._loop
        if loop is None or not self.is_alive:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise RuntimeError("classic live runtime is not running")
        future = asyncio.run_coroutine_threadsafe(awaitable, loop)
        return future.result(
            timeout=self.request_timeout if timeout is None else timeout
        )

    def submit(self, text: str, **kwargs: Any) -> Any:
        return self._call(self.frontend.submit(text, **kwargs))

    def respond_to_interaction(self, **kwargs: Any) -> Any:
        return self._call(
            self.frontend.respond_to_interaction(**kwargs),
            timeout=self.interaction_timeout,
        )

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
        attachment_refs: list[str] | None = None,
        image_refs: list[str] | None = None,
    ) -> Any:
        self.remember_local_message_id(message_id)
        registered = self.client.register_local_message_id(message_id)
        if inspect.isawaitable(registered):
            await registered
        payload = runtime_input(
            busy_policy=busy_policy,
            display_text=text,
            message_id=message_id,
            submitted_at=submitted_at,
            text=text,
            attachment_refs=attachment_refs,
            image_refs=image_refs,
        )
        return await self.client.request(payload)

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
        elif event_type == "error":
            row = {
                "kind": "error",
                "message": str(payload.get("message") or "runtime error"),
            }
        if row is not None and self._on_row is not None:
            self._on_row(row)
