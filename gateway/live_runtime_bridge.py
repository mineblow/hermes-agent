"""Platform-neutral attachment state for gateway live-runtime frontends."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from gateway.session import SessionSource
from gateway.session_state import SessionState
from hermes_cli.live_runtime_protocol import SUPPORTED_CAPABILITIES, runtime_input

logger = logging.getLogger(__name__)

SUBMISSION_TIMEOUT_SECONDS = 30.0
CLOSE_TIMEOUT_SECONDS = 2.0


class AttachmentMode(str, Enum):
    CONTROL = "control"
    OBSERVE = "observe"


def profile_scoped_runtime_key(durable_root: str, profile_home: str | Path) -> str:
    """Return the byte-compatible canonical-owner lookup key."""
    profile = str(Path(profile_home).expanduser().resolve())
    material = f"{profile}\0{durable_root}".encode("utf-8")
    return f"v1:{hashlib.sha256(material).hexdigest()}"


@dataclass(frozen=True)
class LiveRuntimeRoutingKey:
    """Authenticated routing identity; delivery-only metadata is excluded."""

    profile: str
    platform: str
    scope_id: str
    chat_id: str
    thread_id: str
    principal_id: str

    @classmethod
    def from_source(
        cls, source: SessionSource, principal_id: str
    ) -> "LiveRuntimeRoutingKey":
        if not isinstance(principal_id, str) or not principal_id.strip():
            raise ValueError("principal_id must be a non-empty string")
        platform = getattr(source.platform, "value", source.platform)
        values = {
            "profile": source.profile or "default",
            "platform": platform,
            "scope_id": source.scope_id or "",
            "chat_id": source.chat_id,
            "thread_id": source.thread_id or source.prospective_thread_id or "",
            "principal_id": principal_id,
        }
        if any(not isinstance(value, str) for value in values.values()):
            raise ValueError("live runtime routing fields must be strings")
        if not values["platform"].strip() or not values["chat_id"].strip():
            raise ValueError("live runtime route requires platform and chat identity")
        return cls(**values)

    @property
    def token(self) -> str:
        return json.dumps(
            {
                "profile": self.profile,
                "platform": self.platform,
                "scope_id": self.scope_id,
                "chat_id": self.chat_id,
                "thread_id": self.thread_id,
                "principal_id": self.principal_id,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )


def stable_gateway_client_id(
    routing_key: LiveRuntimeRoutingKey, profile_home: str | Path
) -> str:
    """Return a restart-stable opaque ID isolated by resolved profile home."""
    profile = str(Path(profile_home).expanduser().resolve())
    material = f"{profile}\0{routing_key.token}".encode("utf-8")
    return f"gateway-v1:{hashlib.sha256(material).hexdigest()}"


@dataclass(frozen=True)
class LiveRuntimeAttachmentRequest:
    routing_key: LiveRuntimeRoutingKey
    stable_client_id: str
    principal_id: str
    surface: str
    durable_root: str
    durable_session_id: str
    profile_home: str
    conversation_key: str
    mode: AttachmentMode
    requested_capabilities: frozenset[str]


class AttachmentClient(Protocol):
    accepted_capabilities: frozenset[str]
    owner_id: str | None
    owner_generation: int
    runtime_id: str | None
    durable_session_id: str | None
    replay_watermark: tuple[str, int] | None

    async def start(self) -> None: ...

    async def request(self, payload: dict[str, Any]) -> Any: ...

    async def close(self) -> None: ...


@dataclass
class LiveRuntimeAttachment:
    routing_key: LiveRuntimeRoutingKey
    stable_client_id: str
    principal_id: str
    durable_root: str
    durable_session_id: str
    profile_home: str
    conversation_key: str
    mode: AttachmentMode
    source: SessionSource
    requested_capabilities: frozenset[str]
    accepted_capabilities: frozenset[str]
    owner_id: str | None
    owner_generation: int
    runtime_id: str | None
    replay_epoch: str | None
    replay_seq: int
    client: AttachmentClient

    def refresh_runtime_state(self) -> None:
        self.accepted_capabilities = frozenset(self.client.accepted_capabilities)
        self.owner_id = self.client.owner_id
        self.owner_generation = self.client.owner_generation
        self.runtime_id = self.client.runtime_id
        durable_session_id = self.client.durable_session_id
        if durable_session_id:
            self.durable_session_id = durable_session_id
        watermark = self.client.replay_watermark
        if watermark is None:
            self.replay_epoch = None
            self.replay_seq = 0
        else:
            self.replay_epoch, self.replay_seq = watermark


ClientFactory = Callable[
    [LiveRuntimeAttachmentRequest],
    AttachmentClient | Awaitable[AttachmentClient],
]


class LiveRuntimeBridge:
    """Own one async runtime client per authenticated gateway routing key."""

    def __init__(self, state_for: Callable[[str], SessionState]) -> None:
        self._state_for = state_for
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}

    def attachment_for(
        self, session_key: str, routing_key: LiveRuntimeRoutingKey
    ) -> LiveRuntimeAttachment | None:
        """Return the current attachment for an authenticated route, if any."""
        state = self._state_for(session_key)
        return state.live_runtime_attachments.get(routing_key.token)

    @staticmethod
    def _capabilities(mode: AttachmentMode) -> frozenset[str]:
        if mode is AttachmentMode.OBSERVE:
            return frozenset({"observe"})
        if mode is AttachmentMode.CONTROL:
            return SUPPORTED_CAPABILITIES
        raise ValueError("unsupported attachment mode")

    @staticmethod
    def _client_message_id(
        attachment: LiveRuntimeAttachment, event: Any
    ) -> str:
        native_id = getattr(event, "message_id", None)
        native_namespace = "message"
        if not isinstance(native_id, str) or not native_id.strip():
            platform_update_id = getattr(event, "platform_update_id", None)
            if isinstance(platform_update_id, (str, int)) and not isinstance(
                platform_update_id, bool
            ):
                native_id = str(platform_update_id)
                native_namespace = "platform-update"
        if isinstance(native_id, str) and native_id.strip():
            material = (
                f"{attachment.routing_key.token}\0{native_namespace}\0{native_id}"
            ).encode("utf-8")
            return f"gateway-message-v1:{hashlib.sha256(material).hexdigest()}"
        metadata = getattr(event, "metadata", None)
        if not isinstance(metadata, dict):
            metadata = {}
            event.metadata = metadata
        generated = metadata.get("live_runtime_client_message_id")
        if not isinstance(generated, str) or not generated.strip():
            generated = f"gateway-message-v1:{uuid.uuid4().hex}"
            metadata["live_runtime_client_message_id"] = generated
        return generated

    @staticmethod
    async def submit_message(
        attachment: LiveRuntimeAttachment, event: Any
    ) -> Any:
        """Submit one authorized platform event to the canonical scheduler."""
        if (
            attachment.mode is not AttachmentMode.CONTROL
            or "prompt.submit" not in attachment.accepted_capabilities
        ):
            raise PermissionError("live runtime attachment cannot submit prompts")
        source = attachment.source
        platform = getattr(source.platform, "value", source.platform)
        metadata = getattr(event, "metadata", None)
        display_text = (
            metadata.get("live_runtime_display_text")
            if isinstance(metadata, dict)
            else None
        )
        refs = list(dict.fromkeys(getattr(event, "media_urls", None) or ()))
        raw_image_refs = (
            metadata.get("live_runtime_image_refs", ())
            if isinstance(metadata, dict)
            else ()
        )
        image_refs = [
            ref
            for ref in dict.fromkeys(raw_image_refs)
            if isinstance(ref, str) and ref in refs
        ]
        timestamp = getattr(event, "timestamp", None)
        submitted_at = timestamp.timestamp() if timestamp is not None else None
        frame = runtime_input(
            message_id=LiveRuntimeBridge._client_message_id(attachment, event),
            text=getattr(event, "text", ""),
            display_text=display_text,
            submitted_at=submitted_at,
            attachment_refs=refs,
            image_refs=image_refs,
            display_metadata={
                "platform": platform,
                "profile": source.profile or "default",
                "principal_id": attachment.principal_id,
                "user_id": getattr(event, "user_id", None) or source.user_id,
                "user_name": getattr(event, "user_name", None) or source.user_name,
            },
        )
        try:
            return await asyncio.wait_for(
                attachment.client.request(frame),
                timeout=SUBMISSION_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            try:
                await asyncio.wait_for(
                    attachment.client.close(),
                    timeout=CLOSE_TIMEOUT_SECONDS,
                )
            except Exception:
                logger.warning(
                    "Canonical runtime client close did not finish after submission timeout",
                    exc_info=True,
                )
            raise TimeoutError(
                "canonical scheduler acknowledgement timed out; execution outcome is unknown"
            ) from exc

    async def attach(
        self,
        *,
        session_key: str,
        source: SessionSource,
        principal_id: str,
        durable_root: str,
        durable_session_id: str,
        profile_home: str | Path,
        mode: AttachmentMode,
        client_factory: ClientFactory,
    ) -> LiveRuntimeAttachment:
        if not isinstance(session_key, str) or not session_key.strip():
            raise ValueError("session_key must be a non-empty string")
        if not isinstance(durable_root, str) or not durable_root.strip():
            raise ValueError("durable_root must be a non-empty string")
        if not isinstance(durable_session_id, str) or not durable_session_id.strip():
            raise ValueError("durable_session_id must be a non-empty string")
        routing_key = LiveRuntimeRoutingKey.from_source(source, principal_id)
        resolved_profile_home = str(Path(profile_home).expanduser().resolve())
        lock_key = (session_key, routing_key.token)
        lock = self._locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            attachments = self._state_for(session_key).live_runtime_attachments
            existing = attachments.get(routing_key.token)
            if existing is not None:
                if (
                    existing.durable_root == durable_root
                    and existing.durable_session_id == durable_session_id
                    and existing.profile_home == resolved_profile_home
                    and existing.mode is mode
                ):
                    # SessionSource is delivery-only and may carry fresher
                    # message/display metadata without changing identity.
                    existing.source = source
                    existing.refresh_runtime_state()
                    return existing
                attachments.pop(routing_key.token)
                await existing.client.close()

            requested_capabilities = self._capabilities(mode)
            request = LiveRuntimeAttachmentRequest(
                routing_key=routing_key,
                stable_client_id=stable_gateway_client_id(
                    routing_key, resolved_profile_home
                ),
                principal_id=principal_id,
                surface=routing_key.platform,
                durable_root=durable_root,
                durable_session_id=durable_session_id,
                profile_home=resolved_profile_home,
                conversation_key=profile_scoped_runtime_key(
                    durable_root, resolved_profile_home
                ),
                mode=mode,
                requested_capabilities=requested_capabilities,
            )
            client = client_factory(request)
            if inspect.isawaitable(client):
                client = await client
            try:
                await client.start()
                if not frozenset(client.accepted_capabilities) <= requested_capabilities:
                    raise ValueError(
                        "runtime owner accepted unauthorized capabilities"
                    )
            except BaseException:
                await client.close()
                raise
            attachment = LiveRuntimeAttachment(
                routing_key=routing_key,
                stable_client_id=request.stable_client_id,
                principal_id=principal_id,
                durable_root=durable_root,
                durable_session_id=durable_session_id,
                profile_home=resolved_profile_home,
                conversation_key=request.conversation_key,
                mode=mode,
                source=source,
                requested_capabilities=requested_capabilities,
                accepted_capabilities=frozenset(),
                owner_id=None,
                owner_generation=0,
                runtime_id=None,
                replay_epoch=None,
                replay_seq=0,
                client=client,
            )
            attachment.refresh_runtime_state()
            attachments[routing_key.token] = attachment
            return attachment

    async def detach(
        self,
        session_key: str,
        routing_key: LiveRuntimeRoutingKey,
        *,
        expected: LiveRuntimeAttachment | None = None,
    ) -> bool:
        lock_key = (session_key, routing_key.token)
        lock = self._locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            attachments = self._state_for(session_key).live_runtime_attachments
            attachment = attachments.get(routing_key.token)
            if attachment is None:
                return False
            if expected is not None and attachment is not expected:
                return False
            attachments.pop(routing_key.token)
            await attachment.client.close()
            return True

    async def close_all(self) -> None:
        """Close every attachment visible through states touched by this bridge."""
        for session_key, token in list(self._locks):
            state = self._state_for(session_key)
            attachment = state.live_runtime_attachments.get(token)
            if attachment is not None:
                await self.detach(
                    session_key,
                    attachment.routing_key,
                    expected=attachment,
                )
