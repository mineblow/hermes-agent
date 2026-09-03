"""Cross-process canonical live-runtime ownership registry.

The registry contains routing and liveness metadata only. It deliberately does
not contain authentication credentials; local proxy peers authenticate through
OS-owned IPC endpoints.
"""

from __future__ import annotations

import json
import math
import os
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from hermes_constants import get_hermes_home
from hermes_cli.active_sessions import (
    _FileLock,
    _pid_liveness,
    _process_start_time,
)

MAX_RUNTIME_OWNERS = 4096


class RuntimeOwnerRegistryError(RuntimeError):
    """The registry could not prove a safe canonical-owner decision."""


@dataclass(frozen=True)
class RuntimeOwner:
    conversation_key: str
    owner_id: str
    generation: int
    pid: int
    process_start_time: float | None
    endpoint: str
    profile_home: str
    surface: str
    started_at: float


@dataclass
class RuntimeOwnerLease:
    owner: RuntimeOwner
    state_path: Path
    lock_path: Path
    released: bool = False

    def release(self) -> bool:
        return release_runtime_owner(self)


@dataclass(frozen=True)
class OwnerClaimResult:
    kind: Literal["owned", "remote"]
    owner: RuntimeOwner
    lease: RuntimeOwnerLease | None


def _home(registry_home: str | Path | None) -> Path:
    return Path(registry_home) if registry_home is not None else Path(get_hermes_home())


def _paths(registry_home: str | Path | None) -> tuple[Path, Path]:
    runtime = _home(registry_home) / "runtime"
    return runtime / "live_runtime_owners.json", runtime / "live_runtime_owners.lock"


def _parse_owner(raw: Any, path: Path) -> RuntimeOwner:
    if not isinstance(raw, dict):
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry contains invalid entries: {path}"
        )
    required_strings = (
        "conversation_key",
        "owner_id",
        "endpoint",
        "profile_home",
        "surface",
    )
    if any(
        not isinstance(raw.get(key), str) or not raw[key].strip()
        for key in required_strings
    ):
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry contains invalid owner metadata: {path}"
        )
    pid = raw.get("pid")
    generation = raw.get("generation")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation <= 0
    ):
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry contains invalid identity: {path}"
        )
    process_start_time = raw.get("process_start_time")
    if process_start_time is not None:
        if isinstance(process_start_time, bool) or not isinstance(
            process_start_time, (int, float)
        ):
            raise RuntimeOwnerRegistryError(
                f"runtime owner registry contains invalid process start: {path}"
            )
        process_start_time = float(process_start_time)
        if not math.isfinite(process_start_time):
            raise RuntimeOwnerRegistryError(
                f"runtime owner registry contains invalid process start: {path}"
            )
    started_at = raw.get("started_at")
    if isinstance(started_at, bool) or not isinstance(started_at, (int, float)):
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry contains invalid start time: {path}"
        )
    started_at = float(started_at)
    if not math.isfinite(started_at):
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry contains invalid start time: {path}"
        )
    return RuntimeOwner(
        conversation_key=raw["conversation_key"],
        owner_id=raw["owner_id"],
        generation=generation,
        pid=pid,
        process_start_time=process_start_time,
        endpoint=raw["endpoint"],
        profile_home=raw["profile_home"],
        surface=raw["surface"],
        started_at=started_at,
    )


def _read(path: Path) -> tuple[list[RuntimeOwner], int]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return [], 1
    except Exception as exc:
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry unreadable: {path}"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("version") != 1
        or not isinstance(payload.get("entries"), list)
    ):
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry has invalid shape: {path}"
        )
    parsed = [_parse_owner(item, path) for item in payload["entries"]]
    keys = [owner.conversation_key for owner in parsed]
    if len(set(keys)) != len(keys):
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry has duplicate conversation keys: {path}"
        )
    minimum_next = max((owner.generation for owner in parsed), default=0) + 1
    next_generation = payload.get("next_generation", minimum_next)
    if (
        isinstance(next_generation, bool)
        or not isinstance(next_generation, int)
        or next_generation < minimum_next
    ):
        raise RuntimeOwnerRegistryError(
            f"runtime owner registry has invalid next generation: {path}"
        )
    return parsed, next_generation


def _write(
    path: Path, entries: list[RuntimeOwner], next_generation: int
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "version": 1,
                    "entries": [asdict(owner) for owner in entries],
                    "next_generation": next_generation,
                },
                handle,
                sort_keys=True,
            )
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _owner_liveness(owner: RuntimeOwner) -> bool | None:
    return _pid_liveness(owner.pid, owner.process_start_time)


def _sweep_proven_dead(
    entries: list[RuntimeOwner], *, preserve_key: str | None = None
) -> tuple[list[RuntimeOwner], bool]:
    retained: list[RuntimeOwner] = []
    changed = False
    for owner in entries:
        if owner.conversation_key == preserve_key:
            retained.append(owner)
            continue
        if _owner_liveness(owner) is False:
            changed = True
            continue
        retained.append(owner)
    return retained, changed


def _lease(owner: RuntimeOwner, state_path: Path, lock_path: Path) -> RuntimeOwnerLease:
    return RuntimeOwnerLease(owner=owner, state_path=state_path, lock_path=lock_path)


def claim_runtime_owner(
    *,
    conversation_key: str,
    owner_id: str,
    endpoint: str,
    surface: str,
    registry_home: str | Path | None = None,
    profile_home: str | Path | None = None,
) -> OwnerClaimResult:
    """Atomically own an absent/dead key or return its proven-live owner."""
    values = (conversation_key, owner_id, endpoint, surface)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError("runtime owner claim fields must be non-empty strings")
    state_path, lock_path = _paths(registry_home)
    profile = str(
        Path(profile_home) if profile_home is not None else _home(registry_home)
    )
    with _FileLock(lock_path):
        entries, next_generation = _read(state_path)
        existing = next(
            (entry for entry in entries if entry.conversation_key == conversation_key),
            None,
        )
        entries, swept = _sweep_proven_dead(
            entries,
            preserve_key=existing.conversation_key if existing is not None else None,
        )
        generation = next_generation
        if existing is not None:
            liveness = _owner_liveness(existing)
            if liveness is None:
                raise RuntimeOwnerRegistryError("runtime owner liveness is unknown")
            if liveness:
                if swept:
                    _write(state_path, entries, next_generation)
                if (
                    existing.owner_id == owner_id
                    and existing.pid == os.getpid()
                    and existing.endpoint == endpoint
                ):
                    return OwnerClaimResult(
                        "owned", existing, _lease(existing, state_path, lock_path)
                    )
                return OwnerClaimResult("remote", existing, None)
            generation = max(generation, existing.generation + 1)
            entries.remove(existing)
        if len(entries) >= MAX_RUNTIME_OWNERS:
            if swept:
                _write(state_path, entries, next_generation)
            raise RuntimeOwnerRegistryError("runtime owner registry capacity exceeded")
        owner = RuntimeOwner(
            conversation_key=conversation_key,
            owner_id=owner_id,
            generation=generation,
            pid=os.getpid(),
            process_start_time=_process_start_time(os.getpid()),
            endpoint=endpoint,
            profile_home=profile,
            surface=surface,
            started_at=time.time(),
        )
        entries.append(owner)
        _write(state_path, entries, generation + 1)
        return OwnerClaimResult("owned", owner, _lease(owner, state_path, lock_path))


def lookup_runtime_owner(
    *, conversation_key: str, registry_home: str | Path | None = None
) -> RuntimeOwner | None:
    state_path, lock_path = _paths(registry_home)
    with _FileLock(lock_path):
        entries, next_generation = _read(state_path)
        entries, swept = _sweep_proven_dead(entries)
        if swept:
            _write(state_path, entries, next_generation)
        return next(
            (owner for owner in entries if owner.conversation_key == conversation_key),
            None,
        )


def assert_runtime_owner(lease: RuntimeOwnerLease) -> bool:
    with _FileLock(lease.lock_path):
        entries, _next_generation = _read(lease.state_path)
        current = next(
            (
                owner
                for owner in entries
                if owner.conversation_key == lease.owner.conversation_key
            ),
            None,
        )
        return (
            current is not None
            and current == lease.owner
            and _owner_liveness(current) is True
        )


def release_runtime_owner(lease: RuntimeOwnerLease) -> bool:
    if lease.released:
        return False
    with _FileLock(lease.lock_path):
        entries, next_generation = _read(lease.state_path)
        current = next(
            (
                owner
                for owner in entries
                if owner.conversation_key == lease.owner.conversation_key
            ),
            None,
        )
        if current is None or current != lease.owner:
            return False
        entries.remove(current)
        _write(lease.state_path, entries, next_generation)
        lease.released = True
        return True


def release_process_runtime_owners(
    owner_id: str, *, registry_home: str | Path | None = None
) -> int:
    state_path, lock_path = _paths(registry_home)
    with _FileLock(lock_path):
        entries, next_generation = _read(state_path)
        retained = [owner for owner in entries if owner.owner_id != owner_id]
        removed = len(entries) - len(retained)
        if removed:
            _write(state_path, retained, next_generation)
        return removed
