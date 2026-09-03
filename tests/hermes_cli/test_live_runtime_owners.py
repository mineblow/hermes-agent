from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path

import pytest

from hermes_cli import live_runtime_owners as owners


def _claim_in_subprocess(home: str, owner_id: str, results, release) -> None:
    from hermes_cli.live_runtime_owners import claim_runtime_owner

    result = claim_runtime_owner(
        conversation_key="raced-conversation-root",
        owner_id=owner_id,
        endpoint=f"/tmp/{owner_id}.sock",
        surface="test",
        registry_home=home,
    )
    results.put((result.kind, result.owner.owner_id, result.owner.generation))
    release.wait(timeout=10)


def _claim(home: Path, *, owner_id: str, endpoint: str = "/tmp/runtime.sock"):
    return owners.claim_runtime_owner(
        conversation_key="conversation-root",
        owner_id=owner_id,
        endpoint=endpoint,
        surface="test",
        registry_home=home,
    )


def test_same_conversation_key_has_exactly_one_cross_process_owner(tmp_path):
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    release = context.Event()
    processes = [
        context.Process(
            target=_claim_in_subprocess,
            args=(str(tmp_path), f"owner-{index}", results, release),
        )
        for index in range(6)
    ]
    try:
        for process in processes:
            process.start()
        claims = [results.get(timeout=15) for _ in processes]

        owned = [claim for claim in claims if claim[0] == "owned"]
        remote = [claim for claim in claims if claim[0] == "remote"]
        assert len(owned) == 1
        assert len(remote) == 5
        assert {claim[1:] for claim in claims} == {owned[0][1:]}
    finally:
        release.set()
        for process in processes:
            process.join(timeout=10)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        results.close()


def test_live_conversation_key_has_exactly_one_owner(tmp_path):
    first = _claim(tmp_path, owner_id="owner-a", endpoint="/tmp/a.sock")
    second = _claim(tmp_path, owner_id="owner-b", endpoint="/tmp/b.sock")

    assert first.kind == "owned"
    assert first.lease is not None
    assert first.owner.owner_id == "owner-a"
    assert first.owner.generation == 1
    assert second.kind == "remote"
    assert second.lease is None
    assert second.owner == first.owner


def test_dead_owner_is_reclaimed_with_incremented_generation(tmp_path, monkeypatch):
    first = _claim(tmp_path, owner_id="owner-a", endpoint="/tmp/a.sock")
    monkeypatch.setattr(owners, "_owner_liveness", lambda owner: False)

    second = _claim(tmp_path, owner_id="owner-b", endpoint="/tmp/b.sock")

    assert second.kind == "owned"
    assert second.lease is not None
    assert second.owner.owner_id == "owner-b"
    assert second.owner.generation == first.owner.generation + 1


def test_clean_release_preserves_monotonic_owner_generation(tmp_path):
    first = _claim(tmp_path, owner_id="owner-a", endpoint="/tmp/a.sock")
    assert first.lease is not None
    assert owners.release_runtime_owner(first.lease) is True

    second = _claim(tmp_path, owner_id="owner-b", endpoint="/tmp/b.sock")

    assert second.kind == "owned"
    assert second.owner.generation > first.owner.generation


def test_claim_sweeps_unrelated_proven_dead_owners(tmp_path, monkeypatch):
    first = _claim(tmp_path, owner_id="owner-a", endpoint="/tmp/a.sock")
    monkeypatch.setattr(
        owners,
        "_owner_liveness",
        lambda owner: False if owner.owner_id == "owner-a" else True,
    )

    second = owners.claim_runtime_owner(
        conversation_key="another-root",
        owner_id="owner-b",
        endpoint="/tmp/b.sock",
        surface="test",
        registry_home=tmp_path,
    )

    assert first.owner != second.owner
    assert (
        owners.lookup_runtime_owner(
            conversation_key="conversation-root", registry_home=tmp_path
        )
        is None
    )


def test_registry_fails_closed_at_live_owner_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(owners, "MAX_RUNTIME_OWNERS", 1)
    _claim(tmp_path, owner_id="owner-a", endpoint="/tmp/a.sock")

    with pytest.raises(owners.RuntimeOwnerRegistryError, match="capacity"):
        owners.claim_runtime_owner(
            conversation_key="another-root",
            owner_id="owner-b",
            endpoint="/tmp/b.sock",
            surface="test",
            registry_home=tmp_path,
        )


def test_corrupt_owner_registry_fails_closed_without_overwrite(tmp_path):
    state = tmp_path / "runtime" / "live_runtime_owners.json"
    state.parent.mkdir(parents=True)
    state.write_text("{not-json", encoding="utf-8")

    with pytest.raises(owners.RuntimeOwnerRegistryError, match="unreadable"):
        _claim(tmp_path, owner_id="owner-a")

    assert state.read_text(encoding="utf-8") == "{not-json"


def test_stale_release_cannot_delete_new_owner_generation(tmp_path, monkeypatch):
    first = _claim(tmp_path, owner_id="owner-a", endpoint="/tmp/a.sock")
    monkeypatch.setattr(owners, "_owner_liveness", lambda owner: False)
    second = _claim(tmp_path, owner_id="owner-b", endpoint="/tmp/b.sock")
    monkeypatch.setattr(owners, "_owner_liveness", lambda owner: owner == second.owner)

    assert first.lease is not None
    assert second.lease is not None
    assert owners.release_runtime_owner(first.lease) is False
    assert (
        owners.lookup_runtime_owner(
            conversation_key="conversation-root",
            registry_home=tmp_path,
        )
        == second.owner
    )

    payload = json.loads(
        (tmp_path / "runtime" / "live_runtime_owners.json").read_text(encoding="utf-8")
    )
    encoded = json.dumps(payload).lower()
    assert "secret" not in encoded
    assert "token" not in encoded
    assert payload["version"] == 1
    assert payload["entries"][0]["pid"] == os.getpid()
