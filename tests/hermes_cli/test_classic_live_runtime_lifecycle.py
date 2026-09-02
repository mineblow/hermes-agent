from __future__ import annotations

import threading
import uuid
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from hermes_cli.classic_live_runtime import (
    ClassicLiveRuntimeController,
    ClassicLiveRuntimeFrontend,
    ClassicLiveRuntimeSession,
    InProcessGatewayClient,
    build_classic_live_runtime_frontend,
)
from hermes_cli.live_runtime_owners import RuntimeOwner
from hermes_cli.live_runtime_protocol import runtime_event


def _event(seq: int, event_type: str, payload: dict):
    return runtime_event(
        runtime_id="runtime-1",
        durable_session_id="session-1",
        replay_epoch="epoch-1",
        seq=seq,
        event_type=event_type,
        payload=payload,
    )


@pytest.mark.asyncio
async def test_classic_frontend_submits_with_stable_identity_and_closes_cleanly():
    client = AsyncMock()
    client.request.return_value = {"status": "streaming"}
    frontend = ClassicLiveRuntimeFrontend(client=client, client_id="classic-cli:test")

    await frontend.start()
    result = await frontend.submit("hello", submitted_at=12.5, message_id="local-1")
    await frontend.close()

    client.start.assert_awaited_once_with()
    client.register_local_message_id.assert_called_once_with("local-1")
    client.request.assert_awaited_once_with(
        {
            "kind": "runtime.input",
            "protocol": 1,
            "busy_policy": "interrupt",
            "display_text": "hello",
            "message_id": "local-1",
            "submitted_at": 12.5,
            "text": "hello",
        }
    )
    assert result == {"status": "streaming"}
    client.close.assert_awaited_once_with()
    assert frontend.client_id == "classic-cli:test"
    assert frontend.surface == "classic-cli"


def test_classic_frontend_renders_peer_rows_but_deduplicates_local_echoes():
    rows = []
    frontend = ClassicLiveRuntimeFrontend(
        client=AsyncMock(),
        client_id="classic-cli:test",
        on_row=rows.append,
    )
    frontend.remember_local_message_id("local-1")

    frontend.on_event(
        _event(
            1,
            "message.user",
            {
                "message_id": "local-1",
                "text": "local echo",
                "timestamp": 1.0,
                "display_metadata": {"user_name": "Local"},
            },
        )
    )
    frontend.on_event(
        _event(
            2,
            "message.user",
            {
                "message_id": "peer-1",
                "text": "peer prompt",
                "timestamp": 2.0,
                "display_metadata": {"user_name": "Alice", "surface": "discord"},
            },
        )
    )
    frontend.on_event(
        _event(3, "message.complete", {"text": "assistant reply", "status": "complete"})
    )

    assert rows == [
        {
            "kind": "peer-user",
            "message_id": "peer-1",
            "surface": "discord",
            "text": "peer prompt",
            "timestamp": 2.0,
            "user_name": "Alice",
        },
        {"kind": "assistant", "status": "complete", "text": "assistant reply"},
    ]


@pytest.mark.asyncio
async def test_classic_frontend_restores_and_answers_interactions():
    rows = []
    client = AsyncMock()
    client.respond_to_interaction.return_value = {"accepted": True}
    frontend = ClassicLiveRuntimeFrontend(
        client=client,
        client_id="classic-cli:test",
        on_row=rows.append,
    )
    pending = {
        "interaction_type": "approval",
        "request_id": "approval-1",
        "payload": {"command": "deploy", "choices": ["once", "deny"]},
    }

    frontend.on_pending_interaction(pending)
    result = await frontend.respond_to_interaction(
        interaction_type="approval",
        request_id="approval-1",
        choice="once",
    )

    assert rows == [{"kind": "interaction", **pending}]
    client.respond_to_interaction.assert_awaited_once_with(
        {
            "interaction_type": "approval",
            "request_id": "approval-1",
            "choice": "once",
        }
    )
    assert result == {"accepted": True}


def test_builder_uses_compression_root_for_owner_lookup(tmp_path):
    captured = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    build_classic_live_runtime_frontend(
        durable_session_id="session-1",
        conversation_key="compression-root-1",
        profile="worker",
        profile_home=tmp_path,
        client_factory=FakeClient,
        connection_factory=lambda **kwargs: kwargs,
        owner_lookup=lambda *_args, **_kwargs: None,
    )

    from tui_gateway.runtime_proxy import _profile_scoped_conversation_key

    assert captured["conversation_key"] == _profile_scoped_conversation_key(
        "compression-root-1", tmp_path
    )


def test_builder_wires_classic_identity_owner_lookup_and_runtime_proxy(tmp_path):
    captured = {}
    rows = []

    class FakeClient:
        def __init__(self, **kwargs):
            captured["client"] = kwargs

    class FakeConnection:
        def __init__(self, **kwargs):
            captured["connection"] = kwargs

    owner = RuntimeOwner(
        conversation_key="scoped-session",
        owner_id="owner-1",
        generation=1,
        pid=123,
        process_start_time=1.0,
        endpoint=str(tmp_path / "runtime.sock"),
        profile_home=str(tmp_path),
        surface="tui_gateway",
        started_at=1.0,
    )
    looked_up = []

    frontend = build_classic_live_runtime_frontend(
        durable_session_id="session-1",
        conversation_key="compression-root-1",
        profile="worker",
        profile_home=tmp_path,
        client_id="classic-cli:test",
        on_row=rows.append,
        client_factory=FakeClient,
        connection_factory=FakeConnection,
        owner_lookup=lambda key, registry_home=None: looked_up.append(
            (key, registry_home)
        ) or owner,
    )

    client_args = captured["client"]
    assert frontend.client_id == "classic-cli:test"
    assert client_args["surface"] == "classic-cli"
    assert client_args["client_id"] == "classic-cli:test"
    assert client_args["principal"] == {
        "authenticated": True,
        "provider": "classic-cli",
        "subject": "classic-cli:test",
    }
    assert client_args["requested_capabilities"] == {
        "interaction.respond",
        "observe",
        "prompt.submit",
    }
    assert client_args["owner_lookup"](client_args["conversation_key"]) == owner
    assert looked_up == [(client_args["conversation_key"], tmp_path)]

    connection = client_args["connector"](owner)
    assert isinstance(connection, FakeConnection)
    assert captured["connection"] == {
        "owner": owner,
        "durable_session_id": "session-1",
        "profile": "worker",
        "client_id": "classic-cli:test",
        "requested_capabilities": frozenset(
            {"interaction.respond", "observe", "prompt.submit"}
        ),
    }


def test_sync_controller_runs_async_frontend_and_closes_its_thread():
    calls = []

    class FakeFrontend:
        async def start(self):
            calls.append("start")

        async def submit(self, text, **kwargs):
            calls.append(("submit", text, kwargs))
            return {"status": "streaming"}

        async def respond_to_interaction(self, **kwargs):
            calls.append(("respond", kwargs))
            return {"accepted": True}

        async def close(self):
            calls.append("close")

    controller = ClassicLiveRuntimeController(FakeFrontend())
    controller.start()
    assert controller.submit(
        "hello", submitted_at=1.0, message_id="message-1"
    ) == {"status": "streaming"}
    assert controller.respond_to_interaction(
        interaction_type="clarification",
        request_id="clarify-1",
        answer="production",
    ) == {"accepted": True}
    controller.close()

    assert calls == [
        "start",
        (
            "submit",
            "hello",
            {"submitted_at": 1.0, "message_id": "message-1"},
        ),
        (
            "respond",
            {
                "interaction_type": "clarification",
                "request_id": "clarify-1",
                "answer": "production",
            },
        ),
        "close",
    ]
    assert controller.is_alive is False


def test_sync_controller_uses_separate_interaction_timeout():
    class FakeFrontend:
        async def respond_to_interaction(self, **kwargs):
            return kwargs

    controller = ClassicLiveRuntimeController(
        FakeFrontend(), request_timeout=1800, interaction_timeout=30
    )
    observed = []

    def fake_call(awaitable, *, timeout):
        awaitable.close()
        observed.append(timeout)
        return {"accepted": True}

    controller._call = fake_call

    assert controller.respond_to_interaction(
        interaction_type="approval", request_id="approval-1", choice="once"
    ) == {"accepted": True}
    assert observed == [30]


def test_session_creates_gateway_owner_before_starting_classic_frontend(tmp_path):
    calls = []

    class FakeGateway:
        def request(self, method, params):
            calls.append(("rpc", method, params))
            return {
                "session_id": "live-1",
                "stored_session_id": "durable-1",
            }

        def close(self):
            calls.append("gateway.close")

    class FakeController:
        def start(self):
            calls.append("controller.start")

        def submit(self, text, **kwargs):
            calls.append(("submit", text, kwargs))
            return {"status": "streaming"}

        def close(self):
            calls.append("controller.close")

    def build_frontend(**kwargs):
        calls.append(("build", kwargs))
        return object()

    session = ClassicLiveRuntimeSession(
        gateway=FakeGateway(),
        profile="worker",
        profile_home=tmp_path,
        frontend_factory=build_frontend,
        controller_factory=lambda _frontend: FakeController(),
    )

    identity = session.start_new(cols=100, cwd="/workspace")
    result = session.submit("hello", submitted_at=1.0, message_id="message-1")
    session.close()

    assert identity == {
        "conversation_key": "durable-1",
        "durable_session_id": "durable-1",
        "live_session_id": "live-1",
    }
    assert calls[0] == (
        "rpc",
        "session.create",
        {
            "attachment_mode": "control",
            "cols": 100,
            "cwd": "/workspace",
            "profile": "worker",
            "source": "classic-cli",
        },
    )
    assert calls[1][0] == "build"
    assert calls[1][1]["conversation_key"] == "durable-1"
    assert calls[1][1]["durable_session_id"] == "durable-1"
    assert calls[2] == "controller.start"
    assert result == {"status": "streaming"}
    assert calls[-2:] == ["controller.close", "gateway.close"]


def test_session_resume_attaches_to_compression_root(tmp_path):
    calls = []

    class FakeGateway:
        def request(self, method, params):
            calls.append(("rpc", method, params))
            return {
                "session_id": "live-tip",
                "stored_session_id": "durable-tip",
            }

        def close(self):
            pass

    class FakeController:
        def start(self):
            calls.append("controller.start")

        def close(self):
            pass

    def build_frontend(**kwargs):
        calls.append(("build", kwargs))
        return object()

    session = ClassicLiveRuntimeSession(
        gateway=FakeGateway(),
        profile=None,
        profile_home=tmp_path,
        conversation_key_resolver=lambda durable_id: (
            calls.append(("resolve", durable_id)) or "compression-root"
        ),
        frontend_factory=build_frontend,
        controller_factory=lambda _frontend: FakeController(),
    )

    identity = session.start_resume("requested-parent", cols=80)

    assert calls[0] == (
        "rpc",
        "session.resume",
        {
            "attachment_mode": "control",
            "cols": 80,
            "omit_messages": True,
            "profile": None,
            "session_id": "requested-parent",
            "source": "classic-cli",
        },
    )
    assert calls[1] == ("resolve", "durable-tip")
    assert calls[2][1]["conversation_key"] == "compression-root"
    assert identity == {
        "conversation_key": "compression-root",
        "durable_session_id": "durable-tip",
        "live_session_id": "live-tip",
    }


def test_in_process_gateway_correlates_async_response_and_detaches():
    detached = []

    def dispatch(request, transport):
        transport.write(
            {
                "jsonrpc": "2.0",
                "method": "event",
                "params": {"type": "session.info", "payload": {}},
            }
        )
        threading.Timer(
            0.01,
            lambda: transport.write(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"session_id": "live-1"},
                }
            ),
        ).start()
        return None

    client = InProcessGatewayClient(
        dispatch=dispatch,
        detach=lambda transport: detached.append(transport.connection_id),
        timeout=1,
    )

    assert client.request("session.resume", {"session_id": "stored-1"}) == {
        "session_id": "live-1"
    }
    connection_id = client.transport.connection_id
    client.close()

    assert detached == [connection_id]
    assert client.transport.closed is True


def test_session_submit_and_wait_blocks_until_terminal_assistant_row(tmp_path):
    captured = {}

    class FakeGateway:
        def request(self, _method, _params):
            return {"session_id": "live-1", "stored_session_id": "durable-1"}

        def close(self):
            pass

    class FakeController:
        def start(self):
            pass

        def submit(self, text, **kwargs):
            captured["on_row"](
                {"kind": "assistant", "status": "complete", "text": "answer"}
            )
            return {"accepted": True}

        def close(self):
            pass

    def build_frontend(**kwargs):
        captured.update(kwargs)
        return object()

    session = ClassicLiveRuntimeSession(
        gateway=FakeGateway(),
        profile=None,
        profile_home=tmp_path,
        frontend_factory=build_frontend,
        controller_factory=lambda _frontend: FakeController(),
        turn_timeout=1,
    )
    session.start_new(cols=80, cwd="/workspace")

    terminal = session.submit_and_wait(
        "hello", submitted_at=1.0, message_id="message-1"
    )

    assert terminal == {
        "kind": "assistant",
        "status": "complete",
        "text": "answer",
    }


@pytest.mark.parametrize(
    ("busy_input_mode", "expected_policy"),
    [("queue", "queue"), ("steer", "interrupt")],
)
def test_cli_live_turn_routes_through_facade_without_direct_agent(
    monkeypatch, capsys, busy_input_mode, expected_policy
):
    from cli import HermesCLI

    captured = {}

    class FakeRuntime:
        def submit_and_wait(self, text, **kwargs):
            captured["text"] = text
            captured.update(kwargs)
            return {"kind": "assistant", "status": "complete", "text": "answer"}

    cli = object.__new__(HermesCLI)
    cli._classic_live_runtime = FakeRuntime()
    cli.busy_input_mode = busy_input_mode
    cli._last_turn_interrupted = False
    cli.conversation_history = []
    cli.chat = lambda *_args, **_kwargs: pytest.fail("direct chat must not run")
    monkeypatch.setattr(uuid, "uuid4", lambda: type("Id", (), {"hex": "message-1"})())

    image_path = Path("/tmp/image.png")
    terminal = cli._chat_via_classic_live_runtime(
        "hello", images=[image_path]
    )

    assert terminal["text"] == "answer"
    assert captured["text"] == "hello"
    assert captured["message_id"] == "message-1"
    assert captured["busy_policy"] == expected_policy
    assert captured["attachment_refs"] == [str(image_path)]
    assert captured["image_refs"] == [str(image_path)]
    assert cli.conversation_history == [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "answer"},
    ]
    assert capsys.readouterr().out.strip() == "answer"


def test_cli_relays_runtime_approval_and_clarification_through_existing_modals():
    from cli import HermesCLI

    responses = []

    class FakeRuntime:
        def respond_to_interaction(self, **kwargs):
            responses.append(kwargs)

    cli = object.__new__(HermesCLI)
    cli._classic_live_runtime = FakeRuntime()
    cli._approval_callback = lambda command, description, **kwargs: "session"
    cli._clarify_callback = lambda question, choices, **kwargs: "production"

    cli._resolve_classic_live_interaction(
        {
            "kind": "interaction",
            "interaction_type": "approval",
            "request_id": "approval-1",
            "payload": {
                "command": "deploy",
                "description": "Deploy release?",
                "choices": ["once", "session", "deny"],
            },
        }
    )
    cli._resolve_classic_live_interaction(
        {
            "kind": "interaction",
            "interaction_type": "clarification",
            "request_id": "clarify-1",
            "payload": {
                "question": "Which environment?",
                "choices": ["staging", "production"],
                "question_id": "environment",
            },
        }
    )

    assert responses == [
        {
            "interaction_type": "approval",
            "request_id": "approval-1",
            "choice": "session",
        },
        {
            "interaction_type": "clarification",
            "request_id": "clarify-1",
            "answer": "production",
            "question_id": "environment",
        },
    ]


@pytest.mark.parametrize("resumed", [False, True])
def test_cli_starts_authoritative_gateway_runtime(monkeypatch, tmp_path, resumed):
    from cli import HermesCLI
    from hermes_cli import classic_live_runtime as live_module

    calls = []
    gateway = object()

    class FakeRuntime:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def start_new(self, **kwargs):
            calls.append(("new", kwargs))
            return {"durable_session_id": "durable-new"}

        def start_resume(self, session_id, **kwargs):
            calls.append(("resume", session_id, kwargs))
            return {"durable_session_id": "durable-resumed"}

        def close(self):
            calls.append("close")

    monkeypatch.setattr(live_module, "ClassicLiveRuntimeSession", FakeRuntime)
    monkeypatch.setattr(
        live_module, "build_in_process_gateway_client", lambda: gateway
    )
    monkeypatch.chdir(tmp_path)

    cli = object.__new__(HermesCLI)
    cli._session_db = type(
        "DB", (), {"session_runtime_key": lambda self, value: value}
    )()
    cli._resumed = resumed
    cli.session_id = "requested-session"
    cli._classic_live_runtime = None

    cli._start_classic_live_runtime()

    assert calls[0][0] == "init"
    assert calls[0][1]["gateway"] is gateway
    assert calls[0][1]["conversation_key_resolver"]("root") == "root"
    if resumed:
        assert calls[1][0:2] == ("resume", "requested-session")
        assert cli.session_id == "durable-resumed"
    else:
        assert calls[1][0] == "new"
        assert calls[1][1]["cwd"] == str(tmp_path)
        assert cli.session_id == "durable-new"
    assert cli._classic_live_runtime is not None
