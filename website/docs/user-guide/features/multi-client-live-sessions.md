---
sidebar_position: 6
title: "Multi-Client Live Sessions"
description: "Use supported CLI, TUI, Desktop, dashboard chat, and messaging surfaces on one canonical Hermes runtime"
---

# Multi-Client Live Sessions

Hermes can keep one conversation actively connected to multiple presentation clients. A prompt submitted from one authorized client is executed once by one canonical runtime; the other attached clients observe the same ordered user, assistant, tool, approval, clarification, error, usage, and completion events in the form their platform can render.

Typical examples:

- keep a Desktop or TUI session open while replying from Discord;
- resume the same live conversation from the classic CLI in another terminal;
- watch current tool activity in the dashboard while a messaging client submits the next prompt;
- reconnect a replay-capable surface after a transport drop without duplicating accepted events.

## Supported surfaces

Persistent presentation surfaces supported by the shared runtime include:

- classic interactive CLI (`hermes --cli` or a CLI-configured bare `hermes`);
- default TUI (`hermes --tui`);
- Desktop's native chat surface;
- the web dashboard's embedded TUI. Its PTY-side structured-events sidebar is a best-effort view, not a durable replay client;
- authorized gateway messaging adapters, including Discord, Telegram, Slack, API Server/OpenWebUI, Email, and other adapters that use the common `GatewayRunner` contract.

Not every Hermes protocol is a presentation client. One-shot CLI, A2A, cron, and webhooks are producers; MCP is a tool/control protocol; ACP currently owns separate ACP sessions rather than attaching to a canonical gateway runtime. See the authoritative [live-session surface classification](/reference/live-session-surface-classification).

## One runtime, many presentations

```text
              canonical runtime owner
          (scheduler + agent + event log)
                       │
             authenticated runtime proxy
          ┌────────────┼──────────────┐
          │            │              │
      TUI/Desktop   classic CLI   GatewayRunner
          │                           │
      dashboard                  Discord/Slack/…
```

The runtime owner is the only executor and the only writer of canonical runtime output. Attached clients are presentation adapters: they observe, submit input, optionally answer interactions, and render events. They do not start another agent or append observed output to session storage again.

A durable session ID, a compression-lineage root, and a current live runtime ID are related but distinct identities. Hermes resolves those internally. Clients should resume by the user-facing session ID or title and must not construct owner keys themselves.

## Attachment modes

### Create

Starting an interactive CLI, TUI, Desktop chat, or dashboard chat can create a durable session and its canonical runtime. Messaging input creates or uses the gateway session associated with that authorized routing key.

Name a session early to make cross-surface resume unambiguous:

```text
/title live-release-review
```

### Resume and attach

Resume by title or durable session ID:

```bash
# Classic interactive CLI
hermes --cli --resume "live-release-review"

# Default TUI
hermes --tui --resume "live-release-review"
```

From a messaging conversation or the dashboard/TUI composer:

```text
/resume live-release-review
```

If the canonical runtime is already active, Hermes attaches to it instead of creating a second agent. If the transcript is saved but no runtime is active, the interactive surface may become the new canonical owner through the normal create/resume path.

### Reconnect

A temporarily disconnected replay-capable client reconnects to the same owner generation and requests events after its last successfully accepted sequence. Hermes closes superseded connections and ignores stale frames from older owner generations. This guarantee belongs to the client/runtime attachment protocol; auxiliary feeds such as the dashboard's `/api/events` sidebar reconnect without a cursor and can miss events during a drop.

Reconnect is not the same as loading the durable transcript. On a fresh attachment, the frontend loads or displays saved history and starts live event delivery at the owner's current sequence. On transport reconnect, the runtime replays only the missing retained events.

## Identity, ordering, and idempotency

Every canonical input carries a stable `client_message_id`:

- messaging adapters derive it from the platform's native delivery/update identity;
- interactive clients generate one for each submission;
- redelivery of the same native message maps to the same ID and executes once;
- identical text in two distinct native messages has two IDs and executes twice.

Runtime output carries a replay epoch and monotonic event sequence. In the direct TUI client, an event is accepted when all synchronous `event` listeners return successfully; only then does the client advance its local replay watermark. This is callback completion inside the client, not a per-event acknowledgement sent back to the server. Runtime-proxy adapters maintain their own delivered-sequence contract. Best-effort feeds with no cursor, including the dashboard sidebar's `/api/events` socket, provide neither guarantee.

When several clients submit while a turn is busy, the canonical scheduler—not the frontend—applies the submitted busy policy:

- `interrupt`: replace/redirect the active turn;
- `queue`: run after the current turn;
- `reject`: decline the busy submission;

Steering is a separate control operation: where supported, it injects guidance into the active run at a safe boundary. It is not a fourth busy policy. The classic CLI routes steering and interrupt/replacement through the canonical runtime and never calls a separate local agent while attached.

## Approvals and clarifications

Approval and clarification requests have canonical request IDs. Only attachments granted the corresponding `approval.respond` or `clarify.respond` capability receive the sensitive request payload and may answer it; observe-only attachments do not receive those events.

The first valid response wins. Later responses to the same request are rejected or treated as already resolved; they do not run the operation twice. Interaction-response calls use a short, strict deadline. If the transport fails after submission and the outcome is unknown, the client does not retry automatically.

Presentation differs by surface:

- TUI, Desktop, and dashboard use modal/overlay controls;
- messaging adapters use their native buttons, commands, or reply affordances;
- classic CLI uses its existing approval and clarification prompts.

All of them send the original canonical request ID back to the owner.

## Authorization and capabilities

Live attachment does not bypass a platform's existing authorization:

- messaging users must pass the configured allowlist, pairing, tenant/scope, and thread/channel checks;
- profile resolution scopes owner lookup so one profile cannot attach to another profile's runtime;
- the runtime handshake verifies the principal descriptor and intersects requested capabilities with those permitted by the connection;
- observer-only clients receive events but cannot submit prompts or resolve interactions;
- controller capabilities are explicit (`prompt.submit`, `interaction.respond`) rather than implied by visibility.

An unauthorized user cannot attach, observe private runtime events, steer a turn, or answer an interaction merely by knowing a session title or ID.

## Replay and recovery

Replay-capable attachments use the fences implemented by their adapter:

1. **Owner generation** — rejects stale connections after owner replacement.
2. **Replay epoch and sequence** — resumes after the adapter's last accepted event without duplicates.
3. **Authoritative presentation snapshot (runtime-proxy path)** — when the retained event window is too short, runtime-proxy adapters validate the owner's latest assistant completion boundary before advancing. The direct TUI currently fails closed on a truncated `session.attach` replay rather than silently advancing across the gap.

Pending approvals or clarifications are restored by the runtime-proxy handshake for adapters that consume its `pending_interactions` state. Direct `session.attach` clients only recover an interaction when its request event remains in the retained window; the dashboard's best-effort sidebar does not recover pending interactions.

Slow consumers are bounded. A client that cannot keep up may be detached without blocking healthy peers or the runtime owner. It can later reattach using the recovery its adapter implements: retained replay, runtime-proxy snapshot reconciliation, or fail-closed recovery when no safe resynchronization path exists.

## Failure and fallback behavior

Hermes fails in the direction that prevents duplicate execution:

- if no canonical owner exists, a gateway request may use the legacy process-local path only **before** canonical submission;
- if authenticated runtime IPC is unavailable or unsupported, local fallback is allowed only when no canonical request may already have executed;
- after a request is sent and its outcome becomes unknown, Hermes reports the unknown outcome and does not retry it locally;
- owner-attachment failure is surfaced; an attached frontend must not silently create a second agent for the same live conversation;
- an attached presentation client never re-persists canonical output during shutdown.

Local unsent drafts remain local to each frontend. Live sessions synchronize submitted canonical events, not composer buffers, cursor position, theme, scroll position, or platform-specific UI state.

## Platform rendering differences

All clients observe the same canonical meaning, but not identical pixels:

| Canonical event | Rich terminal/Desktop/dashboard | Messaging platforms |
|---|---|---|
| Assistant streaming | Token/chunk updates | Edited message when reliable, otherwise bounded follow-up chunks |
| Thinking/commentary | Dedicated styled regions | Collapsed, summarized, or omitted according to adapter capabilities |
| Tool activity | Structured tool cards/rows | Native text/status rendering, possibly condensed |
| Peer user message | Immediate conversation row | Native message or labelled peer-delivery event where supported |
| Approval/clarification | Modal or prompt | Buttons, commands, or replies |
| Attachments | Local/native references and previews | Platform media references preserved through canonical input |
| Usage/completion | Status widgets and terminal rows | Usually compact or omitted when the platform has no suitable surface |

Unsupported presentation detail may be safely collapsed. It must never be interpreted as permission to execute or persist the event a second time.

## Manual cross-surface test

Use a non-production test profile and an authorized messaging account.

### 1. Start the gateway

```bash
hermes gateway start
```

Confirm the target messaging platform and dashboard are configured before continuing.

### 2. Create the canonical session in TUI

Terminal A:

```bash
hermes --tui
```

Then submit:

```text
/title live-session-test
Remember the marker ALPHA-741 and reply with READY.
```

Wait for `READY`.

### 3. Attach classic CLI

Terminal B:

```bash
hermes --cli --resume "live-session-test"
```

Submit:

```text
What marker did Terminal A provide?
```

Verify Terminal B answers `ALPHA-741` and Terminal A observes Terminal B's submitted user message before or with the canonical assistant response.

### 4. Attach a messaging client

In an authorized Discord, Telegram, or Slack conversation:

```text
/resume live-session-test
```

Then send:

```text
From messaging: append BRAVO-852 to the remembered markers.
```

Verify both terminal clients observe the messaging user event and resulting assistant completion. Redelivering the exact same native platform event must not produce a second model turn.

### 5. Attach dashboard/web

```bash
hermes dashboard
```

Open **Chat**, run `/resume live-session-test`, and verify the ALPHA and BRAVO turns are present. Submit a third distinct marker and verify it appears on the two terminal clients and messaging surface.

### 6. Exercise simultaneous input

Start a deliberately slow prompt from one controller. Before it completes, submit a correction from another controller. Verify the configured canonical busy policy is applied once and all clients converge on the same final ordering.

### 7. Exercise one interaction race

Trigger a harmless approval or clarification. Answer it from one controller while another still displays it. Verify exactly one response wins and the second surface reports or refreshes the resolved state; the operation must not run twice.

### 8. Exercise reconnect

Disconnect Terminal B, submit one turn elsewhere, then run the same resume command again. Verify the missing turn appears once, with no duplicate peer or assistant rows.

### 9. Verify clean shutdown

Exit one attached client and continue from another. The canonical runtime must remain usable while another owner/client is active, and durable history must contain one copy of each canonical user and assistant completion.

## Developer contract

New persistent frontends should use the transport-neutral primitives in `hermes_cli/live_runtime_protocol.py`, the reconnecting client in `gateway/live_runtime_client.py`, and the canonical owner/runtime proxy. Platform adapters should inherit the `GatewayRunner` bridge rather than introducing platform-specific agent execution.

Required invariants:

- one canonical owner per profile-scoped conversation root;
- stable input identity and canonical scheduler ordering;
- event callbacks ordered by epoch/sequence;
- bounded queues and slow-client isolation;
- capability checks before scheduler or interaction mutation;
- no retry after unknown execution outcome;
- no canonical persistence from presentation adapters;
- no network writes while runtime ownership locks are held.

See also:

- [Sessions](/user-guide/sessions)
- [CLI interface](/user-guide/cli)
- [TUI](/user-guide/tui)
- [Desktop](/user-guide/desktop)
- [Messaging gateway](/user-guide/messaging)
- [Gateway internals](/developer-guide/gateway-internals)
- [Live-session surface classification](/reference/live-session-surface-classification)
