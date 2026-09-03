# Multi-client live sessions

Hermes uses one canonical live-session runtime for every client attached to the
same session. Desktop, TUI, messaging gateways, and runtime-proxy clients do not
run independent copies of the agent turn.

## Ownership and attachments

A live session has one runtime owner. The owner holds the agent, model stream,
tool execution, persisted history, and ordered event log.

Clients attach as one of two roles:

- **controller** — may observe and may request state changes allowed by its
  granted capabilities;
- **observer** — receives snapshots and ordered events but cannot mutate the
  session.

An attachment is not ownership. Disconnecting a UI does not transfer or stop
the runtime. Replacing the runtime owner is an explicit, fenced operation;
stale owners and stale runtime-proxy generations cannot continue writing after
replacement.

## Authorization

Live-session RPC authorization is enforced at the canonical TUI gateway before
a handler runs. Controller status alone is insufficient: each live-session RPC
must map to a capability such as `observe`, `prompt.submit`, `session.steer`,
`session.stop`, `session.close`, `approval.respond`, or `attachment.write`.

Methods which resolve a live session but have no explicit policy fail closed.
Read-only methods use `observe`; mutating methods require a controller capability.
This check also applies when the request arrived through a runtime proxy.

## Ordered events, snapshots, and replay

The runtime owner assigns a monotonically increasing sequence number to each
session event. Clients attach with their last processed sequence number and
receive:

1. an authoritative session snapshot and its sequence watermark;
2. replayed events after the requested watermark; and
3. live events in sequence order.

Live events received during attach are buffered until replay completes. Events
at or below the acknowledged watermark are discarded as duplicates; newer
buffered events are then released in order.

The replay log is bounded and compacted. If the requested sequence predates the
oldest retained event, attach reports a truncated replay. The TUI then fetches
durable `session.history`, reconstructs visible history from that source of
truth, and explicitly acknowledges replay resynchronization at the server
watermark before releasing buffered live events. A truncated replay is never
silently treated as complete.

Snapshots and replay events are protocol data. They must not contain runtime
objects, transport handles, credentials, API keys, or other secrets.

## Idempotent user input

`prompt.submit`, `session.steer`, and `session.redirect` use a
`client_message_id`. The durable idempotency identity is:

```
(authenticated principal or stable client_id, client_message_id)
```

A retry by the same authenticated principal or client is accepted exactly once,
including after reconnect. Two different clients may intentionally use the
same message ID without suppressing each other. The scoped identity is persisted
with user-message display metadata so deduplication survives process reload.
Legacy history containing only a bare message ID remains compatible and is
conservatively treated as a global duplicate.

## Busy input and backpressure

Busy input follows the configured `queue`, `steer`, or `interrupt` policy.
Messages which cannot be redirected or steered fall back to the bounded
next-turn queue where the policy permits it.

Both queue implementations are bounded by:

- **32 pending envelopes**, including the head slot and FIFO tail; and
- **1 MiB of aggregate retained UTF-8 payload and attachment metadata**.

The byte budget includes text, media paths/URLs and MIME types, reply/channel
context, interactive response data, and free-form event metadata. Media album
or text merges are projected before mutation, so rejected input cannot partly
modify an already queued envelope.

When admission would exceed either limit, Hermes does not interrupt the active
turn and does not report the input as queued. Messaging clients receive an
explicit “not queued; retry after the current turn” response. Direct TUI RPC
clients receive `-32002 busy queue capacity exceeded`. Claimed attachments and
idempotency reservations are rolled back on direct admission failure.

## Runtime proxy isolation

A runtime proxy provides transport isolation without creating a second session
runtime. Connection establishment is single-flight per client key. Request,
timeout, close, and transport writes are serialized so that:

- only one connection wins for a client key;
- close inserts its sentinel after already accepted writes;
- pending requests resolve exactly once;
- writes after close fail rather than crossing into a replacement generation;
- owner loss produces an explicit owner-unavailable response; and
- handshake timeout uses one absolute deadline.

## Platform support contract

A platform adapter may advertise live-runtime support only if it preserves the
normalized `MessageEvent` contract, stable session routing, reply identity,
ordered busy-input admission, and explicit rejection behavior. Defaults in
registry metadata are not proof of support; representative production adapters
are covered by behavioral contract tests.

Discord acceptance coverage exercises production `GatewayRunner` ingress and
the real adapter boundary, including canonical owner routing and exactly-once
replay after a client disconnects and reconnects.

## Operator diagnostics

Useful failure signals include:

- `4003 live session RPC has no authorization policy` — a live-session method
  lacks an explicit capability classification;
- `-32002 busy queue capacity exceeded` — direct TUI input was rejected and was
  not queued;
- `Pending queue is full ... message was not queued` — messaging input was
  rejected and should be retried after the current turn;
- replay truncation followed by durable history resynchronization — expected
  recovery after compaction, not data loss; and
- owner-unavailable runtime-proxy responses — the canonical runtime owner was
  closed, replaced, or failed its handshake.

Do not work around these signals by granting broad controller permissions,
disabling bounds, or starting a second runtime. Fix the ownership, capability,
or replay state at the canonical owner.
