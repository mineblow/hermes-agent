# Hermes Multi-Client Live Session Feasibility Investigation

Date: 2026-08-28

## Scope and source baseline

This is an investigation only. No Hermes source was modified.

The detailed source audit uses the locally cloned upstream Hermes Agent `0.20.6` tree at commit `420e156`:

`references/upstream/hermes-agent/`

The Hermes installation currently deployed in this workspace is `0.19.0`. Its TUI Gateway is older: it has the same single `session["transport"]` event destination but does not contain upstream `0.20.6`'s `viewers` fallback registry. Any implementation should first pin and upgrade the workspace to the chosen upstream baseline; the newer cross-process durable turn-lease behavior must not be assumed to exist in deployed `0.19.0`.

## Verdict

**Significant but reasonable architectural refactor.**

Read-only in-process fan-out is a reasonable incremental change. The complete target — one authoritative live runtime, many writable clients, reconnect, approvals, and a cross-process guarantee — is a larger but still well-localized refactor at the TUI Gateway/live-session/transport boundary.

It does not require redesigning AIAgent reasoning, providers, tools, memory, MCP, skills, Kanban, STT/TTS, or the durable session schema.

The most important qualification is scope:

- Multiple clients connected to one TUI Gateway process can share one existing runtime without touching AIAgent.
- Separate TUI Gateway processes still have separate `_sessions` tables and separate AIAgent objects.
- Upstream `0.20.6` now serializes the durable load → run → flush region across processes with a SQLite-backed session turn lease. That prevents simultaneous transcript corruption, but it does **not** create one runtime or permit a client to observe/interrupt the runtime hosted by another process.
- Therefore a true generic live-runtime feature should converge on a **single runtime host** rather than distributed runtime ownership.

## 1. Current architecture

### 1.1 Process-global live-session table

`tui_gateway/server.py:144-166` defines:

```python
_sessions: dict[str, dict] = {}
_sessions_lock = threading.RLock()
_session_resume_lock = threading.Lock()
```

Every JSON-RPC request in a given process uses this same module-global table. WebSocket connections handled by `tui_gateway/ws.py` dispatch into the same imported `tui_gateway.server` module, so connections inside one server process share `_sessions`.

A separately launched stdio TUI Gateway process imports its own module and therefore owns a separate `_sessions` table.

### 1.2 The live-session object

There is no `SessionRuntime` class in the TUI Gateway. The runtime is a mutable dictionary created by `_init_session()` in `tui_gateway/server.py:8632-8676`. Agent construction is performed by `_make_agent()` and can be started through `_start_agent_build()`.

Important fields include:

```text
_sid
session_key              durable SessionDB id, after persistence/resume
agent                    live AIAgent
history
history_lock
history_version
running
_run_thread
inflight_turn
queued_prompt
queued_prompts
_turn_cancel_requested
transport                one authoritative event destination
viewers                   fallback transports, not subscribers
pending approval/input state
notification poller
agent build state
compute-host metadata
```

This dictionary already performs most of the conceptual role proposed for `SessionRuntime`.

### 1.3 AIAgent creation and lifetime

The AIAgent is built in:

- `tui_gateway/server.py:8444-8629` — `_make_agent()` calls `AIAgent(**kwargs)`.
- `tui_gateway/server.py:3134-3205` — `_start_agent_build()` performs the deferred build and installs the result into the session record.
- `tui_gateway/server.py:8632-8676` — `_init_session()` creates the live record and stores the supplied agent in `session["agent"]`.

The same `session["agent"]` is reused across normal turns. `_run_prompt_submit()` reads it at `server.py:12232`, and invokes `agent.run_conversation()` at `server.py:12554` using the session history snapshot. Hermes may intentionally replace/reconfigure the agent during capability synchronization, model switching, compression rotation, or recovery, but it does not normally construct a fresh agent for every turn.

The generic messaging gateway is a separate execution stack. `gateway/run.py:7102-7113` owns its own `GatewayRunner._sessions` and per-session turn-lease registry; `gateway/run.py:7185-7197` owns an AIAgent cache. These are not the same runtime objects as TUI Gateway `_sessions`.

### 1.4 Durable-session-to-live-runtime reuse

`session.resume` is implemented in `tui_gateway/methods_session.py:372-1038`.

Within one TUI Gateway process:

- `_find_live_session_by_key()` (`server.py:10333-10353`) scans `_sessions` by durable `session_key`/agent session id.
- `_claim_or_reuse_live()` (`server.py:10089-10136`) resolves a race under `_session_resume_lock` and returns the existing live winner when possible.
- The resume-local `_reuse_live_response()` (`methods_session.py:622-635`) returns the existing runtime payload.

This means a single process already has the beginnings of a runtime manager and can host several independent sessions while avoiding duplicate live records for the same durable key in most resume races.

It is not a cross-process runtime manager.

### 1.5 Single transport ownership

The authoritative event destination is stored directly on the live-session dictionary:

```python
session["transport"]
```

`write_json()` in `tui_gateway/server.py:2478-2508` routes any session-scoped event to:

```python
(_sessions.get(sid) or {}).get("transport")
```

It calls exactly one transport's `write()` method.

`_event_frame()` / `_emit()` in `server.py:2511-2519` build and send:

```json
{
  "jsonrpc": "2.0",
  "method": "event",
  "params": {
    "type": "...",
    "session_id": "...",
    "payload": {}
  }
}
```

and passes it to `write_json()`.

`event_replay.py:52-75` adds a process epoch and per-session monotonic `seq`, then stores the event in a bounded ring. The current envelope has `type`, `session_id`, `payload`, and `seq`; it does not consistently include `event_id`, `timestamp`, or `turn_id`.

### 1.6 Viewer registry is not fan-out

Upstream `0.20.6` has:

```python
session["viewers"] = {transport: last_seen_timestamp}
```

Relevant code:

- `_live_session_payload()` — `server.py:10442-10466`
- `_close_sessions_for_transport()` — `server.py:1494-1576`

However, `_live_session_payload()` still does:

```python
session["transport"] = transport
```

The viewers map is consulted only when the current transport disconnects. Hermes chooses the most recently seen surviving viewer and assigns it back to the single `session["transport"]` slot.

Tests explicitly encode this behavior:

- `tests/test_tui_gateway_server.py:5153-5174` — closing the active transport rebinds to a remaining viewer.
- `tests/test_tui_gateway_server.py:5222-5234` — resume/activate registers a viewer but also makes it the session transport.

An isolated runtime probe against upstream source produced:

```text
active_transport: B
viewers: [A, B]
A_events: 0
B_events: 1
```

Thus the registry fixes disconnect fallback, not simultaneous observation.

### 1.7 Transport rebinding sites

The single destination is rebound at least at these important points:

- `_live_session_payload()` — resume/activate assigns the caller transport (`server.py:10442-10466`).
- `prompt.submit` — every submit assigns `current_transport()` (`methods_prompt.py:360-364`).
- `_enqueue_prompt()` stores the initiating transport in the queued input envelope (`server.py:9542-9560`).
- `_drain_queued_prompt()` restores that queued transport as the session transport before running the queued turn (`server.py:9790-9793`).
- `_close_sessions_for_transport()` chooses another viewer or the detached sentinel (`server.py:1494-1576`).

This makes the latest resume, active submitter, queued submitter, or disconnect fallback the sole destination for later events.

### 1.8 Event callback path

AIAgent does not need to know about WebSockets.

`_agent_cbs()` (`server.py:7731-7745`) and `_wire_callbacks()` (`server.py:7909-7986`) adapt AIAgent/tool callbacks into `_emit()` calls. Examples include:

- `message.start`, `message.delta`, `message.interim`, `message.complete`
- `tool.start`, `tool.complete`, and tool progress/status
- `subagent.start`, `subagent.thinking`, `subagent.text`, `subagent.tool`, `subagent.complete`
- `approval.request`
- `clarify.request`
- `sudo.request`, `secret.request`, `terminal.read.request`
- `agent.terminal.output`, `terminal.close`
- `error`, usage, session info/title/status events

Terminal process output is bridged at `server.py:11917-11959`. Subagent events are bridged at `server.py:7578-7732`.

Because these paths converge on `_emit()`, event fan-out can be inserted without modifying AIAgent or every tool.

Hermes also contains two adjacent but non-equivalent fan-out mechanisms:

- `_broadcast_global_event()` iterates every process-local live transport for session-less events (`server.py:2522-2565`).
- Dashboard `/api/pub` → `/api/events` uses a per-channel subscriber registry for PTY-side best-effort sidebar events (`hermes_cli/web_server.py:16859-16920`, `17713-17792`).

Both demonstrate useful delivery patterns, but neither is the authoritative per-session runtime event bus. The dashboard channel is a sidecar bridge and does not own turn, approval, interrupt, or replay semantics.

### 1.9 Busy input, queue, steer, and interrupt

`prompt.submit` uses `session["history_lock"]` and `session["running"]` to claim one turn (`methods_prompt.py:365-381` and `812-816`). A turn runs on one `_run_thread` (`methods_prompt.py:867-917`).

Busy-input scheduling already exists in `_handle_busy_submit()` (`server.py:9674-9769`):

- `steer` uses `agent.steer()` when supported.
- `interrupt` prefers active-turn redirect; otherwise queues and interrupts.
- `queue` preserves the turn without interrupting.

`_enqueue_prompt()` and `_drain_queued_prompt()` (`server.py:9518-9560`, `9772-9870`) provide FIFO-like server-side queue handling, including separate image envelopes and merged adjacent text behavior.

`session.interrupt` (`methods_session.py:3327-3637`) locates the session record, then interrupts its `agent` or compute-host turn.

The existing scheduler should remain authoritative. Multi-client input needs origin/request metadata and removal of transport ownership, not a second scheduler.

### 1.10 Approvals and clarification

Blocking requests are runtime/process state, not durable frontend state:

- `_block()` — `server.py:4523-4606`
- `_respond()` — `server.py:13463-13490`
- `clarify.respond` / `approval.respond` — `methods_prompt.py:1515-1693`
- gateway approval registry — `tools/approval.py:2761-2910`

The request is emitted as a session event and a process-global pending entry waits on a `threading.Event`.

The gateway approval registry performs an atomic pop under a lock, so one gateway approval resolution wins. The generic `_respond()` path can still accept a second answer before the waiter removes the pending entry, overwriting `_answers[request_id]`. Multi-client support must make all blocking-request resolution atomic and return a deterministic `already_resolved`/`expired` result to late responders.

There is no general `approval.responded` or `clarify.responded` fan-out today. That should be added so every client clears its pending UI from the authoritative runtime resolution.

### 1.11 Disconnect behavior

`tui_gateway/ws.py:558-582` calls `_close_sessions_for_transport()` when a WebSocket exits.

Current behavior:

- If another viewer exists, Hermes rebinds `session["transport"]` to that viewer.
- If no viewer remains, it assigns `_detached_ws_transport` and schedules an orphan reap.
- Depending on `close_on_disconnect`, active turns may be torn down immediately or interrupted/reaped after a grace period.

The disconnect decision is tied to the transport-owner slot, not subscriber count. Multi-client detach must remove only that subscriber; orphan policy should run only after the subscriber registry is empty.

### 1.12 Dashboard and Desktop process relationship

`hermes_cli/web_server.py:17666-17710` exposes `/api/ws` and delegates to `tui_gateway.ws.handle_ws()`. WebSocket clients connected to one dashboard server therefore share that server process's TUI Gateway `_sessions` table.

When dashboard turn isolation is enabled, `tui_gateway/host_supervisor.py:1-6` states that the dashboard process still owns sockets and JSON-RPC dispatch while one persistent compute-host child runs agent turns. The parent remains the natural event-hub/runtime-host boundary.

A standalone stdio TUI Gateway or a second dashboard server process has separate process-local runtime state.

## 2. Why multi-client currently fails

This is source-verified, not inferred:

1. `session.resume`/activate records both viewers but assigns the caller to the one `session["transport"]` slot.
2. `prompt.submit` repeats that assignment for the submitting connection.
3. `_emit()` calls `write_json()`.
4. `write_json()` selects one session transport and performs one write.
5. No code iterates `session["viewers"]` when emitting an event.
6. Disconnect merely promotes one fallback viewer into the same exclusive slot.
7. Queued prompts carry a transport and rebind ownership when drained.

Therefore client B does not merely attach; it steals the asynchronous event destination from A. A remains connected but receives no new live session events.

Across processes, each process can instantiate its own live session dictionary and AIAgent. Upstream's durable turn lease serializes their durable turns, but clients still cannot share one live event stream, approvals, steer target, or interrupt target. The second process is a separate runtime waiting behind, or later running after, the first.

## 3. Recommended architecture

### 3.1 Use the existing live record as the migration seam

A `SessionRuntimeManager` is the correct end-state abstraction, but replacing every session dictionary access with a new class should not be Phase 1. `server.py` is large and the mutable record is referenced broadly.

Recommended progression:

1. Treat `_sessions` plus `_session_resume_lock` as the initial runtime manager.
2. Add a focused `SessionEventHub`/`SubscriberRegistry` owned by each session record.
3. Move asynchronous event destination logic from `session["transport"]` to that hub.
4. Preserve implicit attach during `session.create`, `session.init`, and `session.resume` for backward compatibility.
5. Add explicit `session.attach` and `session.detach` RPCs for clients that want clear lifecycle semantics.
6. Extract a typed `SessionRuntime` and `SessionRuntimeManager` only after behavior is covered by tests.

This minimizes churn while establishing the correct ownership model.

### 3.2 Separate RPC responses from runtime events

JSON-RPC method responses must remain point-to-point to the requesting connection.

Only asynchronous frames with `method == "event"` should pass through the per-session event hub.

Do not broadcast RPC return values or errors associated with one request id.

### 3.3 Publish once, enqueue many

The target path should be:

```text
AIAgent/tool/background callback
          ↓
       _emit()
          ↓
SessionEventHub.publish(event)
          ↓
assign one session seq/event id and record replay once
          ↓
non-blocking enqueue to each subscriber
          ↓
per-subscriber writer independently calls transport.write()
```

Important details:

- Stamp and replay-record the event **once before fan-out**. Calling current `write_json()` once per subscriber would assign different sequence numbers and duplicate the replay entry.
- Use a per-runtime publication lock to establish one order across callbacks arriving from agent, tool, subagent, and process-output threads.
- Keep per-client bounded queues. Never perform socket writes while holding the publication lock or session history lock.
- If a queue overflows, detach/close that subscriber and require replay/reconnect. Do not silently create an unobservable gap.
- Preserve the existing epoch + `session.events.since` recovery contract; expand the envelope later with `event_id`, `timestamp`, and where available `turn_id`.
- `tool_id`, approval `request_id`, subagent id, and terminal `process_id` already exist in relevant payloads and should remain stable.

### 3.4 Attachment model

A subscriber record should contain operational state only:

```text
client_id
transport
attached_at
last_seen_at
last_seen_seq
capabilities
permissions/read-only flag
```

It must not contain conversation history or duplicate approval state.

`session.resume` should load/reuse the runtime and implicitly attach the caller. It must never detach or demote existing subscribers.

`session.detach` removes only the current client. UI tab focus remains client-local; a client should attach/detach intentionally rather than server ownership following selected tabs.

### 3.5 Input routing

Reuse `prompt.submit`, `_handle_busy_submit`, `_enqueue_prompt`, `_drain_queued_prompt`, `agent.steer()`, and `session.interrupt`.

Changes needed:

- Resolve a durable/live session through the shared runtime manager.
- Add `client_id`, `request_id`, and optional origin metadata to queued envelopes.
- Remove queued `transport` as an event destination.
- Keep RPC acknowledgment point-to-point.
- Broadcast resulting runtime events to every subscriber.
- Use the existing `history_lock`/`running` claim so only one turn starts.

The current queue merges adjacent text inputs in some circumstances. If exact one-command-per-request semantics are required, preserve origin/request ids through a merge or disable merging for multi-client submissions. This must be decided explicitly and tested; otherwise two users' inputs could be combined into one user message.

### 3.6 Approval and clarification ownership

Pending requests remain session/run state.

- Broadcast request to every attached readable client.
- Keep subscriber membership separate from controller/approver authority.
- Allow only clients carrying the explicit `approve` or `clarify` capability to respond; a read-only subscriber must never acquire authority merely by attaching.
- Choose and expose an authority policy: one renewable controller lease by default, or first response among explicitly authorized approvers. Do not infer authority from “most recently resumed” transport.
- Resolve using an atomic compare-and-set/pop.
- First valid response wins.
- Return deterministic `already_resolved` or `expired` to duplicates.
- Broadcast a resolution event to all subscribers.

Do not maintain one pending approval per client.

Before multi-client exposure, also fix an existing approval validation discrepancy: TUI `approval.respond` forwards arbitrary `choice` values (`methods_prompt.py:1665-1690`), while downstream treats any resolved value other than `deny`/`None` as approved (`tools/approval.py:5662-5699`). The REST Runs API correctly allowlists `once|session|always|deny` (`gateway/platforms/api_server.py:8086-8097`). The TUI Gateway must apply the same exact allowlist before resolution.

The gateway approval queue already resolves a request atomically under its lock (`tools/approval.py:2829-2868`). Generic clarification `_respond()` does not: a second response can overwrite `_answers[request_id]` after the event is set but before waiter cleanup (`server.py:13463-13490`). Clarification therefore needs compare-and-set resolution; approval should preserve its existing atomic behavior.

### 3.7 Runtime lifetime

Subscriber count and turn/delegation activity should drive lifecycle:

- Any subscribers: runtime stays loaded.
- No subscribers but active turn/delegation/blocking request: continue according to explicit orphan policy.
- No subscribers and idle: start an idle timer.
- Timer expiry: finalize and unload runtime, preserving durable session.

The existing WS orphan-reap machinery can be adapted from “single transport is detached” to “subscriber count is zero.”

## 4. Scope and compatibility

### Primary server modules

Likely modified:

- `tui_gateway/server.py`
  - `_sessions` lifecycle helpers
  - `_init_session`, `_live_session_payload`, `_reuse_live_response`
  - `write_json`, `_emit`
  - `_close_sessions_for_transport`, orphan checks
  - queued-input transport fields
  - blocking request resolution events
- `tui_gateway/methods_session.py`
  - `session.resume`
  - new attach/detach semantics
  - structured `session.status`
  - interrupt/status/replay integration
- `tui_gateway/methods_prompt.py`
  - stop rebinding on `prompt.submit`
  - input origin metadata
  - deterministic approval/clarify duplicate handling
- `tui_gateway/transport.py`
  - transport remains connection I/O; no runtime ownership
  - global live-transport broadcast remains separate from per-session fan-out
- `tui_gateway/ws.py`
  - client identity
  - disconnect detaches subscriptions
  - per-client writer queue/cleanup
- `tui_gateway/event_replay.py`
  - stamp/record once before fan-out
  - immutable/copy-safe replay entries
- new focused module, e.g. `tui_gateway/session_events.py` or `runtime.py`

### Tests

Likely modified/added:

- `tests/test_tui_gateway_server.py`
- `tests/tui_gateway/test_protocol.py`
- WebSocket integration tests
- race tests for resume/attach, submit, approval, disconnect, queue overflow, and replay
- process-level tests retaining the durable session turn lease

### Modules probably not changed in the first implementation

- `run_agent.py` / AIAgent reasoning loop
- tool implementations
- memory, MCP, skills, Kanban, STT/TTS
- durable messages/session schema
- `gateway/session.py`, `gateway/run.py`, `gateway/session_context.py`
- `hermes_cli/active_sessions.py`
- compute-host agent code

The parent-side compute-host relay may need adaptation only at its event sink so relayed events enter the same hub.

### Client compatibility

Backward compatibility is achievable:

- Existing stdio and WebSocket JSON-RPC clients can remain implicitly attached after create/init/resume.
- Existing event frames can retain their current shape and add optional fields.
- Unknown fields are already tolerated by the TUI.
- Single-client behavior becomes a one-subscriber special case.
- Existing clients need no new `session.attach` call until explicit attachment is desired.

The main observable semantic change is beneficial: another client resuming/submitting no longer redirects the first client's stream.

Migration difficulty is medium for in-process read-only fan-out and medium-high for the complete writable/reconnect/lifecycle feature because the current transport field is referenced in queue and disconnect behavior.

## 5. Cross-process implications

### 5.1 Existing protections

There are two different lease systems and they must not be conflated.

`hermes_cli/active_sessions.py` is a concurrency-cap/liveness registry. It does not enforce unique ownership by durable session id. A probe acquired two distinct active-session leases for the same `session_id` without error.

Upstream `0.20.6` also has a durable SQLite session turn lease:

- `hermes_state.py:7949-8111`
- `run_agent.py:8684-8912`
- `tests/state/test_session_turn_lease.py`

It atomically serializes the full load → run → flush region across processes, refreshes a TTL, fences release by holder identity, waits/reloads latest history after contention, and fails closed on timeout/lost lease.

This substantially reduces persistence-corruption risk in current upstream.

It is not a complete linearizability guarantee for every higher-level gateway path. `GatewayRunner` can load history before `AIAgent.run_conversation()` acquires the durable lease (`gateway/run.py:19867-19874`), while the agent reloads after acquisition only when acquisition reported contention (`run_agent.py:8831-8846`). A process can load stale history, wait outside the lease while another process completes, then acquire immediately after release and skip the contention-triggered reload. This narrow race is another reason not to treat independent active/active gateway processes as equivalent to one runtime host.

It does not enforce zero-or-one loaded AIAgent. Multiple processes can still hold idle live agents for the same durable session and cannot share in-memory approvals, events, interrupt state, queued input, or subagents.

The durable lease is host-local protection, not distributed consensus. Holder identity contains a local PID, and dead-owner reclamation consults the local process table (`hermes_state.py:180-232`). Two hosts mounting the same `state.db` could misclassify one another's holders. Do not use shared-filesystem, multi-host runtime ownership; a canonical host must be the sole runtime writer for a Hermes home/profile.

### 5.2 Preferred model: one runtime host

Use Option A.

One long-lived TUI Runtime Host should own:

- `_sessions` / future `SessionRuntimeManager`
- AIAgent instances
- turns, queues, approvals, interruptions
- event hubs and replay rings

TUI, Desktop, VS Code, Control Center, and future voice clients connect to that host over the existing JSON-RPC WebSocket/stdio-compatible protocol.

This is consistent with the dashboard architecture: its parent process already owns sockets/dispatch and can optionally delegate compute to one child.

A standalone stdio TUI should eventually become a client/proxy to the canonical host rather than silently starting an independent runtime owner when a host is available.

Keep the SQLite turn lease as defense-in-depth for old clients, CLI direct execution, crashes, mixed versions, and non-interactive processes during migration.

### 5.3 Why distributed runtime ownership is not recommended

A durable lock can tell process B that process A owns a runtime, but it cannot deliver A's events or route B's input. Distributed ownership also requires:

- owner discovery and address publication
- authenticated proxying
- subscriber forwarding
- failover and fencing
- replay across owner death
- approval/interrupt routing
- split-brain handling

No current Hermes abstraction provides that complete protocol. Building it before a single-host design would be disproportionate.

### 5.4 Generic gateway/API relationship

The TUI Gateway and `GatewayRunner` are separate runtime stacks. A first implementation can provide the capability to all interactive clients that speak TUI Gateway JSON-RPC.

If the requirement later becomes “a Telegram/Discord gateway turn and a Desktop/TUI turn must literally share one in-memory AIAgent,” those adapters must submit/proxy through the canonical runtime host. Merely sharing SessionDB is insufficient. That would be a later migration, not part of read-only fan-out.

## 6. Risk assessment

### Persistence corruption — medium initially, low with controls

Fan-out itself does not write persistence more than once because one runtime still performs one turn. The principal danger is accidentally starting one turn per subscriber or allowing two prompt claims. Keep execution and persistence exclusively on the runtime, never in subscriber handlers.

Retain upstream's durable session turn lease for cross-process defense.

### Event ordering — medium-high

Callbacks arrive from multiple threads. Current `_stamp_event()` sequencing and the subsequent transport write are separate critical sections, so two threads can receive sequence numbers 1 and 2 but enter the transport in reverse order. Assign sequence/order and enqueue once under a per-runtime publication lock. Do not stamp independently per subscriber. Test byte-equivalent ordered event sequences for two clients.

### Deadlocks — medium

Never perform network writes while holding `history_lock`, `_session_resume_lock`, approval locks, or event publication lock. Subscriber queues must make publish non-blocking. Lock-order tests and forced slow transports are required.

### Input races — high

Two clients can submit simultaneously. Preserve the current claim-under-`history_lock` logic and scheduler. Test simultaneous first-turn submissions, busy queue/steer/interrupt, and exact persistence counts.

The existing adjacent-text queue merge needs explicit multi-client semantics.

### Transport backpressure — high without queues, low with queues

`WSTransport.write()` can wait up to ten seconds for non-token frames. Direct sequential fan-out would let one dead socket delay every client and potentially the agent thread. Use bounded per-client queues and independent writer tasks/threads.

### Approval/clarification races — high

Preserve atomic gateway approval resolution and make generic clarification resolution atomic. First authorized, schema-valid response wins and emits a session-wide resolution event. Current generic `_respond()` can be overwritten during its wakeup window.

Validate approval choices against exactly `once|session|always|deny` before resolution. Any other value must receive an exact rejection and must not wake or mutate the pending approval.

### Replay/overflow — medium

The ring is bounded to 512 events per session. Overflowed or long-disconnected clients may need full history/status refresh. A subscriber drop must be explicit; never pretend it remained gap-free.

### Disconnect and lifecycle — medium-high

Current orphan behavior is based on one transport. Rebase it on subscriber count plus active turn/delegation/blocking state. Test every combination of active/idle and one/many subscribers.

### Backward compatibility — low-medium

Implicit attachment and unchanged JSON-RPC event shapes preserve existing clients. Avoid making `session.attach` mandatory in the first release. Keep point-to-point method responses.

### Security and permissions — medium

A read-only observer must not automatically gain prompt/approval/interrupt authority. Today, any authenticated WS client that knows the session/request identifier can attempt `approval.respond` or `clarify.respond`; pending approval replay is a snapshot, not an ownership claim. Subscriber identity, explicit capabilities, and controller/approver authorization checks must be introduced before exposing the host beyond the existing trusted boundary.

## 7. Smallest prototype recommendation

Do not start with a full `SessionRuntimeManager` rewrite.

Build a test-only/minimal `SessionEventHub` proof around one existing `_sessions[sid]` record.

### Prototype changes

1. Add a per-session subscriber collection with two fake/WebSocket transports.
2. Make resume/create implicitly add the current connection without overwriting existing subscribers.
3. Route only `_emit()` session events through the hub.
4. Stamp/replay-record once, then enqueue to both subscribers.
5. Keep prompt submission writable only from client A.
6. Detach B on disconnect; keep A and runtime untouched.
7. Leave cross-process behavior out of this prototype.

### Required proof

```text
A creates session S
B resumes/attaches S
A submits one prompt
A and B receive the same ordered seq/type/payload stream
both receive message.delta, tool.start/progress/complete, message.complete
B disconnects
A continues receiving
SessionDB contains one user message and one assistant message
_sessions contains one live record for S
one AIAgent.run_conversation invocation occurred
```

### Required failure injection

- B's transport blocks or raises: A and agent execution remain unaffected.
- B's queue fills: B is detached and told to replay/reconnect; A remains complete.
- B attaches during an active stream: it receives subsequent live events and uses `session.events.since` for the prefix without duplicates.

Before writable multi-client support, add focused tests for:

- two authorized approval responders racing: one winner, one `already_resolved`;
- two clarification responders racing: no answer overwrite;
- an observer attempting submit/steer/interrupt/approve/clarify: exact authorization rejection;
- invalid approval choices: exact schema rejection with the pending approval unchanged;
- controller disconnect/lease expiry and deliberate authority transfer.

### Tests before real clients

Use fake transports first. They make exact ordering, queue behavior, and write counts deterministic. Then add a two-WebSocket integration test.

## 8. Answers to the 20 feasibility questions

1. **Where is TUI AIAgent instantiated?** `_make_agent()` / deferred build in `tui_gateway/server.py`, stored in the live session dictionary.
2. **What represents a live session?** One mutable record in process-global `tui_gateway.server._sessions`.
3. **Is one agent reused across turns?** Yes, normally `session["agent"]`; intentional rebuild/swap paths exist.
4. **Where is transport stored?** `session["transport"]`.
5. **What rebinds it?** Resume/activate payload, every prompt submit, queued prompt drain, and disconnect fallback.
6. **How are events emitted?** AIAgent/tool/background callbacks → `_emit()` → `write_json()` → one session transport.
7. **Can output be fanned out without touching AIAgent?** Yes. `_emit`/`write_json` is the convergence seam.
8. **How are queued prompts handled?** Existing busy policy plus `queued_prompt`/`queued_prompts`, drained under `history_lock`; queued entries currently pin transport.
9. **How do steer/interrupt find the agent?** They look up `_sessions[sid]`, then call its agent or compute-host supervisor.
10. **How are approvals stored/resolved?** Process-global pending request/event maps plus the gateway approval registry; not per frontend.
11. **What happens on disconnect?** Rebind to one remaining viewer or detach and orphan-reap/interrupt according to policy.
12. **Can one process host several sessions?** Yes; `_sessions` is keyed by UI runtime id and each record owns an agent/build/turn state.
13. **Do WebSockets share the session table?** Yes inside one server process; no across processes.
14. **What does active_sessions.py do?** Concurrency accounting/liveness and caps, not unique runtime ownership by session id.
15. **What prevents two processes resuming one durable session?** Nothing prevents two loaded agents. Upstream's SQLite turn lease prevents their durable turns from running concurrently.
16. **Can the existing lease enforce runtime ownership?** Not as written. It is a per-turn serialization lease and has no event/proxy endpoint. Keep it as safety, not runtime-host discovery.
17. **Does Dashboard use the TUI Gateway?** Yes, `/api/ws` calls `tui_gateway.ws.handle_ws()` in the dashboard process; turn isolation may use one compute child.
18. **Is there pub/sub already?** There is global live-transport broadcast, dashboard `/api/pub` → `/api/events` channel fan-out, plugin-local event delivery, and per-session replay, but no authoritative per-session live subscriber hub. The `viewers` map is only failover metadata.
19. **What stdio assumptions matter?** Stdio expects implicit session attachment and the existing event schema; those can remain. A separately spawned stdio gateway still creates a separate process/runtime unless converted to a host client.
20. **Can it be incremental?** Yes: read-only hub → writable routing → approval/interrupt → reconnect → canonical host/cross-process migration.

## Final recommendation

Proceed with a read-only in-process fan-out prototype against a pinned upstream baseline.

Treat the current session dictionary as the runtime for Phase 1, introduce a focused per-session event hub, and preserve all existing turn scheduling. Do not begin with distributed ownership or a full AIAgent/session rewrite.

For the full invariant, make the TUI Gateway/dashboard server the canonical runtime host and have interactive clients attach to it. Retain the upstream SQLite session turn lease as a fail-closed safety net while older or independent Hermes processes still exist.
