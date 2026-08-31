# Multi-Client Live Sessions Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Let multiple simultaneous TUI Gateway clients observe and, when authorized, control one authoritative live Hermes runtime without event stealing, duplicate agent turns, duplicate persistence, or regressions in existing single-client behavior.

**Architecture:** Evolve the existing process-local `_sessions` record into an explicit runtime/attachment boundary rather than rewriting `AIAgent`. A per-session event hub stamps each event once, records replay once, and sends it through bounded subscriber queues. Existing prompt admission remains the single input scheduler; connection identity and attachment capabilities control which clients may submit, steer, interrupt, approve, or answer clarification. One dashboard/TUI Gateway process is the runtime host; independent processes remain fenced by the durable SQLite turn lease and must connect to the same host to share live state.

**Tech Stack:** Python 3.11, synchronous JSON-RPC TUI Gateway, FastAPI/WebSocket transport, threading/queues, existing `event_replay`, pytest through `scripts/run_tests.sh`, Ruff, shared TypeScript gateway client, Electron/Desktop and Web clients.

**Source baseline:** fork `mineblow/hermes-agent`, commit `4e7eb39947f132f961923f9e3f600bc8e63066dd`, package version `0.20.6`.

**Branch:** `feat/multi-client-live-sessions`.

**Companion audit:** `docs/plans/2026-08-28-multi-client-live-sessions-feasibility.md`.

---

## Non-negotiable invariants

1. One live TUI Gateway process owns at most one authoritative runtime record for a durable session key.
2. One accepted prompt produces exactly one `AIAgent.run_conversation()` call and one persisted user/assistant turn, regardless of subscriber count.
3. Runtime events are stamped and replay-recorded once, then delivered to all attached observers in the same per-session order.
4. RPC responses and errors stay point-to-point to the requesting transport.
5. A slow, failed, or disconnected subscriber cannot block the agent or another subscriber.
6. Read-only observers cannot submit, steer, interrupt, approve, clarify, or answer UI-tool requests.
7. Existing clients that do not send attachment metadata retain current full-control behavior.
8. One client disconnect removes only that attachment. Runtime orphan/idle policy begins only when no attachments remain.
9. Approval and clarification resolution is authorized, schema-valid, and exactly once.
10. Existing durable session turn leases remain fail-closed defense-in-depth across independent processes.
11. Event fan-out does not mutate conversation history or the system prompt and therefore does not affect prompt caching or role alternation.
12. Existing TUI, Dashboard, Desktop, compute-host, terminal, tool, subagent, replay, and single-client tests remain green apart from documented clean-main baseline failures.

## Baseline and quality gates

Repository rules require `scripts/run_tests.sh`; never use direct `pytest` for acceptance.

Recorded clean-main targeted baseline:

```text
629 passed, 1 unrelated failure
failure: test_model_options_preserves_canonical_custom_row_after_agent_init
```

Every task follows RED → GREEN → REFACTOR:

```bash
scripts/run_tests.sh <test-file> -k <new-test> -q
scripts/run_tests.sh <affected-test-files> -q
.venv/bin/ruff check <changed-python-files>
git diff --check
```

Final gates:

```bash
scripts/run_tests.sh tests/test_tui_gateway_server.py \
  tests/test_tui_gateway_ws.py \
  tests/test_tui_gateway_event_replay.py \
  tests/test_tui_gateway_queue_on_busy.py -q
scripts/run_tests.sh tests/tui_gateway/ -q
scripts/run_tests.sh -q
.venv/bin/ruff check .
.venv/bin/ty check
.venv/bin/python scripts/check-windows-footguns.py --all
npm run check --workspace @hermes/shared
npm run check --workspace hermes
```

Only new failures block the branch; the existing baseline failure must remain unchanged or be independently fixed on `main`, not folded into this PR.

---

## Protocol contract

### Connection identity

Every transport has two identities:

```text
connection_id = server-minted random UUID, unique per socket
client_id     = opaque per-window identity, stable across reconnects
stdio         = implicit legacy client_id/connection_id "stdio"
```

`gateway.ready.payload` advertises the additive protocol without trusting an
RPC-supplied identifier as authentication:

```json
{
  "multi_client_sessions": 1,
  "connection_id": "server-uuid",
  "capabilities": ["client.attach", "session.attach", "session.detach", "session.attachments"]
}
```

Negotiated clients then call `client.attach` once per socket:

```json
{
  "protocol_version": 1,
  "client_id": "opaque-stable-per-window-id",
  "surface": "desktop",
  "capabilities": ["session.observe", "session.control", "session.replay"]
}
```

The server validates length/shape, intersects requested capabilities with its
allowlist, and stores authority as `(server-verified auth principal, client_id,
connection_id)`. It never authorizes from `client_id` alone. Unknown requested
capabilities are ignored and the accepted set is returned. Desktop keeps the id
for one renderer/window; browser clients use `sessionStorage`, never shared
`localStorage`. Older clients that never negotiate receive an internal legacy
identity and preserve current behavior.

### Attachment modes

Canonical modes and capabilities:

```python
ATTACHMENT_MODES = {
    "observe": {"observe"},
    "control": {
        "observe",
        "prompt.submit",
        "session.steer",
        "session.interrupt",
        "approval.respond",
        "clarify.respond",
        "ui.respond",
    },
}
```

- `session.create`, `session.init`, and `session.resume` implicitly attach as `control` when no `attachment_mode` is supplied.
- New clients may pass `attachment_mode: "observe"` to resume without briefly acquiring control.
- A client may explicitly downgrade or upgrade through `session.attach`; authorization policy must approve the requested mode.
- The initial local/trusted implementation permits authenticated clients to request `control`, matching existing behavior. The capability checks establish the boundary for stricter remote policy without adding speculative configuration.

### New RPCs

`session.attach`:

```json
{
  "session_id": "live-sid",
  "mode": "observe",
  "last_seen_seq": 120
}
```

Result:

```json
{
  "session_id": "live-sid",
  "client_id": "uuid",
  "mode": "observe",
  "capabilities": ["observe"],
  "latest_seq": 123,
  "replay_epoch": "...",
  "replay": {"events": [], "truncated": false}
}
```

`session.detach` removes only the requesting transport from one session.

`session.attachments` is control-only and returns sanitized attachment metadata; it never exposes auth tokens or transport internals.

### RPC errors

Use existing JSON-RPC error helpers/codes where applicable and stable message/data contracts:

```text
invalid attachment mode       → invalid params
not attached                  → session/client state error
missing capability            → authorization error with required capability
already resolved              → successful result status=already_resolved
expired request               → successful result status=expired
invalid approval choice       → invalid params; pending request unchanged
subscriber overflow           → attachment removed; socket may reconnect/replay
```

### Approval choices

The TUI Gateway must enforce exactly the same allowlist as the Runs API:

```text
once | session | always | deny
```

Any other choice is rejected before pending state changes.

---

## Task 1: Add a focused attachment/event-hub module

**Objective:** Implement independently testable subscriber identity, capabilities, ordered fan-out, bounded queues, and cleanup without changing existing routing yet.

**Files:**
- Create: `tui_gateway/session_events.py`
- Create: `tests/tui_gateway/test_session_events.py`

**Step 1: Write failing tests**

Cover:

1. `AttachmentMode.parse("observe"|"control")` and invalid-mode rejection.
2. Capabilities are immutable and match the protocol contract.
3. Attaching the same transport/client twice is idempotent and updates mode rather than duplicating delivery.
4. Publishing one event delivers the same immutable frame once to two fake transports.
5. Concurrent publishers produce one monotonically ordered sequence observed identically by both clients.
6. A failed writer is detached without affecting another subscriber.
7. A full bounded queue detaches only the slow subscriber.
8. Detach and `close()` stop writer workers without leaking threads.
9. Metadata snapshots contain no transport object or credentials.

Run and verify RED:

```bash
scripts/run_tests.sh tests/tui_gateway/test_session_events.py -q
```

Expected: import or symbol failures because the module does not exist.

**Step 2: Implement the minimal module**

Required public surface:

```python
class AttachmentMode(str, Enum): ...

@dataclass(frozen=True)
class AttachmentSnapshot: ...

class SessionEventHub:
    def attach(self, transport, *, client_id: str, mode: AttachmentMode) -> AttachmentSnapshot: ...
    def detach(self, transport) -> bool: ...
    def has_transport(self, transport) -> bool: ...
    def require(self, transport, capability: str) -> None: ...
    def publish(self, frame: dict) -> bool: ...
    def snapshots(self) -> list[dict]: ...
    def count(self) -> int: ...
    def close(self) -> None: ...
```

Implementation constraints:

- One publication lock determines event order.
- One bounded queue + worker per attachment isolates transport writes.
- Queue size is a module constant, not a new user-facing environment variable.
- Deep-copy or immutable-copy frames per subscriber.
- No network write while holding the publication or registry lock.
- Publish returns after enqueueing, not after socket delivery.
- Worker failure/overflow invokes an injected/session cleanup callback exactly once.

**Step 3: Run GREEN and regression tests**

```bash
scripts/run_tests.sh tests/tui_gateway/test_session_events.py -q
.venv/bin/ruff check tui_gateway/session_events.py tests/tui_gateway/test_session_events.py
git diff --check
```

---

## Task 2: Make event sequencing an atomic publish operation

**Objective:** Stamp and replay-record each session event exactly once before subscriber fan-out, preserving existing replay schema and single-client behavior.

**Files:**
- Modify: `tui_gateway/event_replay.py`
- Modify: `tui_gateway/server.py` around `write_json`, `_event_frame`, `_emit`
- Modify: `tests/test_tui_gateway_event_replay.py`
- Modify: `tests/test_tui_gateway_server.py`

**Step 1: Write failing tests**

1. Two subscribers receive identical `seq`, epoch, type, session id, and payload.
2. Replay ring receives one entry, not one per subscriber.
3. Two concurrent `_emit()` calls cannot deliver `seq=2` before `seq=1` to any subscriber.
4. JSON-RPC responses still go only to `current_transport()`.
5. Sessions without a hub retain the legacy transport fallback during migration.

Run RED with file and `-k` filters.

**Step 2: Implement**

- Expose a stamp-and-record operation that is safe to call once under the hub publication lock.
- `write_json()` detects session event frames and delegates to the session hub when present.
- Do not stamp independently in each worker.
- Preserve existing event envelope and `session.events.since` response.

**Step 3: Verify**

```bash
scripts/run_tests.sh tests/test_tui_gateway_event_replay.py tests/test_tui_gateway_server.py -q
```

Only the recorded unrelated clean-main failure may remain.

---

## Task 3: Attach clients during session lifecycle without transport stealing

**Objective:** Initialize one hub per live runtime and make create/init/resume add an attachment rather than overwrite the sole event destination.

**Files:**
- Modify: `tui_gateway/server.py`
- Modify: `tui_gateway/methods_session.py`
- Modify: `tests/test_tui_gateway_server.py`

**Step 1: Write failing tests**

1. Create implicitly attaches legacy caller as control.
2. Resume from client B preserves client A and delivers to both.
3. Repeated resume by B is idempotent.
4. `attachment_mode="observe"` attaches read-only from the first resume operation.
5. Simultaneous resumes construct/reuse exactly one live agent and attach both callers.
6. Existing live payload fields remain unchanged; new attachment fields are additive.
7. Callback closures still capture `sid`, not transport.

**Step 2: Implement**

Add focused helpers in `server.py` or the new module:

```python
_ensure_session_event_hub(sid, session)
_attach_current_transport(sid, session, mode)
_detach_transport_from_session(sid, session, transport)
```

Stop assigning `session["transport"]` as ownership in normal live payload paths. Keep a compatibility alias only where an untouched legacy code path requires it, and document every remaining assignment.

**Step 3: Verify targeted server tests.**

---

## Task 4: Add client negotiation and explicit session attachment RPCs

**Objective:** Provide stable reconnect identity and generic subscription lifecycle while retaining implicit behavior for older clients.

**Files:**
- Modify: `tui_gateway/methods_session.py`
- Modify: `tui_gateway/method_ctx.py` only if a shared helper is required
- Modify: `tests/test_tui_gateway_server.py`
- Modify: shared TypeScript protocol types under `apps/shared/src/`
- Add/modify: shared gateway protocol tests

**Step 1: Write failing Python and TypeScript tests**

Cover `client.attach` negotiation, malformed/oversized ids, capability
intersection, auth-principal binding, exact session request/result schemas,
invalid mode, unknown session, idempotency, replay from `last_seen_seq`,
detach-only-current-client, sanitized attachment list, and legacy clients.

**Step 2: Implement RPCs and additive protocol types.**

**Step 3: Run Python and shared package tests/typecheck.**

---

## Task 5: Give transports server-minted connection identity and negotiated client identity

**Objective:** Make authorization and attachment idempotency connection-scoped without leaking authentication data.

**Files:**
- Modify: `tui_gateway/transport.py`
- Modify: `tui_gateway/ws.py`
- Modify: `tui_gateway/entry.py` if stdio initialization needs identity
- Modify: `tests/test_tui_gateway_ws.py`
- Modify: `tests/test_tui_gateway_server.py`

**Step 1: Write failing tests**

1. Each WebSocket gets one UUID `connection_id` stable for its lifetime.
2. Different WebSockets have different connection IDs.
3. `client.attach` installs a validated stable `client_id` bound to the authenticated principal.
4. A reconnect may reuse its client id but gets a new connection id.
5. Welcome advertises additive identity/capability fields.
6. No auth token/identity secret enters attachment snapshots or events.
7. Stdio receives stable implicit `stdio` identity and unchanged output format.

**Step 2: Implement UUID connection identity plus validated negotiation.**

Do not derive client ids from token, IP address, headers, or user-agent.

---

## Task 6: Isolate subscriber backpressure and cleanup

**Objective:** Prove that slow/dead observers cannot stall tokens, tool progress, terminal activity, approvals, or completion for another client.

**Files:**
- Modify: `tui_gateway/session_events.py`
- Modify: `tui_gateway/ws.py`
- Modify: `tests/tui_gateway/test_session_events.py`
- Modify: `tests/test_tui_gateway_ws.py`

**Step 1: Write failing timing/cleanup tests**

- Slow transport blocks longer than publish deadline; fast transport still receives completion.
- Queue overflow detaches exactly one client.
- Writer exception closes attachment once.
- Shutdown leaves no worker thread.
- Disconnect racing with publish neither deadlocks nor duplicates.

Avoid brittle wall-clock assertions; use events/barriers and bounded safety timeouts.

**Step 2: Implement deterministic detach callbacks and worker lifecycle.**

---

## Task 7: Enforce observer/control capabilities on all mutation RPCs

**Objective:** Ensure read-only clients can observe everything but cannot mutate the runtime.

**Files:**
- Modify: `tui_gateway/methods_prompt.py`
- Modify: `tui_gateway/methods_session.py`
- Modify: `tui_gateway/server.py` shared response helper paths
- Modify: `tests/test_tui_gateway_server.py`

**Mutation coverage:**

- `prompt.submit`
- `session.steer`
- redirect/busy policies that mutate active input
- `session.interrupt`
- `approval.respond`
- `clarify.respond`
- terminal/preview/window/browser/UI response RPCs routed through `_respond()`

**Step 1: Write one behavioral test per mutation family**

Observer calls must return the exact authorization error, leave state unchanged, and not rebind event delivery. Control calls retain current behavior.

**Step 2: Implement one shared capability check.**

Do not copy authorization logic into every method. Authorization must use `current_transport()` attachment state for the target runtime.

---

## Task 8: Preserve one scheduler for simultaneous multi-client input

**Objective:** Route every writable client through the existing atomic prompt admission, queue, steer, and interrupt paths with origin metadata but no transport ownership.

**Files:**
- Modify: `tui_gateway/methods_prompt.py`
- Modify: `tui_gateway/server.py` queue helpers and runtime events
- Modify: `tests/test_tui_gateway_queue_on_busy.py`
- Modify: `tests/test_tui_gateway_server.py`

**Step 1: Write failing concurrency tests**

1. Two clients submit simultaneously while idle: one turn starts and the other follows configured busy policy.
2. Queue mode preserves FIFO request order and client/request origin.
3. Adjacent messages from different clients are never silently merged into one persisted user message.
4. Prompt versus interrupt has a deterministic result and clears queue as today.
5. Steer from a control attachment targets the same agent and leftover steer queues once.
6. Subscriber count never changes run/persistence counts.

**Step 2: Implement**

- Add `origin_client_id` and JSON-RPC `request_id` to queued envelopes and inflight snapshots.
- Remove queued `transport` event-destination fields.
- Disable text merging across different origin clients; preserve existing merging for one client where current behavior requires it.
- Continue to claim `running` under `history_lock`.

---

## Task 9: Harden approval choices and exactly-once authorization

**Objective:** Make approval safe under multiple authorized responders and close the existing arbitrary-choice discrepancy.

**Files:**
- Modify: `tui_gateway/methods_prompt.py`
- Modify: `tools/approval.py` only if the existing atomic API cannot expose outcome details
- Modify: `tests/test_tui_gateway_server.py`
- Modify/add: approval-focused tests

**Step 1: Write failing tests**

1. Only `once|session|always|deny` is accepted.
2. Invalid choice leaves pending approval unresolved.
3. Observer gets authorization rejection.
4. Two authorized responses race: one resolves; loser receives `already_resolved`.
5. Resolution event broadcasts winning request id/choice/status without credentials or command secrets beyond existing redaction policy.
6. Reconnect pending snapshot remains read-only and does not claim ownership.

**Step 2: Implement validation before `resolve_gateway_approval()`.**

Retain the existing lock-protected queue/pop behavior; do not replace it with a per-client approval.

---

## Task 10: Make clarification and generic UI responses exactly once

**Objective:** Remove `_respond()`’s overwrite window and define deterministic duplicate/expired outcomes.

**Files:**
- Modify: `tui_gateway/server.py` `_block`/`_respond`
- Modify: `tui_gateway/methods_prompt.py`
- Modify: `tests/test_tui_gateway_server.py`

**Step 1: Write failing race tests**

- Two clarification answers race: first authorized answer remains final.
- Batch clarification cannot be edited after final resolution.
- Duplicate response returns `already_resolved`.
- Timeout/late answer returns `expired` without recreating state.
- Interrupt expires pending requests and broadcasts resolution/expiration.
- Existing terminal/preview/window response behavior remains compatible.

**Step 2: Implement lock-protected compare-and-set state.**

Represent pending/resolved/expired explicitly enough to distinguish duplicate from unknown without unbounded memory. Use bounded/TTL cleanup if tombstones are required.

---

## Task 11: Rebase disconnect/orphan lifetime on attachment count

**Objective:** Disconnect one client without changing runtime ownership or interrupting active work while others remain.

**Files:**
- Modify: `tui_gateway/server.py`
- Modify: `tui_gateway/ws.py`
- Modify: `tests/test_tui_gateway_server.py`
- Modify: `tests/test_tui_gateway_ws.py`

**Step 1: Replace old viewer-failover tests with compatibility-aware behavior tests**

1. Owner/observer disconnect leaves remaining attachment and active turn untouched.
2. Last attachment disconnect parks the runtime and starts current orphan grace.
3. Reattach during grace cancels reap and replays missing events.
4. Last disconnect with `close_on_disconnect` preserves existing explicit close semantics.
5. Disconnect during approval/clarification does not strand runtime when another authorized client remains.
6. Dead/overflowed attachment cleanup is idempotent.

**Step 2: Implement zero-subscriber orphan checks.**

Remove `viewers` only after all call sites and tests use hub attachments. If compatibility inspection still requires it, derive it rather than maintaining two authorities.

---

## Task 12: Verify complete event-family fan-out

**Objective:** Prove every requested live event category goes through the session publisher.

**Files:**
- Modify: `tests/test_tui_gateway_server.py`
- Modify/add: focused event contract tests under `tests/tui_gateway/`

**Required event families:**

- assistant start/delta/interim/complete
- thinking/reasoning/status
- tool start/progress/output-risk/complete/generating
- terminal output/close
- approval requested/resolved/expired
- clarification requested/resolved/expired
- subagent start/progress/child message/child tool/complete/error
- runtime errors and cancellation
- session usage/info/completion state

Use parameterized behavioral tests over real `_emit()` paths where practical. Do not create change-detector tests that freeze the entire event-name list.

---

## Task 13: Add real two-WebSocket integration coverage

**Objective:** Exercise authentication, dispatch, fan-out, replay, control enforcement, and disconnect through the actual ASGI WebSocket path.

**Files:**
- Modify: `tests/test_tui_gateway_ws.py`
- Modify/add: dashboard integration tests if required

**Scenario:**

```text
connect A and B
A creates S
B resumes S as observe
A submits synthetic deterministic turn
A and B receive equal ordered events
B mutation is rejected
B disconnects
A receives completion
B reconnects with watermark and receives no duplicate/gap
assert one run_conversation and one persisted turn
```

Use synthetic/fake agent seams already provided by the repository; no network/model calls.

---

## Task 14: Update shared Web/Desktop clients

**Objective:** Make current clients consume attachment metadata and replay safely while preserving older backend compatibility.

**Files:**
- Modify: `apps/shared/src/json-rpc-gateway.ts`
- Modify: shared protocol/type exports
- Modify: `apps/desktop/src/lib/gateway-events.ts` or current event integration only where needed
- Modify: Web gateway client only where it independently defines the protocol
- Add/modify: corresponding TypeScript tests
- Follow: `apps/desktop/AGENTS.md`

**Behavior:**

- Parse optional welcome `client_id` and attachment capabilities.
- Resume defaults to legacy control unless UI explicitly opens an observer/pop-out.
- Observer windows request `attachment_mode="observe"` on resume.
- Reconnect supplies `last_seen_seq`; live events are held while replay applies and deduplicated by watermark using existing logic.
- UI exposes read-only state rather than allowing controls that will be rejected.
- Older backend without `session.attach` continues through existing resume behavior.

No redesign of the Desktop UI is part of this PR.

---

## Task 15: Runtime-host boundary and cross-process behavior

**Objective:** Enforce one canonical mutable runtime across authenticated local gateway processes while retaining safe process-local behavior where authenticated IPC is unavailable.

**Files:**
- Add: canonical runtime-owner registry and Unix proxy transport
- Modify: TUI Gateway resume/dispatch/teardown paths
- Modify: shared reconnect and durable-resync handling
- Modify: owner/proxy protocol and spawned-process tests

**Contract:**

- `SessionDB.session_runtime_key()` resolves every compression/continuation segment to one canonical durable runtime root.
- Registry ownership is keyed by both that root and the normalized Hermes profile/state home, so independent profiles cannot collide.
- A process atomically claims the root before constructing an `AIAgent`; concurrent builders in the winning process additionally single-flight on the profile-scoped root.
- Followers use a same-UID Unix-domain proxy bound to the exact owner ID and generation. Registry files contain routing/liveness metadata only, never credentials.
- A same-UID process does not attest a browser identity. Proxy hello frames therefore reject `auth_identity`; locally verified principals scope stable reconnect routes without being forwarded as trusted owner metadata.
- Negotiated capabilities cross the proxy and are owner-validated against the fixed allowlist. An omitted capability declaration preserves legacy full authority; an explicit empty list remains unprivileged.
- Proxy event writes use independent bounded queues. Responses remain point-to-point, owner-stamped events are not restamped, stale owner generations cannot publish, and timeout/owner loss returns `-32072` with `outcome: unknown` and `retryable: false`.
- Stable clients can replace a physical connection and recover remote routes only under the same locally verified principal. Replay truncation, epoch change, runtime-host change, and owner loss request durable reconstruction.
- On platforms without authenticated Unix peer credentials (`SO_PEERCRED`), normal sessions remain process-local instead of claiming unsupported cross-process guarantees.
- The SQLite turn lease remains defense in depth for persistence fencing; it is not the canonical runtime registry.

**Test:** spawned-process claim races elect one owner; concurrent eager resumes build once; real Unix proxy tests cover response routing, event delivery, capabilities, reconnect, stale generations, timeout, owner loss, and profile isolation.

A distributed broker, cross-machine mutable `AIAgent`, and unauthenticated network proxy are explicitly out of scope. The supported cross-process topology is authenticated local IPC to one canonical runtime owner.

---

## Task 16: Documentation and migration notes

**Objective:** Document the public contract, compatibility behavior, operational topology, and testing instructions to upstream standards.

**Files:**
- Modify/add the authoritative TUI Gateway/API documentation under `website/docs/`
- Modify shared protocol docs if present
- Keep this implementation plan and feasibility audit under `docs/plans/`

Document:

- one runtime host / many attachments model
- observer versus control mode
- explicit RPCs and errors
- replay and overflow behavior
- disconnect/orphan policy
- approval/clarification authority
- canonical local-process owner/proxy topology and profile-scoped ownership
- backward compatibility and authenticated-IPC platform fallback
- no new `.env` settings

Run documentation link/build checks required by repository CI.

---

## Task 17: Final regression, security, and independent review

**Objective:** Produce PR-ready evidence that the full feature works and existing functions remain intact.

**Step 1: Run focused Python suites**

Use the commands in “Baseline and quality gates.” Record exact pass/fail totals.

**Step 2: Run the full Python suite**

```bash
scripts/run_tests.sh -q
```

Compare failures to the clean-main baseline. No new failures allowed.

**Step 3: Run TypeScript tests/typecheck/lint**

Use package scripts discovered from current `package.json`; do not invent commands if names differ.

**Step 4: Static/security review**

Check:

- no credential/token/auth identity in events or attachment snapshots
- exact approval choice allowlist
- no observer mutation bypass
- bounded queues and bounded resolved-request tombstones
- no network write under runtime/history/approval locks
- no unbounded writer thread/process leaks
- no replay duplication or event-order inversion
- no debug prints/generated artifacts

**Step 5: Independent reviews**

Run separate spec-compliance and code-quality/security reviews. Resolve all critical and important findings and repeat review.

**Step 6: Scope verification**

```bash
git diff --check
git diff --name-only origin/main...HEAD
git status --short
```

Every changed path must trace to this feature, its tests, or its documentation.

**Step 7: Prepare commits/PR description**

Use conventional, reviewable commits. Do not push or open the PR until explicitly requested. The PR body must include architecture, compatibility, test totals, known clean-main baseline failure, security decisions, and manual two-client test steps.

---

## Manual acceptance test

After automated gates, run a local host and two clients against the same profile/runtime endpoint.

1. Start one Hermes runtime host.
2. Connect client A in control mode.
3. Create a session and submit a prompt that streams and runs a visible terminal/tool operation.
4. Connect client B to the same session in observe mode during the turn.
5. Confirm B catches up through replay and then receives the same live sequence as A.
6. Attempt submit, steer, interrupt, approval, and clarification responses from B; confirm exact authorization rejection.
7. Upgrade B to control, submit a queued prompt from B, and confirm one ordered turn with origin metadata.
8. Trigger an approval; race authorized responses and confirm one winner.
9. Trigger clarification; race responses and confirm no overwrite.
10. Disconnect A; confirm B continues receiving and controlling the same runtime.
11. Reconnect A with its watermark; confirm no duplicates or gaps.
12. Block/kill B’s socket; confirm A and the agent remain responsive.
13. Inspect persisted history; confirm no duplicated messages.
14. Stop the host and reconnect to a new epoch; confirm durable history reconstruction instead of invalid replay stitching.

## Completion definition

The feature is complete only when:

- all 17 tasks are implemented;
- same-host multi-client observation and writable control work through real WebSockets;
- all requested event families fan out;
- backpressure, replay, authorization, approval, clarification, input, interrupt, and disconnect races have automated tests;
- one runtime/one persistence invariant is proven;
- Python and TypeScript quality gates introduce no regressions;
- documentation and manual test instructions are present;
- independent reviews approve the diff;
- the branch is ready for the user to push and open as an upstream PR.
