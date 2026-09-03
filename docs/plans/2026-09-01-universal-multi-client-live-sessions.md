# Universal Multi-Client Live Sessions Implementation Plan

> **For Hermes:** Execute this plan in order. Do not begin a later phase until the preceding phase's scope and tests pass.

**Goal:** Let Desktop, interactive CLI/TUI, dashboard/web, Discord, and every interactive `GatewayRunner` platform attach to one authoritative durable Hermes session, submit into one scheduler, and observe one causally ordered live event stream without duplicate execution or persistence.

**Architecture:** Keep one canonical runtime owner per profile-scoped durable session root. Generalize the existing authenticated local runtime-owner proxy into a transport-neutral frontend boundary. TUI Gateway clients and `GatewayRunner` platform sessions become adapters over the same runtime protocol; platform adapters continue to own rendering and delivery. One-shot commands, cron, and webhooks remain producers by default rather than pretending to be persistent observers.

**Tech Stack:** Python 3.11, synchronous TUI JSON-RPC, async `GatewayRunner`, authenticated Unix-domain runtime proxy, FastAPI/WebSockets, SQLite durable sessions, TypeScript shared/Desktop/TUI/web clients, pytest, Vitest, Ruff, ty, ESLint.

**Branch:** `feat/multi-client-live-sessions`

**Publication boundary:** Fork PR `mineblow/hermes-agent#1` only. No upstream branch, PR, comment, merge, or other action without explicit user approval.

## Execution status

- [x] Phase 1, Task 1: branch scope manifest
- [x] Phase 1, Task 2: unrelated-diff and formatter-churn cleanup
- [x] Phase 1, Task 3: scoped regression and security gates
- [x] Phase 1, Task 4: cleanup commit and fork push (`72f11d25f`)
- [x] Phase 2, Task 3: authenticated two-WebSocket acceptance
- [x] Phase 2, Task 4: complete live-event families and races
- [x] Phase 3, Task 5: transport-neutral runtime protocol
- [x] Phase 3, Task 6: async GatewayRunner frontend client
- [x] Phase 4, Task 7: platform-neutral attachment state
- [x] Phase 4, Task 8: shared-scheduler messaging input

---

## Supported-surface contract

### Interactive attached clients

These must be able to join the same live runtime and receive ordered events:

1. Native Desktop.
2. Default interactive CLI/TUI (`ui-tui`).
3. Dashboard/web chat using `/api/ws`.
4. Discord through `GatewayRunner`.
5. Every other interactive `GatewayRunner` adapter through the same generic bridge, including Telegram, Slack, Teams, Matrix, Signal, WhatsApp, Email, API Server/OpenWebUI, A2A, and plugin platforms.
6. Classic interactive CLI when the TUI is explicitly disabled.

### Producers, not persistent observers

These may submit to a named durable session but do not need a live renderer unless explicitly upgraded later:

- one-shot `hermes chat -q`;
- cron;
- webhook triggers;
- fire-and-forget automation.

### Separate protocol review

ACP/MCP server entry points must be inventoried. If they already expose a persistent interactive conversation, they must use the shared bridge; otherwise document them as producers/tool protocols, not live clients.

---

## Non-negotiable invariants

1. One durable session root has one authoritative mutable runtime.
2. One accepted `client_message_id` executes and persists at most once.
3. Identical text with different IDs remains separate turns.
4. Draft typing never leaves the originating client.
5. A submitted durable user row is visible to every attached client before assistant/tool output for that turn.
6. Every attached client observes the same per-session event order.
7. Messaging delivery retries cannot execute the agent twice.
8. Platform adapters render events but never own conversation execution.
9. Observer authority cannot submit, steer, interrupt, approve, clarify, close, undo, branch, or invoke privileged UI responses.
10. Existing platform authorization remains fail closed; joining a Discord channel or knowing a session ID does not grant controller authority.
11. Disconnecting one client cannot stop or steal another client's runtime.
12. Replay truncation, epoch change, and runtime-owner replacement require durable reconstruction.
13. Unsupported authenticated IPC falls back to safe process-local execution without claiming universal live attachment.
14. Secrets, auth tokens, raw platform credentials, and transport objects never enter events, snapshots, registry files, or Git.

---

## Phase 1: Repair and freeze branch scope

### Task 1: Produce a commit-to-requirement scope manifest

Status: complete.

**Objective:** Account for every path currently changed from `origin/main` before adding new behavior.

**Files:**
- Create: `docs/plans/2026-09-01-multi-client-scope-audit.md`

**Steps:**
1. Record every commit and changed path in `origin/main...HEAD`.
2. Map each path to a numbered invariant or later task in this plan.
3. Mark paths as `required`, `test-only`, `documentation`, or `unrelated`.
4. Treat `fix(dashboard): preserve proxy prefix during password login` as unrelated unless a direct runtime-attachment dependency is proven.
5. Inspect the large Desktop portion of `4367fe4c9` line by line; do not assume formatting or pre-existing bug fixes are required.
6. Run `git diff --check` and verify the worktree contains only the audit document.

### Task 2: Remove unrelated PR changes without rewriting history

Status: complete in the working tree; commit pending the Task 3 gate.

**Objective:** Make the PR diff multi-client-only while preserving published history and user accountability.

**Files:**
- Restore each unrelated path or hunk from `origin/main` using a normal cleanup commit.
- Do not force-push or rewrite published commits without explicit user approval.

**TDD/verification:**
1. Run the affected baseline tests before restoration.
2. Restore only audited unrelated hunks.
3. Run the same tests after restoration.
4. Confirm `git diff --name-only origin/main...HEAD` maps entirely to this plan.
5. Commit the cleanup separately from new feature work.

**Gate:** No Phase 2 work until the scope manifest has zero unexplained production paths.

---

## Phase 2: Complete the existing TUI-Gateway family

### Task 3: Add real two-WebSocket acceptance coverage

Status: complete.

**Objective:** Prove Desktop/TUI/web semantics through the actual authenticated ASGI WebSocket boundary.

**Files:**
- Modify: `tests/test_tui_gateway_ws.py`
- Modify production code only if the RED test exposes a defect.

**RED scenario:**
1. Connect controllers A and C plus observer B.
2. A creates/resumes a durable session; B and C attach.
3. A submits a stable-ID prompt through the real WebSocket dispatcher.
4. A, B, and C receive the durable `message.user` before assistant events.
5. B's mutations are rejected.
6. Retrying A's message ID executes once; a new ID with identical text executes separately.
7. B disconnects/reconnects with a watermark and receives no gap or duplicate.
8. One `run_conversation()` and one durable row exist per accepted ID.

**Gate:** Focused WebSocket tests pass under `scripts/run_tests.sh`.

**Result:** `tests/test_tui_gateway_ws.py` now drives three authenticated
clients through the real `/api/ws` ASGI route. It proves controller and
observer attachment, fail-closed observer mutation, ordered durable
`message.user` fan-out, reconnect replay without gaps or duplicates, same-ID
exactly-once retry, and same-text/different-ID execution. The RED scenario
exposed idle retries executing twice; prompt admission now keeps a bounded
accepted-ID ledger and reconstructs it from durable user-row metadata.
`scripts/run_tests.sh tests/test_tui_gateway_ws.py` passes 10/10.

### Task 4: Cover complete live-event families and races

Status: complete.

**Objective:** Prove the common publisher covers assistant, thinking, tools, terminal, approval, clarification, subagent, error, cancellation, usage, and completion events.

**Files:**
- Modify: `tests/test_tui_gateway_server.py`
- Modify/add: tests under `tests/tui_gateway/`

**Steps:**
1. Add parameterized event-family fan-out tests over real emit paths.
2. Add barrier-driven controller races for approval and clarification.
3. Add slow-subscriber and disconnect-during-publish tests without wall-clock sleeps.
4. Add unsupported authenticated-IPC fallback coverage.
5. Change production only after each test fails for the intended missing behavior.

**Gate:** Existing TUI Gateway final suites pass with no new failures.

**Result:** Parameterized coverage now proves that assistant, reasoning,
thinking, tool, terminal, approval, clarification, subagent, error, usage, and
completion families traverse one sequenced multi-client publisher. Explicit
barrier tests prove observers receive approval/clarification requests but
cannot resolve them, while controllers release the barrier. The deterministic
slow-subscriber test now also proves detach, healthy-peer continuity, and
successful reattachment. Existing compute-host and authenticated runtime-proxy
tests cover unavailable-IPC inline fallback and unsupported capability
rejection. The canonical five-file gate passes 817/817 under
`scripts/run_tests.sh`.

---

## Phase 3: Define a transport-neutral runtime frontend protocol

### Task 5: Separate runtime protocol primitives from TUI presentation

Status: complete.

**Objective:** Let non-TUI frontends use the canonical owner without importing TUI rendering or request handlers.

**Files:**
- Create: `hermes_cli/live_runtime_protocol.py`
- Create: `tests/hermes_cli/test_live_runtime_protocol.py`
- Modify: `tui_gateway/runtime_proxy.py`
- Modify: `tests/tui_gateway/test_runtime_proxy_protocol.py`

**Protocol:**
- frontend hello: opaque stable client ID, verified principal descriptor, surface, requested capabilities, durable root, replay watermark;
- input: stable message ID, display metadata, attachments, busy policy;
- output: ordered runtime event envelope, runtime identity, replay epoch, durable session ID;
- control responses remain point-to-point;
- runtime protocol contains no Discord/Desktop/TUI rendering types.

**TDD:** Move one behavior at a time behind tests while keeping the existing TUI proxy byte-compatible where required.

**Result:** `hermes_cli/live_runtime_protocol.py` now owns dependency-free
frontend hello, stable input, ordered runtime event, point-to-point control
response, replay watermark, principal, identity, capability, attachment, and
busy-policy validation. The neutral capability vocabulary uses
`interaction.respond`; the existing proxy's `ui.respond` survives only in an
explicit legacy compatibility set. `tui_gateway/runtime_proxy.py` delegates
shared version/client/capability validation while retaining its existing hello
wire shape. The canonical two-file gate passes 46/46.

### Task 6: Add an async frontend client for `GatewayRunner`

Status: complete.

**Objective:** Provide a bounded, reconnecting async client for the canonical runtime owner.

**Files:**
- Create: `gateway/live_runtime_client.py`
- Create: `tests/gateway/test_live_runtime_client.py`
- Modify: `hermes_cli/live_runtime_owners.py` only if generic lookup metadata is missing.

**RED coverage:**
- connect/hello and capability intersection;
- request/response correlation;
- ordered event callback;
- bounded outbound/inbound queues;
- owner-generation fencing;
- reconnect with replay watermark;
- replay truncation durable-resync signal;
- cancellation and clean shutdown;
- no retries after an unknown execution outcome.

**Gate:** Existing TUI proxy tests and new async-client tests both pass.

**Result:** `gateway/live_runtime_client.py` provides an injected-transport
async client with bounded inbound/outbound queues, request correlation,
ordered callback delivery, generation fencing, replay-watermark reconnect,
truncation resync signaling, caller cancellation, and clean shutdown. Sent
requests fail with `UnknownExecutionOutcome` after transport loss and are never
retried. Superseded connections close before reconnect and stale inbound
frames are discarded in favor of replay from the last callback-delivered
watermark. The canonical client/proxy gate passes 40/40.

---

## Phase 4: Bridge `GatewayRunner` into the canonical runtime

### Task 7: Add platform-neutral attachment state

Status: complete.

**Objective:** Track one gateway frontend attachment per authorized routing key without creating another `AIAgent`.

**Files:**
- Create: `gateway/live_runtime_bridge.py`
- Create: `tests/gateway/test_live_runtime_bridge.py`
- Modify: `gateway/session_state.py`
- Modify: `gateway/run.py`

**State:**
- durable session ID/root;
- opaque stable gateway client ID;
- current owner ID/generation/runtime session ID;
- accepted capabilities;
- last event sequence/replay epoch;
- platform `SessionSource` used only for delivery.

**TDD:** Prove attachment reuse, profile isolation, controller/observer modes, disconnect cleanup, and no duplicate local agent creation.

**Result:** `gateway/live_runtime_bridge.py` now owns one async frontend
attachment per authenticated profile/platform/scope/chat/thread/principal key.
Resolved profile home scopes both the byte-compatible canonical-owner key and
the opaque stable gateway client ID. Attachment state has an independent
`SessionState` lifecycle, preserves `SessionSource` only for delivery, narrows
observer capabilities to `observe`, replaces clients on durable-root/mode
changes, and uses compare-and-swap detach protection. `GatewayRunner` creates
the bridge lazily without constructing an `AIAgent` and closes it after agent
drain but before adapter teardown. The reviewed five-file gate passes 64/64.

### Task 8: Route messaging input through the shared scheduler

Status: complete.

**Objective:** Make an authorized `MessageEvent` submit into the canonical runtime rather than call a separate `GatewayRunner` agent.

**Files:**
- Modify: `gateway/run.py` around final session resolution and `_handle_message_with_agent`.
- Modify: `gateway/session.py` for stable runtime-root mapping if needed.
- Modify: `gateway/turn_context.py` only for metadata propagation.
- Add: focused gateway input tests.

**RED coverage:**
- Discord/CLI/Desktop simultaneous input produces one scheduler order;
- platform redelivery with the same native message ID maps to the same `client_message_id` and executes once;
- identical text from distinct native messages executes twice;
- attachments and display text survive the bridge;
- observer input is rejected before scheduler mutation;
- fallback remains process-local when authenticated IPC is unsupported.

**Result:** Authorized generic-gateway input now selects the authenticated,
profile-scoped canonical owner after transcript resolution and before local
agent construction. It preserves gateway preprocessing, renderer-facing text,
event-level author identity, durable attachment references, image-only native
model attachments, timestamps, and stable native/update identities. Owner
absence may use the legacy process-local path only before canonical submission;
submission timeout is bounded, closes with a separate deadline, reports an
unknown outcome, and never replays locally. Direct, busy-queue, and isolated
compute-host paths preserve canonical metadata and images. The focused protocol,
bridge, client, runner, proxy, and TUI gate passes 78/78, and a real authenticated
Unix-socket acceptance test proves `MessageEvent -> bridge -> async client ->
runtime proxy -> prompt.submit` exactly once.

### Task 9: Translate runtime output into `GatewayStreamConsumer`

**Objective:** Deliver one runtime event stream through existing platform rendering without modifying every adapter.

**Files:**
- Modify: `gateway/stream_events.py`
- Modify: `gateway/stream_consumer.py`
- Modify: `gateway/live_runtime_bridge.py`
- Add: `tests/gateway/test_live_runtime_event_translation.py`

**Mapping:**
- `message.delta/complete` -> `MessageChunk/MessageStop`;
- interim/commentary -> `Commentary`;
- tool start/complete -> existing tool events;
- user messages -> peer user-message delivery event;
- approval/clarification -> existing platform control UI paths;
- unsupported presentation events may be safely collapsed, never persisted twice.

**Gate:** Existing Telegram/Discord/Slack stream-consumer suites remain green without platform-specific execution forks.

---

## Phase 5: Prove Discord and all messaging adapters

### Task 10: Add Discord live-session end-to-end coverage

**Objective:** Demonstrate Desktop/TUI plus Discord controlling and observing the same runtime.

**Files:**
- Add: `tests/e2e/test_multi_client_discord_runtime.py`
- Modify: Discord tests only where native message identity or delivery assertions are needed.

**Scenario:**
1. Desktop/TUI creates the session.
2. Authorized Discord user resumes/attaches it.
3. Desktop prompt appears in Discord before assistant output.
4. Discord prompt appears in Desktop/TUI before assistant output.
5. Same Discord delivery ID is idempotent.
6. Disconnect/reconnect replays without duplicates.
7. Unauthorized Discord user cannot attach/control.
8. Approval and clarification have one winner across surfaces.

### Task 11: Add a reusable platform-adapter contract suite

**Objective:** Prove all `GatewayRunner` adapters inherit the bridge without one implementation per platform.

**Files:**
- Create: `tests/gateway/test_platform_live_runtime_contract.py`
- Modify: `gateway/platforms/base.py` only if a minimal delivery hook is missing.

**Contract:** Every adapter can receive assistant/user completion through the common consumer, retains its native routing/thread metadata, and never directly starts a second runtime when attached.

**Representative adapters:** Discord, Telegram, Slack, API Server/OpenWebUI, Email, and one minimal plugin adapter. Registry-level tests prove remaining adapters share the same runner path.

---

## Phase 6: Complete CLI and protocol surfaces

### Task 12: Prove default TUI and classic interactive CLI

**Objective:** Make both interactive CLI modes equivalent attached clients.

**Files:**
- Modify: `hermes_cli/main.py` and `cli.py` only where classic mode needs the bridge.
- Add: CLI lifecycle tests under `tests/hermes_cli/`.
- Keep one-shot `-q` producer semantics explicit.

**Coverage:** attach/resume, prompt identity, live peer rows, replay, interrupt, approval/clarification, reconnect, and clean exit.

**Result:** Classic interactive mode now creates or resumes through gateway ownership,
then attaches as a presentation-only client over the canonical runtime proxy. Prompt
identity, attachments, live peer rows, pending interactions, strict interaction
response deadlines, and canonical interrupt replacement are preserved without local
agent execution or duplicate persistence. Fresh attachment starts at the owner's
latest sequence while reconnect resumes from the callback-delivered epoch/sequence
watermark; truncated replay is fenced by the validated authoritative completion
snapshot. Default TUI responses preserve canonical interaction request identity and
use the same response deadline. One-shot `-q` remains a producer rather than a
persistent attached frontend. Lifecycle, proxy, replay, simultaneous
TUI/classic/Discord, full TUI, typecheck, lint, and build gates pass.

### Task 13: Inventory ACP, MCP, A2A, cron, and webhooks

**Objective:** Prevent inaccurate universal-support claims.

**Files:**
- Create/modify authoritative documentation under `website/docs/`.
- Add protocol tests only for surfaces classified as persistent interactive clients.

**Decision rule:** Persistent conversational frontends attach; request/response automation submits as a producer; tool-serving protocols remain outside the conversation-renderer contract.

---

## Phase 7: Documentation, acceptance, and publication

### Task 14: Publish authoritative user/developer documentation

**Files:**
- Add: `website/docs/user-guide/features/multi-client-live-sessions.md`
- Modify: relevant CLI, gateway, messaging, and session documentation indexes.

**Document:** supported surfaces, attachment modes, identity/idempotency, authorization, replay/recovery, runtime topology, fallback behavior, platform delivery differences, and exact manual test steps.

### Task 15: Run final automated gates

**Python:**
- focused TUI Gateway suites through `scripts/run_tests.sh`;
- all gateway/platform bridge suites;
- full `scripts/run_tests.sh -q`;
- Ruff, ty, and Windows-footgun checks.

**TypeScript:**
- shared, TUI, Desktop, and web canonical typecheck/lint/test/build commands;
- Desktop E2E where available.

**Static/security:**
- no observer bypass;
- no secret/auth leakage;
- no duplicate execution/persistence;
- bounded queues/workers/tombstones;
- no network writes under runtime locks;
- no debug/generated artifacts;
- full triple-dot scope audit.

### Task 16: Run manual cross-surface acceptance

**Required clients:**
1. Desktop PC A.
2. Desktop or TUI PC B.
3. Discord.
4. Dashboard/web.

**Required behavior:** local drafts, immediate durable peer messages, common ordering, simultaneous input, observer rejection, approval/clarification race, disconnect/reconnect, owner replacement, replay truncation recovery, and exactly-once persisted history.

### Task 17: Final review and fork PR readiness

1. Repeat specification, code-quality, and security review after all fixes.
2. Resolve all critical and important findings.
3. Verify fork PR #1 contains only this feature and documented baseline exceptions.
4. Keep unavailable high-core fork jobs classified as unavailable infrastructure; do not weaken workflows.
5. Mark the fork PR ready only after user approval.
6. Take no upstream action without separate explicit approval.

---

## Completion definition

The work is complete only when Desktop, interactive CLI/TUI, dashboard/web, Discord, and generic `GatewayRunner` messaging clients can join one durable live runtime with one scheduler, one persistence path, stable message identity, ordered fan-out, secure attachment authority, replay recovery, and demonstrated cross-surface acceptance. Producer-only and tool-protocol surfaces must be explicitly documented so “multi-client” never overclaims support.
