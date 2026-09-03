# Multi-Client Branch Scope Audit

Audited branch: `feat/multi-client-live-sessions`

Audit base: `origin/main`

Audit tip before cleanup: `2f2f08b741667fea52e6ddfc34a9301794609eeed`

Plan: `docs/plans/2026-09-01-universal-multi-client-live-sessions.md`

## Classification rules

- `required`: implements or proves a live-session invariant in the universal plan.
- `test-only`: verifies required behavior without changing runtime behavior.
- `documentation`: defines architecture, protocol, or acceptance.
- `mixed`: contains required and unrelated hunks; restore the base version and reapply only required hunks.
- `unrelated`: no direct dependency on canonical runtime attachment, ordered fan-out, identity, replay, recovery, or authorization.
- `base-neutral`: an unrelated historical commit whose resulting patch is already present on `origin/main`; reverting it would introduce a regression and is therefore forbidden.

## Required production paths

### Canonical persistence and runtime ownership

| Path | Class | Requirement |
|---|---|---|
| `agent/turn_context.py` | required | Emit exact persisted inbound user-row metadata only after durability. |
| `hermes_cli/live_runtime_owners.py` | required | Cross-process canonical runtime ownership, fencing, and lookup. |
| `tui_gateway/compute_host.py` | required | Preserve prompt identity and metadata across isolated execution. |
| `tui_gateway/event_replay.py` | required | Bounded ordered replay and epoch semantics. |
| `tui_gateway/methods_prompt.py` | required | Stable prompt identity and queued metadata. |
| `tui_gateway/methods_session.py` | required | Attachment-aware resume/branch/teardown and runtime ownership. |
| `tui_gateway/runtime_proxy.py` | required | Authenticated local canonical-owner transport. |
| `tui_gateway/server.py` | required | Attachment authorization, scheduler, durable user fan-out, lifecycle. |
| `tui_gateway/session_events.py` | required | Ordered event hub and attachment capabilities. |
| `tui_gateway/transport.py` | required | Stable transport/client identity state. |
| `tui_gateway/ws.py` | required | Authenticated WebSocket client attachment boundary. |

### Shared, Desktop, TUI, and web clients

| Path | Class | Requirement |
|---|---|---|
| `apps/shared/src/index.ts` | required | Export shared attachment/recovery API. |
| `apps/shared/src/json-rpc-gateway.ts` | required | Identity negotiation, session attachment, replay, host replacement, durable resync. |
| `apps/desktop/electron/remote-ws-headers.ts` | required | Permit authenticated remote Desktop WebSocket attachment without weakening Origin validation. |
| `apps/desktop/src/api/client.ts` | required | Stable Desktop gateway identity and durable recovery callback. |
| `apps/desktop/src/app/chat/composer/controls.tsx` | required | Correct send/steer/queue action while another client owns a running turn. |
| `apps/desktop/src/app/contrib/hooks/use-background-sync.ts` | required | Prevent authoritative hydration from clobbering live/pending cross-client rows. |
| `apps/desktop/src/app/contrib/wiring.tsx` | required | Bind transcript reconciliation to current runtime/session state. |
| `apps/desktop/src/app/gateway/hooks/use-gateway-boot.ts` | required | Attach Desktop gateway lifecycle and recovery callbacks. |
| `apps/desktop/src/app/session/hooks/use-message-stream/gateway-event/lifecycle.ts` | required | Runtime/session replacement handling. |
| `apps/desktop/src/app/session/hooks/use-message-stream/gateway-event/message-stream.ts` | required | Durable peer user-row rendering and sender reconciliation. |
| `apps/desktop/src/app/session/hooks/use-prompt-actions/submit.ts` | required | Stable submitted-message identity and metadata. |
| `apps/desktop/src/app/session/hooks/use-session-actions/utils.ts` | mixed | Keep pending/live cross-client reconciliation; remove unrelated settled tool-commentary presentation hunk. |
| `apps/desktop/src/lib/chat-messages/types.ts` | required | Carry stable client message identity in renderer rows. |
| `apps/desktop/src/store/gateway.ts` | required | Gateway attachment/recovery state. |
| `apps/desktop/src/store/session-states.ts` | required | Rebind runtime-local state after owner replacement. |
| `ui-tui/src/app/createGatewayEventHandler.ts` | required | TUI owner-loss recovery and peer user-event handling. |
| `ui-tui/src/app/submissionCore.ts` | required | Stable TUI prompt identity and display metadata. |
| `ui-tui/src/gatewayClient.ts` | required | TUI attachment, reconnect, replay watermark, and owner takeover. |
| `ui-tui/src/gatewayTypes.ts` | required | TUI live-session protocol types. |
| `ui-tui/src/types.ts` | required | Stable TUI message identity. |
| `web/src/components/ChatSidebar.tsx` | required | Durable reconstruction after replay truncation. |
| `web/src/lib/gatewayClient.ts` | required | Stable web client identity and negotiated attachment. |

## Required test paths

| Path | Class | Requirement |
|---|---|---|
| `tests/agent/test_turn_context.py` | test-only | Exact persisted inbound-row notification. |
| `tests/hermes_cli/test_live_runtime_owners.py` | test-only | Runtime registry ownership/fencing/liveness. |
| `tests/test_tui_gateway_event_replay.py` | test-only | Replay order, bounds, and truncation. |
| `tests/test_tui_gateway_queue_on_busy.py` | test-only | Busy queue stable-ID behavior and metadata. |
| `tests/test_tui_gateway_server.py` | test-only | Attachment authorization, scheduler, lifecycle, fan-out. |
| `tests/test_tui_gateway_ws.py` | test-only | Authenticated WebSocket boundary; Phase 2 will add full two-client acceptance. |
| `tests/tui_gateway/test_compute_host.py` | test-only | Isolated metadata preservation. |
| `tests/tui_gateway/test_partial_session_failure_teardown.py` | test-only | Full teardown after partial initialization failure. |
| `tests/tui_gateway/test_protocol.py` | test-only | Attachment protocol validation. |
| `tests/tui_gateway/test_runtime_proxy_dispatch.py` | test-only | Remote owner dispatch and authorization. |
| `tests/tui_gateway/test_runtime_proxy_protocol.py` | test-only | Authenticated IPC framing and identity. |
| `tests/tui_gateway/test_session_events.py` | test-only | Event hub and replay behavior. |
| `tests/tui_gateway/test_session_resume_db_ownership.py` | test-only | Runtime-key resume and DB-handle ownership. |
| `tests/state/test_session_turn_lease.py` | mixed | Keep public `session_runtime_key` assertions; restore unrelated whole-file formatting. |
| `apps/shared/src/gateway-client-id.test.ts` | test-only | Stable per-client identity. |
| `apps/shared/src/json-rpc-gateway-replay.test.ts` | test-only | Negotiation, replay, takeover, resync, durable identity. |
| `apps/desktop/electron/remote-ws-headers.test.ts` | test-only | Remote Origin/header safety. |
| `apps/desktop/src/api/client-identity.test.ts` | test-only | Desktop identity survives renderer reload. |
| `apps/desktop/src/app/chat/composer/controls.test.tsx` | test-only | Busy send/steer/queue action. |
| `apps/desktop/src/app/contrib/hooks/use-background-sync.test.ts` | test-only | Hydration preserves/reconciles live rows. |
| `apps/desktop/src/app/gateway/hooks/use-gateway-boot.test.tsx` | test-only | Desktop recovery wiring. |
| `apps/desktop/src/app/session/hooks/use-message-stream/session-reclaimed.test.tsx` | test-only | Runtime replacement state rebind. |
| `apps/desktop/src/app/session/hooks/use-message-stream/user-message-event.test.tsx` | test-only | Peer insertion and sender reconciliation by stable ID. |
| `apps/desktop/src/app/session/hooks/use-session-actions/utils.test.ts` | mixed | Keep live/pending hydration tests; remove unrelated settled tool-commentary test if present. |
| `ui-tui/src/__tests__/createGatewayEventHandler.test.ts` | test-only | TUI durable owner-loss recovery and peer rows. |
| `ui-tui/src/__tests__/gatewayClient.test.ts` | test-only | TUI negotiation, reconnect, replay, takeover. |
| `ui-tui/src/__tests__/submissionCore.test.ts` | test-only | TUI stable-ID submission semantics. |
| `web/src/components/ChatSidebar.test.tsx` | test-only | Web durable resync behavior. |
| `web/src/lib/gatewayClient.test.ts` | test-only | Web attachment negotiation. |

## Required documentation

| Path | Class | Requirement |
|---|---|---|
| `docs/plans/2026-08-28-multi-client-live-sessions-feasibility.md` | documentation | Initial architecture/feasibility record. |
| `docs/plans/2026-08-28-multi-client-live-sessions-implementation.md` | documentation | Initial TUI-Gateway-family implementation plan. |
| `docs/plans/2026-09-01-universal-multi-client-live-sessions.md` | documentation | Ordered universal frontend plan. |
| `docs/plans/2026-09-01-multi-client-scope-audit.md` | documentation | This path-level scope manifest. |

## Mixed paths requiring surgical cleanup

### `hermes_state.py`

Keep only:

- public `session_runtime_key()` over the existing turn-lease root resolver;
- `adopt_orphaned_gateway_session()` needed for canonical owner reclamation;
- `session_gateway_runtime()` and its runtime metadata result;
- exact message-row metadata needed to return/publish the persisted inbound row;
- directly associated schema/query helpers and tests.

Remove:

- repository-wide formatter churn;
- unrelated WAL, corruption repair, FTS, Telegram topic, usage, handoff, and logging formatting changes;
- unrelated behavior unless a required gateway test demonstrates dependency.

### `tests/conftest.py`

The `_reset_auxiliary_runtime_context` fixture is test-suite isolation discovered during broad validation, not live-session behavior. Remove it and all formatter churn. If Phase 2 tests expose a real test leak, add local fixture cleanup in the affected live-session test module instead.

### Desktop transcript reconciliation

Keep pending user-message reconciliation and runtime-generation guards in:

- `apps/desktop/src/app/contrib/hooks/use-background-sync.ts`;
- `apps/desktop/src/app/session/hooks/use-session-actions/utils.ts`;
- corresponding focused tests.

Remove the unrelated tool-commentary presentation rule and its interim-sealing regression unless a multi-client RED test proves it is required.

## Unrelated current diff paths

These must be restored to `origin/main` in a normal cleanup commit:

### Desktop E2E and packaging concerns

- `apps/desktop/e2e/correction-session-switch.spec.ts`
- `apps/desktop/e2e/fixtures.ts`
- `apps/desktop/e2e/glyph-spinner.spec.ts`
- `apps/desktop/e2e/hidden-history-messages.spec.ts`
- `apps/desktop/e2e/image-attachment-resume.spec.ts`
- `apps/desktop/e2e/interim-messages.spec.ts`
- `apps/desktop/e2e/mock-server.ts`
- `apps/desktop/e2e/real-session-builder.ts`
- `apps/desktop/e2e/sidebar-states.spec.ts`
- `apps/desktop/e2e/tile-unread-bug.spec.ts`
- `apps/desktop/e2e/warm-resume-jitter.spec.ts`
- `apps/desktop/e2e/worktree-branch-status.spec.ts`
- `apps/desktop/vite.config.ts`

Reasons: correction-script behavior, credential-fixture handling, spinner compositor behavior, hidden-history scripts, image-resume selectors, sidebar/unread rendering, warm-resume paint metrics, worktree branch status, and renderer chunking are separate features or test stabilization. A future multi-client E2E must be a new focused test rather than rewriting these suites.

### Desktop workspace, test formatting, and timeout concerns

- `apps/desktop/src/app/chat/sidebar/index.tsx` (only the unrelated CWD hunk)
- `apps/desktop/src/app/chat/sidebar/project-cwd-sync.ts`
- `apps/desktop/src/app/chat/sidebar/project-cwd-sync.test.ts`
- `apps/desktop/src/app/messaging/index.test.tsx`
- `apps/desktop/src/app/session/hooks/use-message-stream/interim-sealing.test.tsx`
- `apps/desktop/src/app/settings/gateway-settings.test.tsx`
- `apps/desktop/src/lib/markdown-blocks.test.ts`
- `apps/desktop/src/store/session-unread-tile.test.ts`

### Python and TUI test stabilization

- `tests/agent/test_compression_review_76354.py`
- `ui-tui/src/__tests__/virtualHistoryOffsetCache.test.ts`

Reasons: compression timeout stabilization and virtual-scroll timing have no runtime attachment dependency.

### Dashboard password login

- `hermes_cli/dashboard_auth/login_page.py`
- `tests/hermes_cli/test_dashboard_auth_password_login.py`

Reason: proxy-prefix preservation during password login is unrelated to live sessions. Remove from this PR diff through a normal cleanup commit unless current `origin/main` already contains the exact change; in that base-neutral case, do not introduce a reverse patch.

## Base-neutral historical commit

The branch history includes `fix(dashboard): preserve proxy prefix during password login`. Scope is unrelated, but cleanup must be based on the current triple-dot diff, not commit titles. If all affected hunks are already identical to `origin/main`, the historical commit contributes no PR patch and must not be reverted.

## Phase 1 gate

Before Phase 2:

1. Restore all unrelated current-diff paths/hunks.
2. Reapply only required hunks in mixed files.
3. Run focused Python, TUI, shared, Desktop, and web tests covering retained behavior.
4. Run `git diff --check`.
5. Verify every path in `git diff --name-only origin/main...HEAD` appears in the required/test/documentation tables above.
6. Commit cleanup separately; do not rewrite or force-push published history.

## Cleanup result

Completed in the working tree on 2026-09-01.

- Current diff versus `origin/main`: 63 files, down from 100 audited paths.
- Restored every wholly unrelated path listed above.
- Removed the transcript-interim workaround and its tests from all mixed Desktop files.
- Restored base formatting in existing Python files with a formatter-aware three-way merge, preserving only substantive runtime changes.
- Retained one local model-inventory monkeypatch in `tests/test_tui_gateway_server.py`; without it the test reads host Anthropic OAuth state and is nondeterministic. The broad global `tests/conftest.py` workaround was removed.
- No force push or history rewrite was used.
- Python multi-client scope: 921 passed.
- Shared focused tests: 24 passed.
- TUI focused tests: 139 passed.
- Desktop focused tests: 103 passed.
- Desktop prompt-action regression file: 126 passed.
- Web focused tests: 18 passed; the full web suite passed 279 tests.
- The full workspace run exposed stale Desktop expectations for the new
  stable-ID metadata. Those expectations were repaired and their 126-test file
  rerun successfully.
- Two untouched performance tests timed out only during the parallel full run:
  Desktop markdown fuzz passed 6/6 in isolation and TUI cursor drift passed
  4/4 in isolation. Their timeout changes remain excluded from this PR.
- Changed-path Ruff, Python syntax compilation, Desktop typecheck, and
  `git diff --check` pass.
- Existing `ty` diagnostics are baseline-neutral against both `origin/main`
  and the pre-cleanup branch tip; newly added Python files report none.
- Added-line credential scanning found only the synthetic `"secret-token"`
  authentication test fixture.
