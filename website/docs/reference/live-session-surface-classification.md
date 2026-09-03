---
sidebar_position: 9
title: "Live Session Surface Classification"
description: "Authoritative classification of Hermes frontends, producers, and tool-serving protocols"
---

# Live Session Surface Classification

This page is the authoritative boundary for claims about Hermes live-session support. A protocol being able to submit work, read history, or stream one response does **not** by itself make that protocol an attached live-session client.

## Classification rules

Hermes uses three categories:

- **Attached client** — a persistent presentation frontend that joins one canonical live runtime. It observes the runtime's ordered event stream, submits prompts or busy-turn policy through that runtime, answers pending interactions by canonical request ID, reconnects from an authoritative sequence watermark, and never persists canonical output a second time.
- **Producer / request-response automation** — submits a prompt, event, or scheduled job and receives or delivers its result. It may reuse a gateway conversation key, but it does not remain subscribed as a presentation member and does not receive unrelated peer activity.
- **Tool-serving or control protocol** — contributes tools/resources or exposes operations over another protocol. It is outside the renderer contract unless a separate adapter explicitly implements the attached-client lifecycle above.

The canonical runtime owner is the only writer of canonical event history. Producers and tool/control bridges must not infer ownership from access to durable transcripts.

## Protocol matrix

| Surface | Classification | Current execution/session behavior | Canonical live-session status | Persistence authority |
|---|---|---|---|---|
| Classic interactive CLI | Attached client | Creates or resumes through gateway control, then attaches through the runtime proxy. | Supported. Observes canonical events, steers/interrupts through canonical input, answers interactions, and reconnects from the delivered sequence watermark. | Canonical runtime owner only. The CLI does not re-persist attached output. |
| Default TUI and Desktop native chat | Attached clients | Resolve the same gateway conversation owner and use presentation adapters around its event stream. | Supported where the adapter contract is enabled. The direct TUI replays retained events by epoch/sequence and fails closed rather than advancing when replay is truncated. | Canonical runtime owner only. |
| Dashboard Chat PTY | Hosts the attached TUI | The browser transports a real TUI process over `/api/pty`; the TUI, not the browser shell, owns canonical runtime attachment. | Live-session behavior follows the embedded TUI. The adjacent `/api/events` structured sidebar is best-effort and has no durable replay cursor. | Canonical runtime owner only. The dashboard shell does not persist observed output. |
| Messaging adapters using `GatewayRunner` runtime bridge | Attached presentation adapters | Resolve the gateway conversation owner and consume the runtime-proxy stream. | Supported for adapters wired to the bridge. Runtime-proxy reconnect can validate a completion snapshot and restore pending interactions; this must not be generalized to unrelated feeds or producers. | Canonical runtime owner only. |
| One-shot CLI (`-q` and equivalent batch invocation) | Producer | Starts a bounded run, returns the result, and exits. | Not an attached client. | The bounded run owns its output according to the normal one-shot persistence path. |
| ACP (`hermes acp`) | Persistent frontend with an independent runtime; **not currently attached** | `acp_adapter.SessionManager` creates/restores an `AIAgent` per ACP session and persists ACP history to `state.db`. ACP streaming and approvals belong to that ACP-owned run. | Not part of canonical gateway live-session membership today. Do not describe ACP as observing or steering an already-running gateway session. A future integration must attach through the runtime proxy instead of constructing a second agent for the same conversation. | The ACP session manager/agent for ACP-owned sessions. It must not write into a gateway-owned canonical runtime as a second owner. |
| MCP client integration (`mcp_servers`) | Tool-serving protocol | Hermes connects to external MCP servers and registers their tools, resources, prompts, sampling, and elicitation capabilities. | Outside the renderer contract. MCP servers do not become conversation participants by contributing tools. | The active Hermes runtime persists its own conversation; MCP servers own only their protocol-side state. |
| Hermes MCP server (`hermes mcp serve`) | Tool/control bridge | Exposes conversation listing/history, message send, event polling/waiting, and permission operations as MCP tools. `messages_send` is a producer operation; polling is an observer/control operation. | Not an attached client and not the canonical runtime protocol. Access to history or polling does not confer runtime membership, replay fencing, or persistence ownership. | Existing Hermes session/runtime remains authoritative. The MCP bridge must not re-persist observed output. |
| A2A inbound platform | Producer / request-response automation | Converts each A2A task to a gateway `MessageEvent` keyed by `contextId`; HTTP, SSE, or push notification carries that task's result. The context ID supports later related turns. | Reuses the gateway session path but is not a persistent renderer. It does not receive unrelated peer-user, tool, approval, or assistant events from the conversation. | Gateway runtime/session path owns conversation output; A2A separately stores task/protocol audit state. |
| A2A outbound tools | Tool-serving/client integration | `a2a_call` and related tools invoke remote agents from within the active Hermes run. | Outside the local renderer contract. | The active Hermes runtime owns its conversation; remote A2A peers own their task state. |
| Cron | Producer | Each fire starts an independent scheduled execution (or a script-only run), then delivers its result. Optional delivery mirroring or thread seeding provides context for a later human reply. | Not attached. Mirroring a result into a chat transcript does not turn the scheduler into a session participant. | Cron execution owns its run record; destination gateway sessions remain authoritative for their own canonical histories. |
| Webhooks | Producer / direct-delivery automation | A validated POST either triggers a bounded agent run or renders and delivers a message without an agent (`deliver_only`). | Not attached. A webhook request has no persistent presentation membership or replay cursor. | Triggered run/delivery path owns its result; destination sessions own any canonical conversation history. |

## Decision rule for new integrations

Classify a new integration by behavior, not protocol name:

1. If it must continuously render peer-user messages, assistant output, tools, approvals, and clarifications from an existing conversation, implement it as an **attached client** using the canonical runtime proxy and renderer contract.
2. If it only starts work and receives/delivers that work's result, implement it as a **producer**. Reuse a conversation key when continuity is intended, but do not subscribe it implicitly or make it another persistence owner.
3. If it only contributes callable capabilities or administrative operations, keep it a **tool/control protocol** outside the renderer contract.
4. A hybrid protocol may expose separate surfaces in different categories. MCP client integration, `hermes mcp serve`, A2A inbound tasks, and A2A outbound tools are classified separately for this reason.

## Claiming support

“Universal multi-client live sessions” means all **supported persistent presentation frontends** share one canonical runtime. It does not mean every automation ingress, tool protocol, or batch command is a live renderer.

Documentation and release notes must name exceptions explicitly. In particular:

- ACP remains an independent session runtime until it is migrated to runtime-proxy attachment.
- MCP remains outside the renderer contract, including `hermes mcp serve`.
- A2A, cron, webhooks, and one-shot CLI are producers even when they reuse or seed a durable conversation.
- No adapter may silently start a duplicate agent when canonical owner attachment fails.
