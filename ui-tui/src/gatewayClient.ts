import { type ChildProcess, spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { EventEmitter } from 'node:events'
import { existsSync } from 'node:fs'
import { delimiter, resolve } from 'node:path'
import { createInterface } from 'node:readline'

import { WebSocket as UndiciWebSocket } from 'undici'

import type { GatewayEvent } from './gatewayTypes.js'
import { CircularBuffer } from './lib/circularBuffer.js'
import { recordParentLifecycle } from './lib/parentLog.js'

const MAX_GATEWAY_LOG_LINES = 200
const MAX_LOG_LINE_BYTES = 4096
const MAX_BUFFERED_EVENTS = 2000
const MAX_LOG_PREVIEW = 240
const STARTUP_TIMEOUT_MS = Math.max(5000, parseInt(process.env.HERMES_TUI_STARTUP_TIMEOUT_MS ?? '15000', 10) || 15000)
const REQUEST_TIMEOUT_MS = Math.max(30000, parseInt(process.env.HERMES_TUI_RPC_TIMEOUT_MS ?? '120000', 10) || 120000)
export const INTERACTION_REQUEST_TIMEOUT_MS = 30_000
export const requestTimeoutMs = (method: string) =>
  method === 'approval.respond' || method === 'clarify.respond' ? INTERACTION_REQUEST_TIMEOUT_MS : REQUEST_TIMEOUT_MS
const WS_CONNECTING = 0
const WS_OPEN = 1
const WS_CLOSING = 2
const WS_CLOSED = 3

// Keepalive + dead-connection detection. A silent drop (macOS sleep, proxy
// idle timeout, VPN reconnect) kills the TCP socket without a `close` event,
// so the client hangs forever (issue #32997). Browser/undici WebSocket does
// not expose an acknowledged ping/pong API, so this uses a small JSON-RPC
// heartbeat that the TUI gateway explicitly answers. Healthy idle sockets stay
// open; only a missing heartbeat ack forces close -> reconnect.
export const WS_HEARTBEAT_INTERVAL_MS = 15_000
export const WS_HEARTBEAT_DEAD_MS = 45_000
// Exponential backoff for reconnect attempts after a transport drop.
export const RECONNECT_BASE_MS = 1_000
export const RECONNECT_MAX_MS = 30_000

const getWebSocketCtor = (): typeof WebSocket =>
  typeof WebSocket === 'undefined' ? (UndiciWebSocket as unknown as typeof WebSocket) : WebSocket

const truncateLine = (line: string) =>
  line.length > MAX_LOG_LINE_BYTES ? `${line.slice(0, MAX_LOG_LINE_BYTES)}… [truncated ${line.length} bytes]` : line

const describeChild = (proc: ChildProcess | null) => {
  if (!proc) {
    return 'pid=none'
  }

  return `pid=${proc.pid ?? 'unknown'} killed=${proc.killed} exitCode=${proc.exitCode ?? 'null'} signal=${proc.signalCode ?? 'null'}`
}

const resolveGatewayAttachUrl = () => {
  const raw = process.env.HERMES_TUI_GATEWAY_URL?.trim()

  return raw ? raw : null
}

const resolveSidecarUrl = () => {
  const raw = process.env.HERMES_TUI_SIDECAR_URL?.trim()

  return raw ? raw : null
}

const resolvePython = (root: string) => {
  const configured = process.env.HERMES_PYTHON?.trim() || process.env.PYTHON?.trim()

  if (configured) {
    return configured
  }

  const venv = process.env.VIRTUAL_ENV?.trim()

  const hit = [
    venv && resolve(venv, 'bin/python'),
    venv && resolve(venv, 'Scripts/python.exe'),
    resolve(root, '.venv/bin/python'),
    resolve(root, '.venv/bin/python3'),
    resolve(root, 'venv/bin/python'),
    resolve(root, 'venv/bin/python3')
  ].find(p => p && existsSync(p))

  return hit || (process.platform === 'win32' ? 'python' : 'python3')
}

const asGatewayEvent = (value: unknown): GatewayEvent | null =>
  value && typeof value === 'object' && !Array.isArray(value) && typeof (value as { type?: unknown }).type === 'string'
    ? (value as GatewayEvent)
    : null

// Hoisted decoder: attach mode can drive high-frequency binary frames
// (tool deltas, reasoning streams) and constructing a fresh TextDecoder
// per message creates avoidable GC pressure. One module-level instance
// is fine because UTF-8 is stateless and we always pass entire frames.
const _wireDecoder = new TextDecoder()

const asWireText = (raw: unknown): string | null => {
  if (typeof raw === 'string') {
    return raw
  }

  if (raw instanceof ArrayBuffer || ArrayBuffer.isView(raw)) {
    return _wireDecoder.decode(raw as any as ArrayBuffer)
  }

  return null
}

// Matches `<scheme>://user:pass@host…` style user-info segments in
// otherwise-malformed URLs that the WHATWG `URL` parser can't accept.
// Used by the `redactUrl` fallback so embedded credentials are
// scrubbed from log lines even when the URL is unparseable.
const _USERINFO_FALLBACK_RE = /^([a-z][a-z0-9+.-]*:\/\/)[^/?#@]*@/i

// Connection URLs (gateway, sidecar) often carry bearer tokens in the query
// string. We surface them in user-facing log lines and the
// `gateway.start_timeout` payload, so always strip the query string and any
// embedded user-info before logging.
const redactUrl = (raw: string): string => {
  if (!raw) {
    return raw
  }

  try {
    const url = new URL(raw)
    const userInfo = url.username || url.password ? '***@' : ''
    const query = url.search ? '?***' : ''

    return `${url.protocol}//${userInfo}${url.host}${url.pathname}${query}`
  } catch {
    // WHATWG URL rejected the input. Best-effort: strip an embedded
    // `user:pass@` segment AND the query string so a malformed token
    // bearer can never escape into the log tail.
    const noUserInfo = raw.replace(_USERINFO_FALLBACK_RE, '$1***@')
    const queryIdx = noUserInfo.indexOf('?')

    return queryIdx >= 0 ? `${noUserInfo.slice(0, queryIdx)}?***` : noUserInfo
  }
}

interface Pending {
  id: string
  method: string
  params: Record<string, unknown>
  reject: (e: Error) => void
  resolve: (v: unknown) => void
  trackSessionResult: boolean
  timeout: ReturnType<typeof setTimeout>
}

export class GatewayClient extends EventEmitter {
  private proc: ChildProcess | null = null
  private ws: WebSocket | null = null
  private wsConnectPromise: Promise<void> | null = null
  private sidecarWs: WebSocket | null = null
  private attachUrl: null | string = null
  private sidecarUrl: null | string = null
  private reqId = 0
  private logs = new CircularBuffer<string>(MAX_GATEWAY_LOG_LINES)
  private pending = new Map<string, Pending>()
  private durableSessionIds = new Map<string, string>()
  private sessionWatermarks = new Map<string, { epoch: string; seq: number }>()
  private pendingSessionAttach = new Set<string>()
  private queuedSessionEvents = new Map<string, GatewayEvent[]>()
  private runtimeHostId: string | null = null
  private bufferedEvents = new CircularBuffer<GatewayEvent>(MAX_BUFFERED_EVENTS)
  private pendingExit: number | null | undefined
  private ready = false
  private readyTimer: ReturnType<typeof setTimeout> | null = null
  private subscribed = false
  private drainGeneration = 0
  private stdoutRl: ReturnType<typeof createInterface> | null = null
  private stderrRl: ReturnType<typeof createInterface> | null = null
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectAttempts = 0
  private lastActivityAt = 0
  private heartbeatSeq = 0
  private heartbeatPendingId: string | null = null
  private heartbeatSentAt = 0
  private readonly clientId = `tui:${randomUUID()}`
  // Set on kill() so we never auto-reconnect after an intentional shutdown.
  private disposed = false

  constructor() {
    super()
    // useInput / createGatewayEventHandler can legitimately attach many
    // listeners. Default 10-cap triggers spurious warnings.
    this.setMaxListeners(0)
  }

  private publish(ev: GatewayEvent) {
    if (ev.type === 'gateway.ready') {
      this.ready = true

      if (this.readyTimer) {
        clearTimeout(this.readyTimer)
        this.readyTimer = null
      }

      if (ev.payload?.heartbeat && this.ws?.readyState === WS_OPEN) {
        this.startHeartbeat(this.ws)
      }
    }

    if (this.subscribed) {
      return void this.emit('event', ev)
    }

    this.bufferedEvents.push(ev)
  }

  private clearReadyTimer() {
    if (this.readyTimer) {
      clearTimeout(this.readyTimer)
      this.readyTimer = null
    }
  }

  private closeSidecarSocket() {
    try {
      this.sidecarWs?.close()
    } catch {
      // best effort
    } finally {
      this.sidecarWs = null
    }
  }

  private closeGatewaySocket() {
    // Null the active reference BEFORE invoking close(): real WebSocket
    // implementations dispatch the 'close' event after a microtask hop,
    // so by the time the handler runs `this.ws` should already be null
    // and the identity guard will correctly classify the close as
    // belonging to a discarded socket. (Test fakes emit synchronously,
    // so doing the swap up front is also what makes the identity guard
    // match real timing in tests.)
    const ws = this.ws
    this.ws = null
    this.wsConnectPromise = null

    try {
      ws?.close()
    } catch {
      // best effort
    }
  }

  private startHeartbeat(ws: WebSocket) {
    this.stopHeartbeat()
    this.lastActivityAt = Date.now()
    this.heartbeatPendingId = null
    this.heartbeatSentAt = 0
    this.heartbeatTimer = setInterval(() => {
      if (this.ws !== ws || ws.readyState !== WS_OPEN) {
        return
      }

      const now = Date.now()

      if (this.heartbeatPendingId && now - this.heartbeatSentAt > WS_HEARTBEAT_DEAD_MS) {
        this.lifecycle('[lifecycle] websocket silent drop detected (heartbeat ack timeout); forcing reconnect')
        this.stopHeartbeat()

        try {
          ws.close()
        } catch {
          // ignore
        }

        return
      }

      if (this.heartbeatPendingId) {
        return
      }

      const id = `h${++this.heartbeatSeq}`

      this.heartbeatPendingId = id
      this.heartbeatSentAt = now

      try {
        ws.send(
          JSON.stringify({
            id,
            jsonrpc: '2.0',
            method: 'gateway.ping',
            params: { last_activity_ms: this.lastActivityAt }
          })
        )
      } catch {
        this.lifecycle('[lifecycle] websocket heartbeat send failed; forcing reconnect')
        this.stopHeartbeat()

        try {
          ws.close()
        } catch {
          // ignore
        }
      }
    }, WS_HEARTBEAT_INTERVAL_MS)
    this.heartbeatTimer.unref?.()
  }

  private stopHeartbeat() {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }

    this.heartbeatPendingId = null
    this.heartbeatSentAt = 0
  }

  private scheduleReconnect() {
    if (this.disposed || this.reconnectTimer !== null) {
      return
    }

    const delay = Math.min(RECONNECT_BASE_MS * 2 ** this.reconnectAttempts, RECONNECT_MAX_MS)
    this.reconnectAttempts += 1
    this.lifecycle(`[lifecycle] scheduling gateway reconnect in ${delay}ms (attempt ${this.reconnectAttempts})`)
    this.publish({ type: 'gateway.reconnecting', payload: { attempt: this.reconnectAttempts, delay_ms: delay } })
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null

      if (this.disposed) {
        return
      }

      this.start()
    }, delay)
    this.reconnectTimer.unref?.()
  }

  private clearReconnect() {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }

    this.reconnectAttempts = 0
  }

  private resetStartupState() {
    // Reject any in-flight RPCs left over from the previous transport
    // before we swap. Otherwise the old transport's stale exit/close
    // handlers (now identity-gated to ignore unrelated transports)
    // never fire `rejectPending`, leaving callers hanging on promises
    // attached to a discarded child / socket.
    this.rejectPending(new Error('gateway restarting'))
    this.ready = false
    // `drain()` establishes the EventEmitter subscription for the lifetime of
    // this client, not one transport. Preserve it across websocket reconnects
    // so replay + restored live delivery do not remain buffered forever.
    // Invalidate any pending deferred drain() flush from a prior transport so
    // its queued microtask becomes a no-op (it captured the old generation).
    this.drainGeneration += 1
    this.bufferedEvents.clear()
    this.pendingExit = undefined
    this.stdoutRl?.close()
    this.stderrRl?.close()
    this.stdoutRl = null
    this.stderrRl = null
    this.clearReadyTimer()
  }

  private startReadyTimer(python: string, cwd: string) {
    this.readyTimer = setTimeout(() => {
      if (this.ready) {
        return
      }

      // Append the most recent gateway stderr/log lines to the timeout
      // event so users can tell apart "wrong python", "missing dep",
      // and "config parse failure" from one glance instead of having
      // to dig through `/logs`.  Capped to keep the activity feed
      // readable on slow boots.
      const stderrTail = this.getLogTail(20)

      this.lifecycle(`[startup] timed out waiting for gateway.ready (python=${python}, cwd=${cwd})`)
      this.publish({
        type: 'gateway.start_timeout',
        payload: { cwd, python, stderr_tail: stderrTail }
      })
    }, STARTUP_TIMEOUT_MS)
  }

  private handleTransportExit(code: null | number, reason?: string) {
    this.clearReadyTimer()
    this.closeSidecarSocket()
    this.lifecycle(`[lifecycle] transport exit code=${code ?? 'null'} reason=${reason ?? 'none'}`)
    this.rejectPending(new Error(reason || `gateway exited${code === null ? '' : ` (${code})`}`))

    // Self-heal: a dropped transport (real close OR silent drop caught by the
    // heartbeat) should reconnect instead of stranding the UI on a dead socket
    // (issue #32997). Intentional shutdown sets `disposed` and skips this.
    // Schedule before the synchronous 'exit' emission: useMainApp's existing
    // recovery subscriber may call start() immediately, and start() cancels this
    // timer so there is only one recovery owner.
    this.scheduleReconnect()

    // A websocket transport drop does not destroy same-host live sessions.
    // Keep the UI's active sid intact while this client reconnects and
    // session.attach restores the server-side subscription. Emitting `exit`
    // here would make useMainApp clear sid and independently session.resume the
    // same durable conversation.
    if (this.attachUrl && this.durableSessionIds.size > 0) {
      return
    }

    if (this.subscribed) {
      this.emit('exit', code)
    } else {
      this.pendingExit = code
    }
  }

  private connectSidecarMirror() {
    this.closeSidecarSocket()

    if (!this.sidecarUrl) {
      return
    }

    const WebSocketCtor = getWebSocketCtor()

    if (typeof WebSocketCtor === 'undefined') {
      this.pushLog(`[sidecar] WebSocket unavailable; skipping mirror to ${redactUrl(this.sidecarUrl)}`)

      return
    }

    try {
      const ws = new WebSocketCtor(this.sidecarUrl)

      this.sidecarWs = ws
      ws.addEventListener('close', () => {
        if (this.sidecarWs === ws) {
          this.sidecarWs = null
        }
      })
      ws.addEventListener('error', () => {
        this.pushLog('[sidecar] mirror connection error')
      })
    } catch (err) {
      this.pushLog(`[sidecar] failed to connect ${redactUrl(this.sidecarUrl)} (constructor error)`)
      this.sidecarWs = null
    }
  }

  private mirrorEventToSidecar(rawFrame: string) {
    const ws = this.sidecarWs

    if (!ws || ws.readyState !== WS_OPEN) {
      return
    }

    try {
      ws.send(rawFrame)
    } catch {
      // best effort
    }
  }

  publishLocalEvent(ev: GatewayEvent) {
    const frame = JSON.stringify({ jsonrpc: '2.0', method: 'event', params: ev })

    this.mirrorEventToSidecar(frame)
    this.publish(ev)
  }

  private handleWebSocketFrame(raw: unknown) {
    this.lastActivityAt = Date.now()
    const text = asWireText(raw)

    if (!text) {
      return
    }

    try {
      const frame = JSON.parse(text) as Record<string, unknown>

      if (frame.method === 'event') {
        this.mirrorEventToSidecar(text)
      }

      this.dispatch(frame)
    } catch {
      const preview = text.trim().slice(0, MAX_LOG_PREVIEW) || '(empty frame)'

      this.pushLog(`[protocol] malformed websocket frame: ${preview}`)
      this.publish({ type: 'gateway.protocol_error', payload: { preview } })
    }
  }

  private startSpawnedGateway(root: string) {
    const python = resolvePython(root)
    const cwd = process.env.HERMES_CWD || root
    const env = { ...process.env }
    const pyPath = env.PYTHONPATH?.trim()

    env.PYTHONPATH = pyPath ? `${root}${delimiter}${pyPath}` : root
    // Tell the gateway child where the Hermes source root is so its import
    // guard can force it ahead of any same-named package in the launch cwd.
    env.HERMES_PYTHON_SRC_ROOT = root
    this.startReadyTimer(python, cwd)
    this.proc = spawn(python, ['-m', 'tui_gateway.entry'], { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] })
    this.lifecycle(`[lifecycle] spawned gateway child ${describeChild(this.proc)} python=${python} cwd=${cwd}`)

    this.stdoutRl = createInterface({ input: this.proc.stdout! })
    this.stdoutRl.on('line', raw => {
      try {
        this.dispatch(JSON.parse(raw))
      } catch {
        const preview = raw.trim().slice(0, MAX_LOG_PREVIEW) || '(empty line)'

        this.pushLog(`[protocol] malformed stdout: ${preview}`)
        this.publish({ type: 'gateway.protocol_error', payload: { preview } })
      }
    })

    this.stderrRl = createInterface({ input: this.proc.stderr! })
    this.stderrRl.on('line', raw => {
      const line = truncateLine(raw.trim())

      if (!line) {
        return
      }

      this.pushLog(line)
      this.publish({ type: 'gateway.stderr', payload: { line } })
    })

    const ownedProc = this.proc
    this.proc.on('error', err => {
      // Skip stale errors on an already-replaced child.
      if (this.proc !== ownedProc) {
        this.pushLog(`[lifecycle] stale child error ignored ${describeChild(ownedProc)} message=${err.message}`)

        return
      }

      const line = `[spawn] ${err.message}`

      this.lifecycle(`[lifecycle] child error ${describeChild(ownedProc)} message=${err.message}`)
      this.pushLog(line)
      this.publish({ type: 'gateway.stderr', payload: { line } })
      // Detach the reference up front so the late `exit` event for
      // this same child is identity-skipped (we don't want to emit
      // 'exit' twice). Then run the full teardown — clears the
      // startup timer so we don't fire a misleading
      // `gateway.start_timeout`, rejects pending RPCs, and emits or
      // queues a single `exit`.
      this.proc = null
      this.handleTransportExit(1, `gateway error: ${err.message}`)
    })
    this.proc.on('exit', (code, signal) => {
      // start() can replace `this.proc` while an old child is still
      // tearing down. Skip stale exits so we don't clear the new
      // startup timer or reject newly-issued pending requests.
      if (this.proc !== ownedProc) {
        this.pushLog(
          `[lifecycle] stale child exit ignored ${describeChild(ownedProc)} code=${code ?? 'null'} signal=${signal ?? 'null'}`
        )

        return
      }

      this.lifecycle(
        `[lifecycle] child exit ${describeChild(ownedProc)} code=${code ?? 'null'} signal=${signal ?? 'null'}`
      )
      this.handleTransportExit(code)
    })
  }

  private startAttachedGateway(attachUrl: string) {
    const safeAttachUrl = redactUrl(attachUrl)
    this.startReadyTimer('websocket', safeAttachUrl)

    const WebSocketCtor = getWebSocketCtor()

    if (typeof WebSocketCtor === 'undefined') {
      const line = `[startup] WebSocket API unavailable; cannot attach to ${safeAttachUrl}`

      this.pushLog(line)
      this.publish({ type: 'gateway.stderr', payload: { line } })
      this.handleTransportExit(1, 'gateway websocket unavailable')

      return
    }

    try {
      const ws = new WebSocketCtor(attachUrl)
      let settled = false

      this.ws = ws

      const connectPromise = new Promise<void>((resolve, reject) => {
        ws.addEventListener(
          'open',
          () => {
            if (!settled) {
              settled = true
              resolve()
            }

            this.lastActivityAt = Date.now()
            this.clearReconnect()
            this.connectSidecarMirror()
          },
          { once: true }
        )

        ws.addEventListener(
          'error',
          () => {
            if (!settled) {
              this.pushLog('[startup] gateway websocket connect error')
              settled = true
              reject(new Error('gateway websocket connection failed'))
            }
          },
          { once: true }
        )
        ws.addEventListener(
          'close',
          ev => {
            if (!settled) {
              settled = true
              reject(new Error(`gateway websocket closed (${ev.code}) during connect`))
            }
          },
          { once: true }
        )
      })

      // The connect promise is only awaited by RPCs that arrive while
      // the socket is still connecting. If no request races the open
      // (or a teardown drops the reference before anyone observes it),
      // a connect-error / early-close rejection would surface as an
      // unhandled promise rejection in Node. Attach a no-op handler to
      // ensure the rejection is always observed.
      connectPromise.catch(() => {})
      this.wsConnectPromise = connectPromise

      ws.addEventListener('message', ev => this.handleWebSocketFrame(ev.data))
      ws.addEventListener('close', ev => {
        // Skip close events from sockets that have already been
        // replaced — start() / closeGatewaySocket() can swap `this.ws`
        // before an in-flight close lands, and we must not clear the
        // new ready timer or reject the new pending requests on behalf
        // of a stale socket.
        if (this.ws !== ws) {
          this.pushLog(`[lifecycle] stale websocket close ignored code=${ev.code}`)

          return
        }

        this.pushLog(`[lifecycle] websocket close code=${ev.code}`)
        this.stopHeartbeat()
        this.ws = null
        this.wsConnectPromise = null
        this.handleTransportExit(ev.code, `gateway websocket closed${ev.code ? ` (${ev.code})` : ''}`)
      })
      ws.addEventListener('error', () => {
        const line = '[gateway] websocket transport error'

        this.pushLog(line)
        this.publish({ type: 'gateway.stderr', payload: { line } })
      })
    } catch (err) {
      this.pushLog(`[startup] failed to connect websocket gateway ${safeAttachUrl} (constructor error)`)
      this.handleTransportExit(1, 'gateway websocket startup failed')
    }
  }

  start() {
    this.disposed = false
    this.clearReconnect()

    const root = process.env.HERMES_PYTHON_SRC_ROOT ?? resolve(import.meta.dirname, '../../')
    const attachUrl = resolveGatewayAttachUrl()
    const sidecarUrl = resolveSidecarUrl()

    this.attachUrl = attachUrl
    this.sidecarUrl = sidecarUrl
    this.resetStartupState()
    this.clearReconnect()
    this.stopHeartbeat()

    if (this.proc && !this.proc.killed && this.proc.exitCode === null) {
      this.lifecycle(`[lifecycle] replacing live gateway child ${describeChild(this.proc)}`)
      this.proc.kill()
    }

    this.proc = null
    this.closeGatewaySocket()
    this.closeSidecarSocket()

    if (attachUrl) {
      this.startAttachedGateway(attachUrl)

      return
    }

    this.startSpawnedGateway(root)
  }

  private async reconstructSessionsAfterHostTakeover() {
    const tracked = [...this.durableSessionIds.entries()]
    const oldSessionIds: string[] = []
    const recoveredSessionIds: string[] = []
    const durableSessionIds: string[] = []

    for (const [oldSessionId, durableSessionId] of tracked) {
      const result = await this.request<{
        session_id?: unknown
      }>('session.resume', { session_id: durableSessionId }, false)

      const recoveredSessionId = typeof result?.session_id === 'string' ? result.session_id : ''

      if (!recoveredSessionId) {
        throw new Error(`session recovery returned no live id for ${durableSessionId}`)
      }

      oldSessionIds.push(oldSessionId)
      recoveredSessionIds.push(recoveredSessionId)
      durableSessionIds.push(durableSessionId)
    }

    for (const oldSessionId of oldSessionIds) {
      this.durableSessionIds.delete(oldSessionId)
    }

    recoveredSessionIds.forEach((sessionId, index) => {
      this.durableSessionIds.set(sessionId, durableSessionIds[index]!)
    })

    if (oldSessionIds.length > 0) {
      this.publish({
        type: 'session.runtime_owner_lost',
        payload: {
          durable_session_ids: durableSessionIds,
          recovered_session_ids: recoveredSessionIds,
          session_ids: oldSessionIds
        }
      })
    }
  }

  private publishSessionEvent(ev: GatewayEvent, attachmentReplay = false) {
    const wire = ev as GatewayEvent & { epoch?: unknown; seq?: unknown }
    const sessionId = typeof ev.session_id === 'string' ? ev.session_id : ''
    let acceptedWatermark: { epoch: string; seq: number } | null = null

    if (sessionId && this.pendingSessionAttach.has(sessionId) && !attachmentReplay) {
      const queued = this.queuedSessionEvents.get(sessionId) ?? []

      queued.push(ev)
      this.queuedSessionEvents.set(sessionId, queued)

      return
    }

    if (sessionId && typeof wire.seq === 'number' && Number.isInteger(wire.seq) && wire.seq >= 0) {
      const epoch = typeof wire.epoch === 'string' ? wire.epoch : ''
      const previous = this.sessionWatermarks.get(sessionId)

      if (previous?.epoch === epoch && wire.seq <= previous.seq) {
        return
      }

      acceptedWatermark = { epoch, seq: wire.seq }
    }

    if (ev.type === 'session.runtime_owner_lost') {
      const payload = (ev.payload ?? {}) as { session_ids?: unknown }

      const sessionIds = Array.isArray(payload.session_ids)
        ? payload.session_ids.filter((value): value is string => typeof value === 'string')
        : []

      this.publish({
        ...ev,
        payload: {
          durable_session_ids: sessionIds.map(id => this.durableSessionIds.get(id) ?? ''),
          session_ids: sessionIds
        }
      })
    } else {
      this.publish(ev)
    }

    if (sessionId && acceptedWatermark) {
      this.sessionWatermarks.set(sessionId, acceptedWatermark)
    }
  }

  private async restoreSessionAttachments(replayEpoch: string) {
    for (const [sessionId, durableSessionId] of [...this.durableSessionIds.entries()]) {
      const watermark = this.sessionWatermarks.get(sessionId)
      const lastSeenSeq = watermark?.epoch === replayEpoch ? watermark.seq : 0

      this.pendingSessionAttach.add(sessionId)
      this.queuedSessionEvents.set(sessionId, [])

      try {
        const result = await this.request<{
          events?: unknown
          latest_seq?: unknown
          replay_epoch?: unknown
          truncated?: unknown
        }>(
          'session.attach',
          {
            last_seen_seq: lastSeenSeq,
            mode: 'control',
            session_id: sessionId
          },
          false
        )

        if (result?.truncated === true) {
          const recoveredSessionId = await new Promise<string>((resolve, reject) => {
            this.publish({
              payload: {
                complete: resolve,
                durable_session_id: durableSessionId,
                fail: reject,
                live_session_id: sessionId
              },
              session_id: sessionId,
              type: 'session.replay_resync_required'
            })
          })

          const epoch = typeof result.replay_epoch === 'string' ? result.replay_epoch : replayEpoch

          const latestSeq =
            typeof result.latest_seq === 'number' && Number.isInteger(result.latest_seq) && result.latest_seq >= 0
              ? result.latest_seq
              : 0

          this.sessionWatermarks.delete(sessionId)
          this.sessionWatermarks.set(recoveredSessionId, { epoch, seq: latestSeq })

          const queued = this.queuedSessionEvents.get(sessionId) ?? []
          queued.sort((a, b) => Number((a as { seq?: unknown }).seq ?? 0) - Number((b as { seq?: unknown }).seq ?? 0))

          for (const event of queued) {
            this.publishSessionEvent(
              recoveredSessionId === sessionId ? event : { ...event, session_id: recoveredSessionId },
              true
            )
          }

          continue
        }

        const events = Array.isArray(result?.events)
          ? result.events.map(asGatewayEvent).filter((event): event is GatewayEvent => event !== null)
          : []

        events.sort((a, b) => Number((a as { seq?: unknown }).seq ?? 0) - Number((b as { seq?: unknown }).seq ?? 0))

        for (const event of events) {
          this.publishSessionEvent(event, true)
        }

        const queued = this.queuedSessionEvents.get(sessionId) ?? []
        queued.sort((a, b) => Number((a as { seq?: unknown }).seq ?? 0) - Number((b as { seq?: unknown }).seq ?? 0))

        for (const event of queued) {
          this.publishSessionEvent(event, true)
        }
      } finally {
        this.pendingSessionAttach.delete(sessionId)
        this.queuedSessionEvents.delete(sessionId)
      }
    }
  }

  private publishReadyAfterAttachment(ev: GatewayEvent) {
    const payload = ev.payload as
      | {
          capabilities?: unknown
          multi_client?: { methods?: unknown }
          replay_epoch?: unknown
          runtime_host_id?: unknown
        }
      | undefined

    const capabilities = Array.isArray(payload?.capabilities) ? payload.capabilities : []
    const methods = Array.isArray(payload?.multi_client?.methods) ? payload.multi_client.methods : []

    const nextRuntimeHostId =
      typeof payload?.runtime_host_id === 'string' && payload.runtime_host_id ? payload.runtime_host_id : null

    const runtimeHostChanged =
      this.runtimeHostId !== null && nextRuntimeHostId !== null && this.runtimeHostId !== nextRuntimeHostId

    const sameRuntimeHostReconnect =
      this.runtimeHostId !== null && nextRuntimeHostId !== null && this.runtimeHostId === nextRuntimeHostId

    const replayEpoch = typeof payload?.replay_epoch === 'string' ? payload.replay_epoch : ''

    if (!capabilities.includes('client.attach') && !methods.includes('client.attach')) {
      if (nextRuntimeHostId !== null) {
        this.runtimeHostId = nextRuntimeHostId
      }

      this.publish(ev)

      return
    }

    const generation = this.drainGeneration

    const fail = (err: unknown) => {
      if (this.drainGeneration !== generation || this.disposed) {
        return
      }

      const message = err instanceof Error ? err.message : String(err)

      this.pushLog(`[protocol] client attachment/recovery failed: ${message}`)
      this.publish({ type: 'gateway.protocol_error', payload: { preview: message.slice(0, MAX_LOG_PREVIEW) } })

      if (this.ws) {
        try {
          this.ws.close()
        } catch {
          // best effort; the close handler owns reconnect.
        }
      } else {
        this.proc?.kill()
      }
    }

    void this.request('client.attach', {
      client_id: this.clientId,
      protocol_version: 1,
      surface: 'tui'
    })
      .then(async () => {
        if (runtimeHostChanged) {
          await this.reconstructSessionsAfterHostTakeover()
        } else if (
          sameRuntimeHostReconnect &&
          (capabilities.includes('session.attach') || methods.includes('session.attach'))
        ) {
          await this.restoreSessionAttachments(replayEpoch)
        }
      })
      .then(() => {
        if (this.drainGeneration === generation && !this.disposed) {
          if (nextRuntimeHostId !== null) {
            this.runtimeHostId = nextRuntimeHostId
          }

          this.publish(ev)
        }
      }, fail)
  }

  private dispatch(msg: Record<string, unknown>) {
    const id = msg.id as string | undefined

    if (id && id === this.heartbeatPendingId) {
      this.heartbeatPendingId = null
      this.heartbeatSentAt = 0

      return
    }

    const p = id ? this.pending.get(id) : undefined

    if (p) {
      this.settle(p, msg.error ? this.toError(msg.error) : null, msg.result)

      return
    }

    if (msg.method === 'event') {
      const ev = asGatewayEvent(msg.params)

      if (ev) {
        if (ev.type === 'gateway.ready') {
          this.publishReadyAfterAttachment(ev)
        } else {
          this.publishSessionEvent(ev)
        }
      }
    }
  }

  private toError(raw: unknown): Error {
    const err = raw as { message?: unknown } | null | undefined

    return new Error(typeof err?.message === 'string' ? err.message : 'request failed')
  }

  private settle(p: Pending, err: Error | null, result: unknown) {
    clearTimeout(p.timeout)
    this.pending.delete(p.id)

    if (err) {
      p.reject(err)
    } else {
      if (p.trackSessionResult) {
        this.trackSessionResult(p.method, p.params, result)
      }

      p.resolve(result)
    }
  }

  private trackSessionResult(method: string, params: Record<string, unknown>, result: unknown) {
    if (method !== 'session.create' && method !== 'session.resume') {
      return
    }

    const value = result as {
      session_id?: unknown
      stored_session_id?: unknown
      session_key?: unknown
      resumed?: unknown
    } | null

    const liveSessionId = typeof value?.session_id === 'string' ? value.session_id : ''

    if (!liveSessionId) {
      return
    }

    const requested = typeof params.session_id === 'string' ? params.session_id : ''

    const storedSessionId = typeof value?.stored_session_id === 'string' ? value.stored_session_id : ''

    const sessionKey = typeof value?.session_key === 'string' ? value.session_key : ''
    const resumed = typeof value?.resumed === 'string' ? value.resumed : ''

    const durableSessionId =
      method === 'session.resume'
        ? requested || resumed || sessionKey || liveSessionId
        : storedSessionId || sessionKey || liveSessionId

    for (const [sessionId, durableId] of this.durableSessionIds) {
      if (durableId === durableSessionId) {
        this.durableSessionIds.delete(sessionId)
      }
    }

    this.durableSessionIds.set(liveSessionId, durableSessionId)
  }

  private pushLog(line: string) {
    this.logs.push(truncateLine(line))
  }

  // Death-explaining breadcrumbs (spawn / exit / kill / replace) — kept in the
  // in-memory tail for /logs AND persisted to the gateway crash log so the
  // reason survives a parent exit and lands next to the child's SIGTERM panic.
  private lifecycle(line: string) {
    this.pushLog(line)
    recordParentLifecycle(line)
  }

  private rejectPending(err: Error) {
    for (const p of this.pending.values()) {
      clearTimeout(p.timeout)
      p.reject(err)
    }

    this.pending.clear()
  }

  // Arrow class-field — stable identity, so `setTimeout(this.onTimeout, …, id)`
  // doesn't allocate a bound function per request.
  private onTimeout = (id: string) => {
    const p = this.pending.get(id)

    if (p) {
      this.pending.delete(id)
      p.reject(new Error(`timeout: ${p.method}`))
    }
  }

  drain() {
    // Defer the buffered-event replay to the next microtask, and DO NOT flip
    // `subscribed` until that microtask runs.
    //
    // `drain()` is called from the consumer's mount-time subscribe effect
    // (ui-tui/src/app/useMainApp.ts). In *attach* mode the gateway is already
    // running, so it replays `gateway.ready` / `session.info` the instant the
    // socket connects — those land in `bufferedEvents` *before* the consumer
    // subscribes. If we emitted them synchronously here, the `gateway.ready`
    // handler's `patchUiState` / `setHistoryItems` cascade would run while
    // React is still inside the first commit, tripping "Too many re-renders"
    // (Minified React error #301) — issue #36658. Spawn/inline/sidecar modes
    // don't hit this because `gateway.ready` only arrives after the Python
    // child boots, i.e. on a later async tick.
    //
    // Crucially, `subscribed` stays false until the flush so any LIVE event
    // arriving in the gap between here and the microtask keeps buffering
    // (publish() pushes when !subscribed) instead of emitting synchronously
    // and jumping ahead of the chronologically-earlier replayed events. The
    // flush re-drains the buffer right after flipping `subscribed`, so any
    // in-window arrivals are delivered in FIFO order. A generation token makes
    // the queued microtask a no-op if the transport was reset/killed meanwhile.
    const generation = this.drainGeneration

    queueMicrotask(() => {
      if (this.drainGeneration !== generation) {
        return
      }

      this.subscribed = true

      // Replay everything buffered up to now, then any events that arrived in
      // the gap before this microtask ran — all in chronological order.
      for (const ev of this.bufferedEvents.drain()) {
        this.emit('event', ev)
      }

      if (this.pendingExit !== undefined) {
        const code = this.pendingExit

        this.pendingExit = undefined
        this.emit('exit', code)
      }
    })
  }

  getLogTail(limit = 20): string {
    return this.logs.tail(Math.max(1, limit)).join('\n')
  }

  private async ensureAttachedWebSocket(method: string): Promise<WebSocket> {
    if (!this.attachUrl) {
      throw new Error('gateway not running')
    }

    if (!this.ws || this.ws.readyState === WS_CLOSED || this.ws.readyState === WS_CLOSING) {
      this.start()
    }

    if (this.ws?.readyState === WS_CONNECTING) {
      try {
        await this.wsConnectPromise
      } catch (err) {
        throw err instanceof Error ? err : new Error(String(err))
      }
    }

    if (!this.ws || this.ws.readyState !== WS_OPEN) {
      throw new Error(`gateway not connected: ${method}`)
    }

    return this.ws
  }

  private requestOverWebSocket<T = unknown>(
    method: string,
    params: Record<string, unknown> = {},
    trackSessionResult = true
  ): Promise<T> {
    return this.ensureAttachedWebSocket(method).then(
      ws =>
        new Promise<T>((resolve, reject) => {
          const id = `r${++this.reqId}`
          const timeout = setTimeout(this.onTimeout, requestTimeoutMs(method), id)

          timeout.unref?.()
          this.pending.set(id, {
            id,
            method,
            params,
            reject,
            resolve: v => resolve(v as T),
            trackSessionResult,
            timeout
          })

          try {
            ws.send(JSON.stringify({ id, jsonrpc: '2.0', method, params }))
          } catch (e) {
            const pending = this.pending.get(id)

            if (pending) {
              clearTimeout(pending.timeout)
              this.pending.delete(id)
            }

            reject(e instanceof Error ? e : new Error(String(e)))
          }
        })
    )
  }

  request<T = unknown>(method: string, params: Record<string, unknown> = {}, trackSessionResult = true): Promise<T> {
    const attachUrl = resolveGatewayAttachUrl()

    if (attachUrl) {
      if (this.attachUrl !== attachUrl) {
        // The env var rotated at runtime — restart the transport so
        // switching from spawned-gateway mode to attach mode also
        // tears down the old Python child. Merely closing `this.ws`
        // would leave a previously spawned gateway process alive.
        this.rejectPending(new Error('gateway attach url changed'))
        this.start()
      }

      return this.requestOverWebSocket<T>(method, params, trackSessionResult)
    }

    if (!this.proc?.stdin || this.proc.killed || this.proc.exitCode !== null) {
      this.start()
    }

    if (!this.proc?.stdin) {
      return Promise.reject(new Error('gateway not running'))
    }

    const id = `r${++this.reqId}`

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(this.onTimeout, requestTimeoutMs(method), id)

      timeout.unref?.()

      this.pending.set(id, {
        id,
        method,
        params,
        reject,
        resolve: v => resolve(v as T),
        trackSessionResult,
        timeout
      })

      try {
        this.proc!.stdin!.write(JSON.stringify({ id, jsonrpc: '2.0', method, params }) + '\n')
      } catch (e) {
        const pending = this.pending.get(id)

        if (pending) {
          clearTimeout(pending.timeout)
          this.pending.delete(id)
        }

        reject(e instanceof Error ? e : new Error(String(e)))
      }
    })
  }

  kill(reason = 'requested') {
    this.disposed = true
    this.clearReconnect()
    this.stopHeartbeat()
    const proc = this.proc
    const killed = proc?.kill()

    this.lifecycle(
      `[lifecycle] GatewayClient.kill reason=${reason} ${describeChild(proc)} killResult=${killed ?? 'none'}`
    )
    this.closeGatewaySocket()
    this.closeSidecarSocket()
    this.clearReadyTimer()
    // The ws 'close' handler is identity-gated on `this.ws === ws`
    // and we just nulled `this.ws`, so it will short-circuit and
    // skip handleTransportExit. Reject pending RPCs explicitly so
    // attach-mode promises do not hang after an intentional kill.
    this.rejectPending(new Error('gateway closed'))
  }
}
