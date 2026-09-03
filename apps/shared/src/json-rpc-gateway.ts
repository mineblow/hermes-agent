export type GatewayEventName =
  | 'gateway.ready'
  | 'session.info'
  | 'session.usage'
  | 'message.start'
  | 'message.delta'
  | 'message.interim'
  | 'message.complete'
  | 'thinking.delta'
  | 'reasoning.delta'
  | 'reasoning.available'
  | 'status.update'
  | 'tool.start'
  | 'tool.progress'
  | 'tool.complete'
  | 'tool.generating'
  | 'todo.updated'
  | 'clarify.request'
  | 'approval.request'
  | 'sudo.request'
  | 'secret.request'
  | 'background.complete'
  | 'error'
  | 'skin.changed'
  | (string & {})

export interface GatewayEvent<P = unknown> {
  /** Opaque server numbering generation paired with seq. */
  epoch?: string
  payload?: P
  /** Renderer-side source tag added by the Desktop gateway registry. */
  profile?: string
  /** Registry connection whose socket delivered the event (renderer-side tag;
   * absent for the local/legacy primary path). */
  connectionId?: string
  session_id?: string
  seq?: number
  type: GatewayEventName
}

export type ConnectionState = 'idle' | 'connecting' | 'open' | 'closed' | 'error'
export type DurableResyncReason =
  'replay_epoch_changed' | 'replay_truncated' | 'runtime_host_changed' | 'runtime_owner_lost'
export interface DurableResyncRequest {
  durable_session_id?: string
  reason: DurableResyncReason
  session_id: string
}
export interface RuntimeSessionRebound {
  durable_session_id: string
  new_session_id: string
  old_session_id: string
}
export type GatewayRequestId = number | string
export type AttachmentMode = 'observe' | 'control'
export type AttachmentCapability =
  | 'observe'
  | 'prompt.submit'
  | 'session.steer'
  | 'session.interrupt'
  | 'approval.respond'
  | 'clarify.respond'
  | 'ui.respond'
export type ClientCapability = 'session.observe' | 'session.control' | 'session.replay'
export type MultiClientMethod =
  'client.attach' | 'session.attach' | 'session.detach' | 'session.attachments' | 'session.events.since'

export interface GatewayReadyPayload {
  capabilities?: MultiClientMethod[]
  change_events?: boolean
  connection_id: string
  heartbeat?: boolean
  multi_client?: {
    attachment_modes: AttachmentMode[]
    methods: MultiClientMethod[]
    protocol_version: 1
  }
  multi_client_sessions?: 1
  replay_epoch: string
  /** Opaque canonical-runtime host generation. Optional for legacy gateways. */
  runtime_host_id?: string
  skin?: unknown
}

export interface ClientAttachParams {
  capabilities?: ClientCapability[]
  client_id: string
  protocol_version: 1
  surface: string
}

export interface GatewayClientIdStorage {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
}

const GATEWAY_CLIENT_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/
const GATEWAY_SURFACE_PATTERN = /^[a-z0-9][a-z0-9._-]{0,31}$/

function randomGatewayClientId(): string {
  try {
    if (typeof globalThis.crypto?.randomUUID === 'function') {
      return globalThis.crypto.randomUUID()
    }
  } catch {
    // Fall through for restricted browser contexts.
  }

  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 14)}`
}

/** Stable per-window identity: sessionStorage survives reconnect/reload but is isolated across windows. */
export function getOrCreateGatewayClientId(
  surface: string,
  storage?: GatewayClientIdStorage | null,
  createId: () => string = randomGatewayClientId
): string {
  const normalizedSurface = surface.trim().toLowerCase()

  if (!GATEWAY_SURFACE_PATTERN.test(normalizedSurface)) {
    throw new TypeError('gateway client surface is invalid')
  }

  const key = `hermes.gateway.client-id.${normalizedSurface}.v1`

  try {
    const existing = storage?.getItem(key)

    if (existing && GATEWAY_CLIENT_ID_PATTERN.test(existing)) {
      return existing
    }
  } catch {
    // Storage can be unavailable in private/restricted browser contexts.
  }

  const generated = `${normalizedSurface}:${createId()}`

  if (!GATEWAY_CLIENT_ID_PATTERN.test(generated)) {
    throw new TypeError('generated gateway client identity is invalid')
  }

  try {
    storage?.setItem(key, generated)
  } catch {
    // The in-memory identity remains valid for this client instance.
  }

  return generated
}

export interface ClientAttachResult {
  capabilities: ClientCapability[]
  client_id: string
  connection_id: string
  idempotent: boolean
  protocol_version: 1
  surface: string
}

export interface SessionAttachmentSnapshot {
  capabilities: AttachmentCapability[]
  client_id: string
  mode: AttachmentMode
}

export interface SessionAttachParams {
  last_seen_seq?: number
  mode: AttachmentMode
  session_id: string
}

export interface SessionAttachResult extends SessionAttachmentSnapshot {
  epoch: string
  events: GatewayEvent[]
  latest_seq: number
  replay?: {
    events: GatewayEvent[]
    truncated: boolean
  }
  replay_epoch: string
  session_id: string
  truncated: boolean
}

export interface SessionDetachParams {
  session_id: string
}

export interface SessionDetachResult {
  detached: boolean
  session_id: string
}

export interface SessionAttachmentsParams {
  session_id: string
}

export interface SessionAttachmentsResult {
  attachments: SessionAttachmentSnapshot[]
  session_id: string
}

export interface SessionEventsSinceParams {
  last_seen?: number
  last_seen_seq?: number
  session_id: string
  since_seq?: number
}

export interface SessionEventsSinceResult {
  count: number
  epoch: string
  events: GatewayEvent[]
  latest_seq: number
  truncated: boolean
}

export interface JsonRpcErrorPayload {
  code?: number
  data?: unknown
  message?: string
}

export interface JsonRpcFrame {
  error?: JsonRpcErrorPayload
  id?: GatewayRequestId | null
  method?: string
  params?: GatewayEvent
  result?: unknown
}

/** JSON-RPC error with optional structured `data` from the gateway. */
export class JsonRpcGatewayError extends Error {
  readonly code?: number
  readonly data?: unknown

  constructor(message: string, options?: { code?: number; data?: unknown }) {
    super(message)
    this.name = 'JsonRpcGatewayError'
    this.code = options?.code
    this.data = options?.data
  }
}

export type WebSocketLike = WebSocket

type PendingCall = {
  reject: (error: Error) => void
  resolve: (value: unknown) => void
  timer?: ReturnType<typeof setTimeout>
}

type TrackedSession = {
  durableSessionId?: string
  mode: AttachmentMode
}

export interface GatewayClientOptions {
  /** Stable logical identity and capability request for negotiated multi-client gateways. */
  clientAttachment?: ClientAttachParams
  closedErrorMessage?: string
  connectErrorMessage?: string
  connectTimeoutMs?: number
  createRequestId?: (nextId: number) => GatewayRequestId
  heartbeatDeadlineMs?: number
  heartbeatIntervalMs?: number
  onDurableResyncRequired?: (request: DurableResyncRequest) => void
  onRuntimeSessionRebound?: (rebound: RuntimeSessionRebound) => void
  /** Return true to intercept the default closed-state transition. */
  onSocketClose?: (event: CloseEvent) => boolean | void
  requestIdPrefix?: string
  requestTimeoutMs?: number
  socketFactory?: (url: string) => WebSocketLike
  notConnectedErrorMessage?: string
}

const ANY = '*'
const DEFAULT_REQUEST_TIMEOUT_MS = 120_000
// Replay fetch after reconnect: bounded so a wedged backend can't hold the
// guard open; generous enough for a 512-frame ring to drain.
const REPLAY_REQUEST_TIMEOUT_MS = 10_000
const DEFAULT_HEARTBEAT_INTERVAL_MS = 15_000
const DEFAULT_HEARTBEAT_DEADLINE_MS = 45_000
// A reconnect after sleep/wake must not hang forever in 'connecting' (which
// keeps the composer disabled and stuck on "Starting Hermes..."). If the open
// handshake doesn't land in this window, fail to 'error' so callers can retry.
const DEFAULT_CONNECT_TIMEOUT_MS = 15_000

export class JsonRpcGatewayClient {
  private nextId = 0
  private pending = new Map<GatewayRequestId, PendingCall>()
  private socket: WebSocketLike | null = null
  private state: ConnectionState = 'idle'
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private heartbeatSequence = 0
  private lastInboundAt = 0
  /** Last observed event seq per session_id — drives lossless reconnect replay. */
  private lastSeenSeq = new Map<string, number>()
  /** Live sessions plus their durable resume ids, restored after reconnect/owner loss. */
  private trackedSessions = new Map<string, TrackedSession>()
  /** Set while a post-reconnect replay fetch is in flight (dedup guard). */
  private replayInFlight = false
  /**
   * While a replay fetch is in flight, live seq'd frames for the sessions
   * being replayed are parked here instead of dispatching immediately.
   * Without this hold, a live frame racing the replay response is dispatched
   * twice (once live, once when the replay returns the same seq) or, worse,
   * advances the watermark so the gap events the replay carries get skipped.
   */
  private replayHold: Map<string, GatewayEvent[]> | null = null
  /**
   * Server process identity for the replay contract (from gateway.ready /
   * session.events.since). Seq counters are in-process on the backend, so a
   * restart resets them while we still hold high watermarks — without this
   * check events_since(sid, 97) returns [] + truncated=false forever and we
   * silently believe nothing was missed.
   */
  private replayEpoch: string | null = null
  /** Canonical runtime host generation learned from gateway.ready. */
  private runtimeHostId: string | null = null
  private runtimeHostRecoveryPending = false
  private readonly eventHandlers = new Map<string, Set<(event: GatewayEvent) => void>>()
  private readonly stateHandlers = new Set<(state: ConnectionState) => void>()
  private readonly options: Required<
    Omit<
      GatewayClientOptions,
      'clientAttachment' | 'onDurableResyncRequired' | 'onRuntimeSessionRebound' | 'socketFactory'
    >
  > &
    Pick<
      GatewayClientOptions,
      'clientAttachment' | 'onDurableResyncRequired' | 'onRuntimeSessionRebound' | 'socketFactory'
    >
  private negotiatingConnect: {
    reject: (error: Error) => void
    resolve: () => void
    socket: WebSocketLike
  } | null = null

  constructor(options: GatewayClientOptions = {}) {
    this.options = {
      clientAttachment: options.clientAttachment,
      closedErrorMessage: options.closedErrorMessage ?? 'WebSocket closed',
      connectErrorMessage: options.connectErrorMessage ?? 'WebSocket connection failed',
      connectTimeoutMs: options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
      createRequestId: options.createRequestId ?? ((nextId: number) => `${options.requestIdPrefix ?? 'r'}${nextId}`),
      heartbeatDeadlineMs: options.heartbeatDeadlineMs ?? DEFAULT_HEARTBEAT_DEADLINE_MS,
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
      notConnectedErrorMessage: options.notConnectedErrorMessage ?? 'gateway not connected',
      onDurableResyncRequired: options.onDurableResyncRequired,
      onRuntimeSessionRebound: options.onRuntimeSessionRebound,
      onSocketClose: options.onSocketClose ?? (() => false),
      requestIdPrefix: options.requestIdPrefix ?? 'r',
      requestTimeoutMs: options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      socketFactory: options.socketFactory
    }
  }

  get connectionState(): ConnectionState {
    return this.state
  }

  async connect(wsUrl: string): Promise<void> {
    // Refuse garbage; WebSocket coerces non-strings into
    // `ws://<origin>/[object%20Object]` (#68250 stale-emit boot loop).
    const invalidUrl = () => {
      const got = typeof wsUrl === 'string' ? JSON.stringify(wsUrl) : `type "${typeof wsUrl}"`

      return new Error(`gateway connect() requires a ws:// or wss:// URL string, got ${got}`)
    }

    if (typeof wsUrl !== 'string') {
      throw invalidUrl()
    }

    let url: URL

    try {
      url = new URL(wsUrl)
    } catch {
      throw invalidUrl()
    }

    if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
      throw invalidUrl()
    }

    if (this.socket?.readyState === WebSocket.OPEN || this.state === 'connecting') {
      return
    }

    this.setState('connecting')

    const socket = this.options.socketFactory?.(wsUrl) ?? new WebSocket(wsUrl)
    this.socket = socket
    this.stopHeartbeat()

    socket.addEventListener('message', message => {
      if (this.socket !== socket) {
        return
      }

      this.lastInboundAt = Date.now()
      this.handleMessage(message.data)
    })

    socket.addEventListener('close', event => {
      if (this.socket !== socket) {
        return
      }

      if (this.options.onSocketClose(event)) {
        return
      }

      this.socket = null
      this.stopHeartbeat()
      this.setState('closed')
      this.rejectAllPending(new Error(this.options.closedErrorMessage))
    })

    await new Promise<void>((resolve, reject) => {
      let settled = false
      let timer: ReturnType<typeof setTimeout> | undefined

      const cleanup = () => {
        if (timer !== undefined) {
          clearTimeout(timer)
        }

        socket.removeEventListener('open', onOpen)
        socket.removeEventListener('error', onError)
      }

      const complete = () => {
        if (settled || this.socket !== socket) {
          return
        }

        settled = true
        this.setState('open')

        if (this.negotiatingConnect?.socket === socket) {
          this.negotiatingConnect = null
        }

        cleanup()
        resolve()
      }

      const fail = (error: Error, updateState = true) => {
        if (settled) {
          return
        }

        settled = true

        if (this.negotiatingConnect?.socket === socket) {
          this.negotiatingConnect = null
        }

        cleanup()

        if (updateState) {
          this.setState('error')
        }

        reject(error)
      }

      const onOpen = () => {
        if (settled || this.socket !== socket) {
          return
        }

        if (this.options.clientAttachment) {
          this.negotiatingConnect = {
            reject: fail,
            resolve: complete,
            socket
          }

          return
        }

        complete()
        // Legacy clients retain their pre-negotiation replay behavior.
        void this.fetchReplay()
      }

      const onError = () => {
        if (this.socket !== socket) {
          return
        }

        fail(new Error(this.options.connectErrorMessage))
      }

      socket.addEventListener('open', onOpen, { once: true })
      socket.addEventListener('error', onError, { once: true })

      if (this.options.connectTimeoutMs > 0) {
        timer = setTimeout(() => {
          if (settled) {
            return
          }

          const isCurrentSocket = this.socket === socket

          // Drop the half-open or unnegotiated socket so the next connect()
          // starts clean instead of short-circuiting on a zombie state.
          if (isCurrentSocket) {
            try {
              socket.close()
            } catch {
              // ignore
            }

            this.socket = null
          }

          fail(new Error(this.options.connectErrorMessage), isCurrentSocket)
        }, this.options.connectTimeoutMs)
      }
    })
  }

  attachClient(params: ClientAttachParams): Promise<ClientAttachResult> {
    return this.request<ClientAttachResult>('client.attach', { ...params })
  }

  async attachSession(params: SessionAttachParams): Promise<SessionAttachResult> {
    const result = await this.request<SessionAttachResult>('session.attach', { ...params })

    const existing = this.trackedSessions.get(params.session_id)
    this.trackedSessions.set(params.session_id, {
      durableSessionId: existing?.durableSessionId,
      mode: params.mode
    })
    this.applySessionAttachResult(result)

    return result
  }

  async detachSession(params: SessionDetachParams): Promise<SessionDetachResult> {
    const result = await this.request<SessionDetachResult>('session.detach', { ...params })

    if (result.detached) {
      this.trackedSessions.delete(params.session_id)
      this.lastSeenSeq.delete(params.session_id)
    }

    return result
  }

  listSessionAttachments(params: SessionAttachmentsParams): Promise<SessionAttachmentsResult> {
    return this.request<SessionAttachmentsResult>('session.attachments', { ...params })
  }

  getSessionEventsSince(params: SessionEventsSinceParams): Promise<SessionEventsSinceResult> {
    return this.request<SessionEventsSinceResult>('session.events.since', { ...params })
  }

  close(): void {
    const socket = this.socket

    if (!socket) {
      return
    }

    try {
      socket.close()
    } finally {
      this.socket = null
      this.stopHeartbeat()
      this.setState('closed')
      this.rejectAllPending(new Error(this.options.closedErrorMessage))
    }
  }

  /**
   * Invalidate the current socket generation after an ambiguous transport
   * outcome. The outer connection owner decides whether/when to reconnect.
   */
  invalidate(message = this.options.closedErrorMessage): void {
    const socket = this.socket

    if (!socket) {
      return
    }

    this.invalidateSocket(socket, new Error(message))
  }

  on<P = unknown>(type: GatewayEventName, handler: (event: GatewayEvent<P>) => void): () => void {
    let handlers = this.eventHandlers.get(type)

    if (!handlers) {
      handlers = new Set()
      this.eventHandlers.set(type, handlers)
    }

    handlers.add(handler as (event: GatewayEvent) => void)

    return () => handlers?.delete(handler as (event: GatewayEvent) => void)
  }

  onAny(handler: (event: GatewayEvent) => void): () => void {
    return this.on(ANY as GatewayEventName, handler)
  }

  onEvent(handler: (event: GatewayEvent) => void): () => void {
    return this.onAny(handler)
  }

  onState(handler: (state: ConnectionState) => void): () => void {
    this.stateHandlers.add(handler)
    handler(this.state)

    return () => this.stateHandlers.delete(handler)
  }

  request<T>(
    method: string,
    params: Record<string, unknown> = {},
    timeoutMs = this.options.requestTimeoutMs,
    signal?: AbortSignal
  ): Promise<T> {
    const socket = this.socket

    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error(this.options.notConnectedErrorMessage))
    }

    if (signal?.aborted) {
      return Promise.reject(new DOMException('Aborted', 'AbortError'))
    }

    const id = this.options.createRequestId(++this.nextId)

    return new Promise<T>((resolve, reject) => {
      let onAbort: (() => void) | undefined

      const detach = () => {
        if (onAbort && signal) {
          signal.removeEventListener('abort', onAbort)
        }
      }

      const pending: PendingCall = {
        resolve: value => {
          detach()
          this.trackSuccessfulSessionRequest(method, params, value)
          resolve(value as T)
        },
        reject: error => {
          detach()
          reject(error)
        }
      }

      if (timeoutMs > 0) {
        pending.timer = setTimeout(() => {
          if (this.pending.delete(id)) {
            detach()
            // Include the configured timeout so a caller (or a user looking
            // at an error toast) can tell whether the default 30s window
            // fired or a per-call override — e.g. /compress opts into 120s.
            const seconds = Math.round(timeoutMs / 1000)
            reject(new Error(`request timed out after ${seconds}s: ${method}`))
          }
        }, timeoutMs)
      }

      // Abort drops the pending call immediately (no dangling resolver/timer);
      // server-side cancellation is a separate cooperative RPC where it matters.
      if (signal) {
        onAbort = () => {
          const call = this.pending.get(id)

          if (call?.timer) {
            clearTimeout(call.timer)
          }

          this.pending.delete(id)
          detach()
          reject(new DOMException('Aborted', 'AbortError'))
        }

        signal.addEventListener('abort', onAbort, { once: true })
      }

      this.pending.set(id, pending)

      try {
        socket.send(
          JSON.stringify({
            jsonrpc: '2.0',
            id,
            method,
            params
          })
        )
      } catch (error) {
        this.clearPending(id)
        detach()
        reject(error instanceof Error ? error : new Error(String(error)))
      }
    })
  }

  private trackSuccessfulSessionRequest(method: string, params: Record<string, unknown>, value: unknown): void {
    if (method !== 'session.create' && method !== 'session.resume') {
      return
    }

    const result = value as Record<string, unknown> | null
    const liveSessionId = typeof result?.session_id === 'string' ? result.session_id : ''

    if (!liveSessionId) {
      return
    }

    const requested = typeof params.session_id === 'string' ? params.session_id : ''

    const storedSessionId = typeof result?.stored_session_id === 'string' ? result.stored_session_id : ''

    const resultKey = typeof result?.session_key === 'string' ? result.session_key : ''
    const resumed = typeof result?.resumed === 'string' ? result.resumed : ''

    const durableSessionId =
      method === 'session.resume'
        ? requested || resumed || resultKey || liveSessionId
        : storedSessionId || resultKey || liveSessionId

    let mode: AttachmentMode = 'control'

    for (const [sessionId, tracked] of this.trackedSessions) {
      if (tracked.durableSessionId === durableSessionId) {
        mode = tracked.mode
        this.trackedSessions.delete(sessionId)
        this.lastSeenSeq.delete(sessionId)
      }
    }

    this.trackedSessions.set(liveSessionId, { durableSessionId, mode })
  }

  private async reconstructTrackedSessionsAfterHostChange(): Promise<void> {
    const tracked = [...this.trackedSessions.entries()]

    for (const [staleSessionId, session] of tracked) {
      try {
        const result = await this.request<{ session_id?: string }>('session.resume', {
          session_id: session.durableSessionId
        })

        const recoveredSessionId = result?.session_id

        if (session.durableSessionId && recoveredSessionId) {
          this.options.onRuntimeSessionRebound?.({
            durable_session_id: session.durableSessionId,
            new_session_id: recoveredSessionId,
            old_session_id: staleSessionId
          })
        }
      } catch (error: unknown) {
        // Never attach a stale process-local id to a replacement runtime.
        this.trackedSessions.delete(staleSessionId)
        this.lastSeenSeq.delete(staleSessionId)
        this.dispatchEvent({
          type: 'error',
          session_id: staleSessionId,
          payload: {
            code: 'runtime_recovery_failed',
            message:
              error instanceof Error
                ? `Session recovery failed: ${error.message}`
                : 'Session recovery failed after runtime host takeover.'
          }
        })
      }
    }
  }

  private applySessionAttachResult(result: SessionAttachResult): void {
    if (result.epoch) {
      this.adoptReplayEpoch(result.epoch)
    }

    if (result.truncated) {
      this.lastSeenSeq.delete(result.session_id)
      const durableSessionId = this.trackedSessions.get(result.session_id)?.durableSessionId
      this.options.onDurableResyncRequired?.({
        ...(durableSessionId ? { durable_session_id: durableSessionId } : {}),
        reason: 'replay_truncated',
        session_id: result.session_id
      })

      return
    }

    for (const event of result.events) {
      if (event?.type) {
        this.dispatchIfNewer(event)
      }
    }
  }

  private async negotiateReady(payload: unknown): Promise<void> {
    const pending = this.negotiatingConnect
    const attachment = this.options.clientAttachment

    if (!pending || !attachment || pending.socket !== this.socket) {
      return
    }

    const methods = (payload as GatewayReadyPayload | undefined)?.multi_client?.methods

    try {
      if (methods?.includes('client.attach')) {
        await this.attachClient(attachment)

        if (this.runtimeHostRecoveryPending) {
          this.runtimeHostRecoveryPending = false
          await this.reconstructTrackedSessionsAfterHostChange()
        }

        if (methods.includes('session.attach') && this.trackedSessions.size > 0) {
          this.replayHold = new Map([...this.trackedSessions.keys()].map(sessionId => [sessionId, []]))

          try {
            for (const [sessionId, tracked] of this.trackedSessions) {
              const result = await this.request<SessionAttachResult>('session.attach', {
                session_id: sessionId,
                mode: tracked.mode,
                last_seen_seq: this.lastSeenSeq.get(sessionId) ?? 0
              })

              this.applySessionAttachResult(result)
            }
          } finally {
            this.flushReplayHold()
          }
        }
      } else {
        // Legacy gateway: identity negotiation is unavailable, so retain the
        // previous best-effort replay path and let connect proceed.
        void this.fetchReplay()
      }

      pending.resolve()
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error))

      pending.reject(failure)

      if (this.socket === pending.socket) {
        this.invalidateSocket(pending.socket, failure)
      }
    }
  }

  private handleMessage(raw: unknown): void {
    const text = typeof raw === 'string' ? raw : String(raw)
    let frame: JsonRpcFrame

    try {
      frame = JSON.parse(text) as JsonRpcFrame
    } catch {
      return
    }

    if (frame.id !== undefined && frame.id !== null) {
      const call = this.pending.get(frame.id)

      if (!call) {
        return
      }

      this.clearPending(frame.id)

      if (frame.error) {
        call.reject(
          new JsonRpcGatewayError(frame.error.message || 'Hermes RPC failed', {
            code: typeof frame.error.code === 'number' ? frame.error.code : undefined,
            data: frame.error.data
          })
        )
      } else {
        call.resolve(frame.result)
      }

      return
    }

    if (frame.method === 'event' && frame.params?.type) {
      if (frame.params.type === 'session.runtime_owner_lost') {
        const payload = frame.params.payload as { session_ids?: unknown } | undefined
        const sessionIds = Array.isArray(payload?.session_ids) ? payload.session_ids : []

        for (const sessionId of sessionIds) {
          if (typeof sessionId !== 'string' || !this.trackedSessions.has(sessionId)) {
            continue
          }

          this.lastSeenSeq.delete(sessionId)
          this.replayHold?.delete(sessionId)
          const tracked = this.trackedSessions.get(sessionId)

          const resync: DurableResyncRequest = {
            reason: 'runtime_owner_lost',
            session_id: sessionId
          }

          if (tracked?.durableSessionId) {
            resync.durable_session_id = tracked.durableSessionId
          }

          this.options.onDurableResyncRequired?.(resync)

          if (tracked?.durableSessionId) {
            void this.request<{ session_id?: string }>('session.resume', {
              session_id: tracked.durableSessionId
            })
              .then(result => {
                if (typeof result?.session_id === 'string' && result.session_id) {
                  this.options.onRuntimeSessionRebound?.({
                    durable_session_id: tracked.durableSessionId!,
                    new_session_id: result.session_id,
                    old_session_id: sessionId
                  })
                }
              })
              .catch((error: unknown) => {
                this.dispatchEvent({
                  type: 'error',
                  session_id: sessionId,
                  payload: {
                    code: 'runtime_recovery_failed',
                    message:
                      error instanceof Error
                        ? `Session recovery failed: ${error.message}`
                        : 'Session recovery failed after runtime owner loss.'
                  }
                })
              })
          }
        }
      }

      if (frame.params.type === 'gateway.ready') {
        if (this.gatewayReadyAdvertisesHeartbeat(frame.params.payload)) {
          const socket = this.socket

          if (socket) {
            this.startHeartbeat(socket)
          }
        }

        const ready = frame.params.payload as GatewayReadyPayload | undefined
        const epoch = ready?.replay_epoch
        const runtimeHostId = ready?.runtime_host_id
        let hostChanged = false

        if (typeof runtimeHostId === 'string' && runtimeHostId) {
          if (this.runtimeHostId && this.runtimeHostId !== runtimeHostId) {
            hostChanged = true
            this.runtimeHostRecoveryPending = true
            this.lastSeenSeq.clear()
            this.replayHold = null

            for (const sessionId of this.trackedSessions.keys()) {
              this.options.onDurableResyncRequired?.({
                reason: 'runtime_host_changed',
                session_id: sessionId
              })
            }
          }

          this.runtimeHostId = runtimeHostId
        }

        if (typeof epoch === 'string' && epoch) {
          this.adoptReplayEpoch(epoch, !hostChanged)
        }

        if (this.options.clientAttachment) {
          void this.negotiateReady(frame.params.payload)
        }

        if (hostChanged && !this.negotiatingConnect) {
          this.runtimeHostRecoveryPending = false
          void this.reconstructTrackedSessionsAfterHostChange()
        }
      }

      const eventEpoch = frame.params.epoch

      if (typeof eventEpoch === 'string' && eventEpoch) {
        // Counter metadata is bounded. A long-lived gateway may rotate its
        // replay epoch without reconnecting; clear stale watermarks before
        // evaluating this event's restarted sequence.
        this.adoptReplayEpoch(eventEpoch)
      }

      const sid = frame.params.session_id
      const seqValue = (frame.params as { seq?: unknown }).seq

      if (this.replayHold && sid && typeof seqValue === 'number' && this.replayHold.has(sid)) {
        // Replay in flight for this session: park the frame; flushReplayHold
        // dispatches it after the replayed gap, gated on seq.
        this.replayHold.get(sid)?.push(frame.params)

        return
      }

      // Presentation owns acknowledgement: only persist the watermark after
      // every registered callback accepted the frame without throwing.
      this.dispatchEvent(frame.params)
      this.recordSeq(frame.params)
    }
  }

  /**
   * Track each session's last observed event seq. Events without a seq
   * (legacy backend, session-less globals) leave the map untouched.
   */
  private recordSeq(event: GatewayEvent): void {
    const sid = event.session_id
    const seq = (event as { seq?: unknown }).seq

    if (!sid || typeof seq !== 'number' || !Number.isFinite(seq)) {
      return
    }

    const prev = this.lastSeenSeq.get(sid) ?? 0

    if (seq > prev) {
      this.lastSeenSeq.set(sid, seq)
    }
  }

  /** Test/telemetry hook: current last-seen seq map snapshot. */
  getSeqWatermarks(): Record<string, number> {
    return Object.fromEntries(this.lastSeenSeq)
  }

  /**
   * After a reconnect, ask the gateway to replay every event newer than our
   * per-session watermarks. Replayed frames go through the SAME dispatchEvent
   * path as live frames — dedupe happens naturally because recordSeq ignores
   * non-increasing seqs and downstream stores key on event identity.
   * Best-effort: failures are swallowed (the next reconnect retries).
   */
  private async fetchReplay(): Promise<void> {
    if (this.replayInFlight || this.lastSeenSeq.size === 0) {
      return
    }

    this.replayInFlight = true
    // Park live frames for the sessions we're about to replay so a frame
    // racing the replay response can't dispatch ahead of (or duplicate) the
    // gap events. Sessions without watermarks are unaffected.
    const hold = new Map<string, GatewayEvent[]>()

    for (const sid of this.lastSeenSeq.keys()) {
      hold.set(sid, [])
    }

    this.replayHold = hold

    try {
      const entries = Object.entries(this.getSeqWatermarks())

      // One RPC per known session keeps params flat; sessions are few (<20).
      const results = await Promise.allSettled(
        entries.map(([sid, lastSeen]) =>
          this.request<{
            events?: Array<{ type: string; session_id?: string; seq?: number; payload?: unknown }>
            truncated?: boolean
          }>('session.events.since', { session_id: sid, last_seen: lastSeen }, REPLAY_REQUEST_TIMEOUT_MS)
        )
      )

      for (const [index, result] of results.entries()) {
        if (result.status !== 'fulfilled' || !Array.isArray(result.value?.events)) {
          continue
        }

        const [sessionId] = entries[index]

        if (result.value.truncated === true) {
          this.lastSeenSeq.delete(sessionId)
          const durableSessionId = this.trackedSessions.get(sessionId)?.durableSessionId
          this.options.onDurableResyncRequired?.({
            ...(durableSessionId ? { durable_session_id: durableSessionId } : {}),
            reason: 'replay_truncated',
            session_id: sessionId
          })

          continue
        }

        const epoch = (result.value as { epoch?: unknown }).epoch

        if (typeof epoch === 'string' && epoch && this.replayEpoch && epoch !== this.replayEpoch) {
          // Backend restarted: its seq numbering reset, so our watermarks —
          // and this replay window — are meaningless. Drop them and start
          // fresh under the new epoch.
          this.adoptReplayEpoch(epoch)

          continue
        }

        if (typeof epoch === 'string' && epoch && !this.replayEpoch) {
          this.replayEpoch = epoch
        }

        for (const event of result.value.events) {
          if (!event?.type) {
            continue
          }

          this.dispatchIfNewer(event as GatewayEvent)
        }
      }
    } catch {
      // Replay is an optimization over lossy-reconnect; never surface errors.
    } finally {
      this.flushReplayHold()
      this.replayInFlight = false
    }
  }

  /**
   * Dispatch an event only when its seq advances the session watermark.
   * Seq-less events always dispatch (no ordering contract to violate).
   */
  private dispatchIfNewer(event: GatewayEvent): void {
    const sid = event.session_id
    const seq = (event as { seq?: unknown }).seq

    if (sid && typeof seq === 'number' && Number.isFinite(seq)) {
      const prev = this.lastSeenSeq.get(sid) ?? 0

      if (seq <= prev) {
        return
      }

      // A replay callback may fail while applying the event to presentation
      // state. Keep the previous durable watermark so a reconnect can retry it.
      this.dispatchEvent(event)
      this.lastSeenSeq.set(sid, seq)

      return
    }

    this.dispatchEvent(event)
  }

  /**
   * Record the server's replay epoch; on change (backend restart) the old
   * seq watermarks describe a numbering that no longer exists — clear them
   * so the next reconnect doesn't silently believe it missed nothing.
   */
  private adoptReplayEpoch(epoch: string, notify = true): void {
    if (this.replayEpoch === epoch) {
      return
    }

    if (this.replayEpoch !== null) {
      this.lastSeenSeq.clear()
      this.replayHold = null

      if (notify) {
        for (const sessionId of this.trackedSessions.keys()) {
          this.options.onDurableResyncRequired?.({
            reason: 'replay_epoch_changed',
            session_id: sessionId
          })
        }
      }
    }

    this.replayEpoch = epoch
  }

  /** Release frames parked during a replay fetch, seq-gated against dupes. */
  private flushReplayHold(): void {
    const hold = this.replayHold
    this.replayHold = null

    if (!hold) {
      return
    }

    for (const parked of hold.values()) {
      for (const event of parked) {
        this.dispatchIfNewer(event)
      }
    }
  }

  private gatewayReadyAdvertisesHeartbeat(payload: unknown): boolean {
    return Boolean(payload && typeof payload === 'object' && (payload as { heartbeat?: unknown }).heartbeat === true)
  }

  private startHeartbeat(socket: WebSocketLike): void {
    this.stopHeartbeat()
    this.lastInboundAt = Date.now()

    if (this.options.heartbeatIntervalMs <= 0 || this.options.heartbeatDeadlineMs <= 0) {
      return
    }

    this.heartbeatTimer = setInterval(() => {
      if (this.socket !== socket || socket.readyState !== WebSocket.OPEN) {
        return
      }

      if (Date.now() - this.lastInboundAt >= this.options.heartbeatDeadlineMs) {
        this.invalidateSocket(socket, new Error('WebSocket heartbeat acknowledgement timed out'))

        return
      }

      try {
        socket.send(
          JSON.stringify({
            jsonrpc: '2.0',
            id: `heartbeat-${++this.heartbeatSequence}`,
            method: 'gateway.ping',
            params: {}
          })
        )
      } catch (error) {
        this.invalidateSocket(socket, error instanceof Error ? error : new Error(String(error)))
      }
    }, this.options.heartbeatIntervalMs)
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  private invalidateSocket(socket: WebSocketLike, error: Error): void {
    if (this.socket !== socket) {
      return
    }

    this.socket = null
    this.stopHeartbeat()

    try {
      socket.close()
    } catch {
      // The generation was already invalidated; the reconnect owner can redial.
    }

    this.setState('closed')
    this.rejectAllPending(error)
  }

  private clearPending(id: GatewayRequestId): void {
    const call = this.pending.get(id)

    if (call?.timer) {
      clearTimeout(call.timer)
    }

    this.pending.delete(id)
  }

  private dispatchEvent(event: GatewayEvent): void {
    for (const handler of this.eventHandlers.get(event.type) ?? []) {
      handler(event)
    }

    for (const handler of this.eventHandlers.get(ANY) ?? []) {
      handler(event)
    }
  }

  private rejectAllPending(error: Error): void {
    for (const [id, call] of this.pending) {
      if (call.timer) {
        clearTimeout(call.timer)
      }

      call.reject(error)
      this.pending.delete(id)
    }
  }

  private setState(state: ConnectionState): void {
    if (this.state === state) {
      return
    }

    this.state = state

    for (const handler of this.stateHandlers) {
      handler(state)
    }
  }
}
