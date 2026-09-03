import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

interface ListenerEntry {
  callback: (event: any) => void
  once: boolean
}

const { FakeWebSocket } = vi.hoisted(() => {
  class FakeWebSocket {
    static CONNECTING = 0
    static OPEN = 1
    static CLOSING = 2
    static CLOSED = 3
    static instances: FakeWebSocket[] = []

    readyState = FakeWebSocket.CONNECTING
    sent: string[] = []
    readonly url: string
    private listeners = new Map<string, ListenerEntry[]>()

    constructor(url: string) {
      this.url = url
      FakeWebSocket.instances.push(this)
    }

    static reset() {
      FakeWebSocket.instances = []
    }

    addEventListener(type: string, callback: (event: any) => void, options?: unknown) {
      const once =
        typeof options === 'object' &&
        options !== null &&
        'once' in options &&
        Boolean((options as { once?: unknown }).once)

      const entries = this.listeners.get(type) ?? []

      entries.push({ callback, once })
      this.listeners.set(type, entries)
    }

    removeEventListener(type: string, callback: (event: any) => void) {
      const entries = this.listeners.get(type)

      if (!entries) {
        return
      }

      this.listeners.set(
        type,
        entries.filter(entry => entry.callback !== callback)
      )
    }

    send(payload: string) {
      if (this.readyState !== FakeWebSocket.OPEN) {
        throw new Error('socket not open')
      }

      this.sent.push(payload)
    }

    close(code = 1000) {
      if (this.readyState === FakeWebSocket.CLOSED) {
        return
      }

      this.readyState = FakeWebSocket.CLOSED
      this.emit('close', { code })
    }

    open() {
      this.readyState = FakeWebSocket.OPEN
      this.emit('open', {})
    }

    message(data: string) {
      this.emit('message', { data })
    }

    private emit(type: string, event: any) {
      const entries = [...(this.listeners.get(type) ?? [])]

      for (const entry of entries) {
        entry.callback(event)

        if (entry.once) {
          this.removeEventListener(type, entry.callback)
        }
      }
    }
  }

  return { FakeWebSocket }
})

vi.mock('undici', () => ({ WebSocket: FakeWebSocket }))

import {
  GatewayClient,
  INTERACTION_REQUEST_TIMEOUT_MS,
  RECONNECT_BASE_MS,
  RECONNECT_MAX_MS,
  requestTimeoutMs,
  WS_HEARTBEAT_DEAD_MS,
  WS_HEARTBEAT_INTERVAL_MS
} from '../gatewayClient.js'

it('uses a strict timeout only for interaction responses', () => {
  expect(requestTimeoutMs('approval.respond')).toBe(INTERACTION_REQUEST_TIMEOUT_MS)
  expect(requestTimeoutMs('clarify.respond')).toBe(INTERACTION_REQUEST_TIMEOUT_MS)
  expect(requestTimeoutMs('prompt.submit')).toBeGreaterThan(INTERACTION_REQUEST_TIMEOUT_MS)
})

describe('GatewayClient websocket attach mode', () => {
  const originalWebSocket = globalThis.WebSocket
  let originalGatewayUrl: string | undefined
  let originalSidecarUrl: string | undefined

  beforeEach(() => {
    originalGatewayUrl = process.env.HERMES_TUI_GATEWAY_URL
    originalSidecarUrl = process.env.HERMES_TUI_SIDECAR_URL
    FakeWebSocket.reset()
    ;(globalThis as { WebSocket?: unknown }).WebSocket = FakeWebSocket as unknown as typeof WebSocket
  })

  afterEach(() => {
    if (originalGatewayUrl === undefined) {
      delete process.env.HERMES_TUI_GATEWAY_URL
    } else {
      process.env.HERMES_TUI_GATEWAY_URL = originalGatewayUrl
    }

    if (originalSidecarUrl === undefined) {
      delete process.env.HERMES_TUI_SIDECAR_URL
    } else {
      process.env.HERMES_TUI_SIDECAR_URL = originalSidecarUrl
    }

    FakeWebSocket.reset()

    if (originalWebSocket) {
      globalThis.WebSocket = originalWebSocket
    } else {
      delete (globalThis as { WebSocket?: unknown }).WebSocket
    }
  })

  it('waits for websocket open and resolves RPC requests', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    gw.start()
    const gatewaySocket = FakeWebSocket.instances[0]!
    const req = gw.request<{ ok: boolean }>('session.create', { cols: 80 })

    expect(gatewaySocket.sent).toHaveLength(0)
    gatewaySocket.open()
    await vi.waitFor(() => expect(gatewaySocket.sent).toHaveLength(1))

    const frame = JSON.parse(gatewaySocket.sent[0] ?? '{}') as { id: string; method: string }
    expect(frame.method).toBe('session.create')

    gatewaySocket.message(JSON.stringify({ id: frame.id, jsonrpc: '2.0', result: { ok: true } }))
    await expect(req).resolves.toEqual({ ok: true })

    gw.kill()
  })

  it('maps owner loss to the durable id from a production session resume', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()
    const events: any[] = []

    gw.on('event', event => events.push(event))
    gw.start()
    gw.drain()
    await Promise.resolve()
    const socket = FakeWebSocket.instances[0]!
    socket.open()

    const resume = gw.request('session.resume', { session_id: 'stored-session' })
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1))
    const request = JSON.parse(socket.sent[0] ?? '{}') as { id: string }
    socket.message(
      JSON.stringify({
        id: request.id,
        jsonrpc: '2.0',
        result: { session_id: 'live-session', session_key: 'stored-session', resumed: 'stored-session' }
      })
    )
    await resume

    socket.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          type: 'session.runtime_owner_lost',
          payload: { session_ids: ['live-session'] }
        }
      })
    )

    await vi.waitFor(() =>
      expect(events.at(-1)?.payload).toEqual({
        durable_session_ids: ['stored-session'],
        session_ids: ['live-session']
      })
    )
    gw.kill()
  })

  it('maps owner loss to stored_session_id from a production session create', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()
    const events: any[] = []

    gw.on('event', event => events.push(event))
    gw.start()
    gw.drain()
    await Promise.resolve()
    const socket = FakeWebSocket.instances[0]!
    socket.open()

    const create = gw.request('session.create', {})
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1))
    const request = JSON.parse(socket.sent[0] ?? '{}') as { id: string }
    socket.message(
      JSON.stringify({
        id: request.id,
        jsonrpc: '2.0',
        result: { session_id: 'live-created', stored_session_id: 'durable-created' }
      })
    )
    await create

    socket.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          type: 'session.runtime_owner_lost',
          payload: { session_ids: ['live-created'] }
        }
      })
    )

    await vi.waitFor(() =>
      expect(events.at(-1)?.payload).toEqual({
        durable_session_ids: ['durable-created'],
        session_ids: ['live-created']
      })
    )
    gw.kill()
  })

  it('reconstructs durable sessions before readiness after runtime host takeover', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()
    const events: any[] = []

    gw.on('event', event => events.push(event))
    gw.start()
    gw.drain()
    await Promise.resolve()
    const first = FakeWebSocket.instances[0]!
    first.open()

    first.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          type: 'gateway.ready',
          payload: {
            capabilities: ['client.attach'],
            connection_id: 'connection-1',
            replay_epoch: 'epoch-1',
            runtime_host_id: 'host-1'
          }
        }
      })
    )
    await vi.waitFor(() => expect(first.sent).toHaveLength(1))
    const firstAttach = JSON.parse(first.sent[0] ?? '{}') as { id: string }
    first.message(JSON.stringify({ id: firstAttach.id, jsonrpc: '2.0', result: { ok: true } }))
    await vi.waitFor(() => expect(events.filter(event => event.type === 'gateway.ready')).toHaveLength(1))

    const create = gw.request('session.create', {})
    await vi.waitFor(() => expect(first.sent).toHaveLength(2))
    const createFrame = JSON.parse(first.sent[1] ?? '{}') as { id: string }
    first.message(
      JSON.stringify({
        id: createFrame.id,
        jsonrpc: '2.0',
        result: { session_id: 'live-old', stored_session_id: 'durable-session' }
      })
    )
    await create

    gw.start()
    gw.drain()
    await Promise.resolve()
    const second = FakeWebSocket.instances[1]!
    second.open()
    second.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          type: 'gateway.ready',
          payload: {
            capabilities: ['client.attach'],
            connection_id: 'connection-2',
            replay_epoch: 'epoch-2',
            runtime_host_id: 'host-2'
          }
        }
      })
    )
    await vi.waitFor(() => expect(second.sent).toHaveLength(1))
    const secondAttach = JSON.parse(second.sent[0] ?? '{}') as { id: string }
    second.message(JSON.stringify({ id: secondAttach.id, jsonrpc: '2.0', result: { ok: true } }))

    await vi.waitFor(() => expect(second.sent).toHaveLength(2))

    const resumeFrame = JSON.parse(second.sent[1] ?? '{}') as {
      id: string
      method: string
      params: { session_id: string }
    }

    expect(resumeFrame).toMatchObject({
      method: 'session.resume',
      params: { session_id: 'durable-session' }
    })
    expect(events.filter(event => event.type === 'gateway.ready')).toHaveLength(1)

    second.message(
      JSON.stringify({
        id: resumeFrame.id,
        jsonrpc: '2.0',
        result: {
          session_id: 'live-new',
          session_key: 'durable-session',
          resumed: 'durable-session'
        }
      })
    )

    await vi.waitFor(() =>
      expect(events.map(event => event.type).slice(-2)).toEqual(['session.runtime_owner_lost', 'gateway.ready'])
    )
    expect(events.at(-2)?.payload).toEqual({
      durable_session_ids: ['durable-session'],
      recovered_session_ids: ['live-new'],
      session_ids: ['live-old']
    })
    gw.kill()
  })

  it('reattaches a live session with its replay watermark and restores event delivery after same-host reconnect', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()
    const events: any[] = []
    const exits: Array<null | number> = []

    gw.on('event', event => events.push(event))
    gw.on('exit', code => exits.push(code))
    gw.start()
    gw.drain()
    await Promise.resolve()
    const first = FakeWebSocket.instances[0]!
    first.open()
    first.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          type: 'gateway.ready',
          payload: {
            capabilities: ['client.attach', 'session.attach'],
            replay_epoch: 'epoch-1',
            runtime_host_id: 'host-1'
          }
        }
      })
    )
    await vi.waitFor(() => expect(first.sent).toHaveLength(1))
    const firstAttach = JSON.parse(first.sent[0] ?? '{}') as { id: string }
    first.message(JSON.stringify({ id: firstAttach.id, jsonrpc: '2.0', result: { ok: true } }))
    await vi.waitFor(() => expect(events.some(event => event.type === 'gateway.ready')).toBe(true))

    const create = gw.request('session.create', {})
    await vi.waitFor(() => expect(first.sent).toHaveLength(2))
    const createFrame = JSON.parse(first.sent[1] ?? '{}') as { id: string }
    first.message(
      JSON.stringify({
        id: createFrame.id,
        jsonrpc: '2.0',
        result: { session_id: 'live-session', stored_session_id: 'durable-session' }
      })
    )
    await create
    first.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          epoch: 'epoch-1',
          payload: { text: 'before drop' },
          seq: 7,
          session_id: 'live-session',
          type: 'message.delta'
        }
      })
    )

    first.close(1011)
    gw.start()
    await Promise.resolve()
    const second = FakeWebSocket.instances[1]!
    second.open()
    second.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          type: 'gateway.ready',
          payload: {
            capabilities: ['client.attach', 'session.attach'],
            replay_epoch: 'epoch-1',
            runtime_host_id: 'host-1'
          }
        }
      })
    )
    await vi.waitFor(() => expect(second.sent).toHaveLength(1))
    const clientAttach = JSON.parse(second.sent[0] ?? '{}') as { id: string }
    second.message(JSON.stringify({ id: clientAttach.id, jsonrpc: '2.0', result: { ok: true } }))
    await vi.waitFor(() => expect(second.sent).toHaveLength(2))

    const sessionAttach = JSON.parse(second.sent[1] ?? '{}') as {
      id: string
      method: string
      params: { last_seen_seq: number; mode: string; session_id: string }
    }

    expect(sessionAttach).toMatchObject({
      method: 'session.attach',
      params: { last_seen_seq: 7, mode: 'control', session_id: 'live-session' }
    })
    second.message(
      JSON.stringify({
        id: sessionAttach.id,
        jsonrpc: '2.0',
        result: {
          events: [
            {
              epoch: 'epoch-1',
              payload: { text: 'replayed' },
              seq: 8,
              session_id: 'live-session',
              type: 'message.delta'
            }
          ],
          latest_seq: 8,
          replay_epoch: 'epoch-1',
          session_id: 'live-session'
        }
      })
    )

    await vi.waitFor(() => expect(events.some(event => event.payload?.text === 'replayed')).toBe(true))
    expect(exits).toEqual([])
    second.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          epoch: 'epoch-1',
          payload: { text: 'live again' },
          seq: 9,
          session_id: 'live-session',
          type: 'message.delta'
        }
      })
    )
    await vi.waitFor(() =>
      expect(
        events
          .map(event => event.payload?.text)
          .filter(Boolean)
          .slice(-2)
      ).toEqual(['replayed', 'live again'])
    )
    gw.kill()
  })

  it('fails closed without publishing or advancing when session.attach replay is truncated', async () => {
    const gw = new GatewayClient()
    const events: any[] = []

    const internal = gw as unknown as {
      durableSessionIds: Map<string, string>
      request: (method: string, params: Record<string, unknown>, track?: boolean) => Promise<unknown>
      restoreSessionAttachments: (replayEpoch: string) => Promise<void>
      sessionWatermarks: Map<string, { epoch: string; seq: number }>
    }

    gw.on('event', event => events.push(event))
    gw.drain()
    await Promise.resolve()
    internal.durableSessionIds.set('live-session', 'durable-session')
    internal.sessionWatermarks.set('live-session', { epoch: 'epoch-1', seq: 7 })
    internal.request = vi.fn(async () => ({
      events: [
        {
          epoch: 'epoch-1',
          payload: { text: 'partial replay' },
          seq: 12,
          session_id: 'live-session',
          type: 'message.delta'
        }
      ],
      latest_seq: 20,
      replay_epoch: 'epoch-1',
      truncated: true
    }))

    await expect(internal.restoreSessionAttachments('epoch-1')).rejects.toThrow('truncated')
    expect(events).toEqual([])
    expect(internal.sessionWatermarks.get('live-session')).toEqual({ epoch: 'epoch-1', seq: 7 })
    gw.kill()
  })

  it('does not advance past the last replay event accepted by listeners', async () => {
    const gw = new GatewayClient()

    const internal = gw as unknown as {
      durableSessionIds: Map<string, string>
      request: (method: string, params: Record<string, unknown>, track?: boolean) => Promise<unknown>
      restoreSessionAttachments: (replayEpoch: string) => Promise<void>
      sessionWatermarks: Map<string, { epoch: string; seq: number }>
    }

    gw.on('event', () => undefined)
    gw.drain()
    await Promise.resolve()
    internal.durableSessionIds.set('live-session', 'durable-session')
    internal.sessionWatermarks.set('live-session', { epoch: 'epoch-1', seq: 7 })
    internal.request = vi.fn(async () => ({
      events: [
        {
          epoch: 'epoch-1',
          payload: { text: 'accepted replay' },
          seq: 8,
          session_id: 'live-session',
          type: 'message.delta'
        }
      ],
      latest_seq: 10,
      replay_epoch: 'epoch-1',
      truncated: false
    }))

    await internal.restoreSessionAttachments('epoch-1')
    expect(internal.sessionWatermarks.get('live-session')).toEqual({ epoch: 'epoch-1', seq: 8 })
    gw.kill()
  })

  it('advances a session watermark only after event listeners accept the event', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()
    const accepted: any[] = []

    const rejectEvent = () => {
      throw new Error('renderer rejected event')
    }

    gw.start()
    const socket = FakeWebSocket.instances[0]!
    socket.open()
    gw.drain()
    await Promise.resolve()
    gw.on('event', rejectEvent)

    const frame = JSON.stringify({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        epoch: 'epoch-1',
        payload: { text: 'deliver me' },
        seq: 4,
        session_id: 'live-session',
        type: 'message.delta'
      }
    })

    expect(() => socket.message(frame)).toThrow('renderer rejected event')
    gw.off('event', rejectEvent)
    gw.on('event', event => accepted.push(event))
    socket.message(frame)

    expect(accepted.map(event => event.payload?.text)).toEqual(['deliver me'])
    gw.kill()
  })

  it('negotiates a stable TUI identity before publishing gateway readiness', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()
    const events: string[] = []

    gw.on('event', event => events.push(event.type))
    gw.start()
    gw.drain()
    await Promise.resolve()
    const gatewaySocket = FakeWebSocket.instances[0]!

    gatewaySocket.open()
    gatewaySocket.message(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          type: 'gateway.ready',
          payload: {
            capabilities: ['client.attach'],
            connection_id: 'connection-1',
            replay_epoch: 'epoch-1'
          }
        }
      })
    )

    await vi.waitFor(() => expect(gatewaySocket.sent).toHaveLength(1))
    expect(events).not.toContain('gateway.ready')

    const attach = JSON.parse(gatewaySocket.sent[0] ?? '{}') as {
      id: string
      method: string
      params: { client_id: string; protocol_version: number; surface: string }
    }

    expect(attach.method).toBe('client.attach')
    expect(attach.params).toMatchObject({
      client_id: expect.stringMatching(/^tui:/),
      protocol_version: 1,
      surface: 'tui'
    })
    gatewaySocket.message(
      JSON.stringify({
        id: attach.id,
        jsonrpc: '2.0',
        result: {
          capabilities: ['session.observe', 'session.control', 'session.replay'],
          client_id: attach.params.client_id,
          connection_id: 'connection-1',
          idempotent: false,
          protocol_version: 1,
          surface: 'tui'
        }
      })
    )

    await vi.waitFor(() => expect(events).toContain('gateway.ready'))
    gw.kill()
  })

  it('drains buffered events on a later microtask, not synchronously inside drain()', async () => {
    // Regression for #36658: in attach mode the already-running gateway
    // replays `gateway.ready` the instant the socket connects, so it lands in
    // bufferedEvents BEFORE the consumer's mount-time subscribe effect runs.
    // If drain() emitted those synchronously, the gateway.ready handler's
    // setState cascade would run inside React's first commit -> "Too many
    // re-renders" (#301). drain() must defer the buffered flush so the first
    // commit settles first.
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    gw.start()
    const gatewaySocket = FakeWebSocket.instances[0]!

    gatewaySocket.open()
    // Server replays ready BEFORE the consumer subscribes (attach-mode timing):
    gatewaySocket.message(
      JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { type: 'gateway.ready', payload: {} } })
    )

    const order: string[] = []

    gw.on('event', ev => order.push(`event:${ev.type}`))
    gw.drain()
    order.push('after-drain')

    // Buffered event must NOT have fired synchronously inside drain():
    expect(order).toEqual(['after-drain'])

    // ...and must arrive on the next microtask.
    await vi.waitFor(() => expect(order).toContain('event:gateway.ready'))
    expect(order).toEqual(['after-drain', 'event:gateway.ready'])

    gw.kill()
  })

  it('preserves FIFO order when a live event arrives before the deferred flush', async () => {
    // #36658 hardening: `subscribed` must NOT flip synchronously in drain().
    // A live event delivered in the window between drain() returning and the
    // deferred microtask running must still queue BEHIND the chronologically
    // earlier buffered events, not jump ahead of them.
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    gw.start()
    const gatewaySocket = FakeWebSocket.instances[0]!

    gatewaySocket.open()
    // Buffered first (replayed on connect, before subscribe):
    gatewaySocket.message(
      JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { type: 'gateway.ready', payload: {} } })
    )

    const order: string[] = []

    gw.on('event', ev => order.push(ev.type))
    gw.drain()

    // A LIVE event arrives synchronously in the post-drain / pre-microtask gap:
    gatewaySocket.message(
      JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { type: 'session.info', payload: {} } })
    )

    // Nothing emitted yet (subscribed stays false until the microtask):
    expect(order).toEqual([])

    await vi.waitFor(() => expect(order.length).toBe(2))
    // FIFO preserved: the earlier-buffered gateway.ready precedes the live one.
    expect(order).toEqual(['gateway.ready', 'session.info'])

    gw.kill()
  })

  it('mirrors event frames to sidecar websocket when configured', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    process.env.HERMES_TUI_SIDECAR_URL = 'ws://gateway.test/api/pub?token=abc&channel=demo'

    const gw = new GatewayClient()
    const seen: string[] = []

    gw.on('event', ev => seen.push(ev.type))
    gw.start()

    const gatewaySocket = FakeWebSocket.instances[0]!
    gatewaySocket.open()
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2))

    const sidecarSocket = FakeWebSocket.instances[1]!

    sidecarSocket.open()
    gw.drain()
    // drain() flips `subscribed` on a microtask now (#36658); let it settle so
    // the subsequent live event takes the synchronous publish path.
    await Promise.resolve()

    const eventFrame = JSON.stringify({
      jsonrpc: '2.0',
      method: 'event',
      params: { type: 'tool.start', payload: { tool_id: 't1' } }
    })

    gatewaySocket.message(eventFrame)

    expect(seen).toContain('tool.start')
    expect(sidecarSocket.sent).toContain(eventFrame)

    gw.kill()
  })

  it('publishes local dashboard-control events to the sidecar websocket', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    process.env.HERMES_TUI_SIDECAR_URL = 'ws://gateway.test/api/pub?token=abc&channel=demo'

    const gw = new GatewayClient()
    const seen: string[] = []

    gw.on('event', ev => seen.push(ev.type))
    gw.start()

    const gatewaySocket = FakeWebSocket.instances[0]!

    gatewaySocket.open()
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2))

    const sidecarSocket = FakeWebSocket.instances[1]!

    sidecarSocket.open()
    gw.drain()
    // drain() flips `subscribed` on a microtask now (#36658); let it settle.
    await Promise.resolve()

    gw.publishLocalEvent({
      payload: { reason: 'idle_exit_hotkey' },
      session_id: 'sid-old',
      type: 'dashboard.new_session_requested'
    })

    expect(seen).toContain('dashboard.new_session_requested')
    expect(JSON.parse(sidecarSocket.sent.at(-1) ?? '{}')).toEqual({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        payload: { reason: 'idle_exit_hotkey' },
        session_id: 'sid-old',
        type: 'dashboard.new_session_requested'
      }
    })

    gw.kill()
  })

  it('emits exit when attached websocket closes', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()
    const exits: Array<null | number> = []

    gw.on('exit', code => exits.push(code))
    gw.start()

    const gatewaySocket = FakeWebSocket.instances[0]!

    gatewaySocket.open()
    gw.drain()
    // drain() flips `subscribed` on a microtask now (#36658); let it settle so
    // the close below takes the synchronous exit path.
    await Promise.resolve()
    gatewaySocket.close(1011)

    expect(exits).toEqual([1011])
    expect(gw.getLogTail(20)).toContain('[lifecycle] websocket close code=1011')
    expect(gw.getLogTail(20)).toContain('[lifecycle] transport exit code=1011')
  })

  it('rejects pending RPCs with websocket wording when the attached socket closes', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    gw.start()
    const gatewaySocket = FakeWebSocket.instances[0]!

    gatewaySocket.open()
    gw.drain()

    const req = gw.request('session.create', {})
    await vi.waitFor(() => expect(gatewaySocket.sent.length).toBeGreaterThan(0))

    gatewaySocket.close(1011)

    await expect(req).rejects.toThrow(/gateway websocket closed \(1011\)/)
  })

  it('rejects pending RPCs when kill() closes the attached websocket', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    gw.start()
    const gatewaySocket = FakeWebSocket.instances[0]!

    gatewaySocket.open()
    gw.drain()

    const req = gw.request('session.create', {})
    await vi.waitFor(() => expect(gatewaySocket.sent.length).toBeGreaterThan(0))

    gw.kill('test.shutdown')

    await expect(req).rejects.toThrow(/gateway closed/)
    expect(gw.getLogTail(20)).toContain('[lifecycle] GatewayClient.kill reason=test.shutdown')
  })

  it('reattaches when HERMES_TUI_GATEWAY_URL rotates between requests', async () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway-old.test/api/ws?token=abc'
    const gw = new GatewayClient()

    gw.start()
    const firstSocket = FakeWebSocket.instances[0]!

    firstSocket.open()
    gw.drain()

    const stale = gw.request('session.create', {})
    await vi.waitFor(() => expect(firstSocket.sent.length).toBeGreaterThan(0))

    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway-new.test/api/ws?token=xyz'
    const next = gw.request('session.create', {})

    await expect(stale).rejects.toThrow(/gateway attach url changed/)
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2))

    const secondSocket = FakeWebSocket.instances[1]!
    expect(secondSocket.url).toContain('gateway-new.test')

    secondSocket.open()
    await vi.waitFor(() => expect(secondSocket.sent.length).toBeGreaterThan(0))

    const frame = JSON.parse(secondSocket.sent[0] ?? '{}') as { id: string }
    secondSocket.message(JSON.stringify({ id: frame.id, jsonrpc: '2.0', result: { ok: true } }))

    await expect(next).resolves.toEqual({ ok: true })
    gw.kill()
  })

  it('uses the undici WebSocket fallback when global WebSocket is unavailable', () => {
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=hunter2&channel=secret'
    delete (globalThis as { WebSocket?: unknown }).WebSocket

    const gw = new GatewayClient()

    gw.start()
    expect(FakeWebSocket.instances).toHaveLength(1)
    expect(FakeWebSocket.instances[0]?.url).toBe('ws://gateway.test/api/ws?token=hunter2&channel=secret')

    gw.kill()
  })

  it('redacts attach URL secrets when the WebSocket constructor throws', () => {
    const secretUrl = 'ws://gateway.test/api/ws?token=hunter2&channel=secret'

    process.env.HERMES_TUI_GATEWAY_URL = secretUrl
    ;(globalThis as { WebSocket?: unknown }).WebSocket = class ThrowingWebSocket extends FakeWebSocket {
      constructor(url: string) {
        throw new TypeError(`Invalid URL: ${url}`)
      }
    } as unknown as typeof WebSocket

    const gw = new GatewayClient()

    gw.start()
    gw.drain()

    const tail = gw.getLogTail(20)
    expect(tail).not.toContain('hunter2')
    expect(tail).not.toContain('channel=secret')
    expect(tail).not.toContain(secretUrl)
    expect(tail).toContain('ws://gateway.test/api/ws?***')

    gw.kill()
  })

  it('redacts sidecar URL secrets when the WebSocket constructor throws', async () => {
    const sidecarUrl = 'ws://gateway.test/api/pub?token=hunter2&channel=secret'

    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    process.env.HERMES_TUI_SIDECAR_URL = sidecarUrl
    ;(globalThis as { WebSocket?: unknown }).WebSocket = class ThrowingSidecarWebSocket extends FakeWebSocket {
      constructor(url: string) {
        if (url.includes('/api/pub')) {
          throw new TypeError(`Invalid URL: ${url}`)
        }

        super(url)
      }
    } as unknown as typeof WebSocket

    const gw = new GatewayClient()

    gw.start()
    const gatewaySocket = FakeWebSocket.instances[0]!
    gatewaySocket.open()
    await vi.waitFor(() => expect(gw.getLogTail(20)).toContain('[sidecar] failed to connect'))

    const tail = gw.getLogTail(20)
    expect(tail).not.toContain('hunter2')
    expect(tail).not.toContain('channel=secret')
    expect(tail).not.toContain(sidecarUrl)
    expect(tail).toContain('ws://gateway.test/api/pub?***')

    gw.kill()
  })

  it('redacts user-info credentials even on URLs the WHATWG parser rejects', () => {
    // Port 99999 is outside the WHATWG URL parser's valid 0–65535
    // range and survives `.trim()`, so the fixture deterministically
    // exercises `redactUrl()`'s fallback branch across Node versions.
    // (An earlier `%zz` user-info fixture did NOT actually throw in
    // recent Node — WHATWG accepts malformed percent escapes there —
    // which silently routed the test through the structured-URL path.)
    const fixture = 'ws://alice:hunter2@gateway.test:99999/api/ws?token=secret'
    expect(() => new URL(fixture)).toThrow()

    process.env.HERMES_TUI_GATEWAY_URL = fixture
    ;(globalThis as { WebSocket?: unknown }).WebSocket = class ThrowingWebSocket extends FakeWebSocket {
      constructor(url: string) {
        throw new TypeError(`Invalid URL: ${url}`)
      }
    } as unknown as typeof WebSocket

    const gw = new GatewayClient()

    gw.start()
    gw.drain()

    const tail = gw.getLogTail(20)
    expect(tail).not.toContain('alice')
    expect(tail).not.toContain('hunter2')
    expect(tail).not.toContain('token=secret')

    gw.kill()
  })

  it('keeps a healthy idle websocket open when heartbeat acknowledgements arrive (issue #32997)', async () => {
    vi.useFakeTimers()
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    try {
      gw.start()
      const socket = FakeWebSocket.instances[0]!

      socket.open()
      socket.message(
        JSON.stringify({
          jsonrpc: '2.0',
          method: 'event',
          params: { type: 'gateway.ready', payload: { heartbeat: true } }
        })
      )
      await vi.advanceTimersByTimeAsync(WS_HEARTBEAT_INTERVAL_MS)

      const heartbeat = JSON.parse(socket.sent.at(-1) ?? '{}') as { id: string; method: string }

      expect(heartbeat.method).toBe('gateway.ping')
      socket.message(JSON.stringify({ id: heartbeat.id, jsonrpc: '2.0', result: { ok: true } }))

      await vi.advanceTimersByTimeAsync(WS_HEARTBEAT_DEAD_MS + WS_HEARTBEAT_INTERVAL_MS)
      expect(socket.readyState).toBe(FakeWebSocket.OPEN)
      expect(FakeWebSocket.instances).toHaveLength(1)
    } finally {
      gw.kill()
      vi.useRealTimers()
    }
  })

  it('auto-reconnects after a missing heartbeat acknowledgement (issue #32997)', async () => {
    vi.useFakeTimers()
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    try {
      gw.start()
      const first = FakeWebSocket.instances[0]!

      first.open()
      first.message(
        JSON.stringify({
          jsonrpc: '2.0',
          method: 'event',
          params: { type: 'gateway.ready', payload: { heartbeat: true } }
        })
      )
      await vi.advanceTimersByTimeAsync(WS_HEARTBEAT_INTERVAL_MS)
      expect(JSON.parse(first.sent.at(-1) ?? '{}')).toMatchObject({ method: 'gateway.ping' })
      await vi.advanceTimersByTimeAsync(WS_HEARTBEAT_DEAD_MS + WS_HEARTBEAT_INTERVAL_MS)
      await vi.advanceTimersByTimeAsync(RECONNECT_BASE_MS)
      expect(FakeWebSocket.instances.length).toBeGreaterThanOrEqual(2)
    } finally {
      gw.kill()
      vi.useRealTimers()
    }
  })

  it('does not heartbeat an older backend that omits the capability', async () => {
    vi.useFakeTimers()
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    try {
      gw.start()
      const socket = FakeWebSocket.instances[0]!

      socket.open()
      socket.message(
        JSON.stringify({
          jsonrpc: '2.0',
          method: 'event',
          params: { type: 'gateway.ready', payload: {} }
        })
      )
      await vi.advanceTimersByTimeAsync(WS_HEARTBEAT_DEAD_MS + WS_HEARTBEAT_INTERVAL_MS)
      expect(socket.readyState).toBe(FakeWebSocket.OPEN)
      expect(socket.sent).toEqual([])
      expect(FakeWebSocket.instances).toHaveLength(1)
    } finally {
      gw.kill()
      vi.useRealTimers()
    }
  })

  it('does not double-reconnect when the exit subscriber restarts immediately', async () => {
    vi.useFakeTimers()
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()

    try {
      gw.on('exit', () => gw.start())
      gw.start()
      const first = FakeWebSocket.instances[0]!

      first.open()
      gw.drain()
      await Promise.resolve()
      first.close(1011)

      expect(FakeWebSocket.instances).toHaveLength(2)
      await vi.advanceTimersByTimeAsync(RECONNECT_BASE_MS)
      expect(FakeWebSocket.instances).toHaveLength(2)
    } finally {
      gw.kill()
      vi.useRealTimers()
    }
  })

  it('does not auto-reconnect after an intentional kill() (issue #32997)', async () => {
    vi.useFakeTimers()
    process.env.HERMES_TUI_GATEWAY_URL = 'ws://gateway.test/api/ws?token=abc'
    const gw = new GatewayClient()
    gw.start()
    FakeWebSocket.instances[0]!.open()
    gw.kill() // sets disposed
    await vi.advanceTimersByTimeAsync(WS_HEARTBEAT_DEAD_MS + RECONNECT_MAX_MS + 1000)
    expect(FakeWebSocket.instances.length).toBe(1) // no reconnect attempted
    vi.useRealTimers()
  })
})
