import { beforeEach, describe, expect, it, vi } from 'vitest'

import { type GatewayClientOptions, JsonRpcGatewayClient } from './json-rpc-gateway'

/**
 * Minimal EventTarget-based WebSocket stand-in so the seq-tracking and
 * replay-resume logic can be driven with real dispatch semantics.
 */
class FakeWebSocket extends EventTarget {
  static OPEN = 1
  static instances: FakeWebSocket[] = []

  readyState = 0
  sent: string[] = []
  url: string

  constructor(url: string) {
    super()
    this.url = url
    FakeWebSocket.instances.push(this)
  }

  send(data: string): void {
    this.sent.push(data)
  }

  close(): void {
    this.readyState = 3
    this.dispatchEvent(new Event('close'))
  }

  // Test drivers
  open(): void {
    this.readyState = 1
    this.dispatchEvent(new Event('open'))
  }

  serverFrame(obj: unknown): void {
    this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(obj) }))
  }

  lastRequest(): { id: string; method: string; params: Record<string, unknown> } {
    const last = this.sent[this.sent.length - 1]

    return JSON.parse(last ?? '{}')
  }
}

let sockets: FakeWebSocket[]

const makeClient = (options: GatewayClientOptions = {}) => {
  const client = new JsonRpcGatewayClient({
    ...options,
    socketFactory: url => new FakeWebSocket(url) as unknown as WebSocket,
    heartbeatIntervalMs: 0,
    heartbeatDeadlineMs: 0,
    connectTimeoutMs: 1000
  })

  return client
}

describe('JsonRpcGatewayClient event-seq tracking + replay resume', () => {
  beforeEach(() => {
    FakeWebSocket.instances = []
    sockets = FakeWebSocket.instances as unknown as FakeWebSocket[]
  })

  it('does not publish open before identity negotiation completes', async () => {
    const client = makeClient({
      clientAttachment: {
        client_id: 'state-gated-client',
        protocol_version: 1,
        surface: 'desktop',
        capabilities: ['session.observe']
      }
    })

    const states: string[] = []
    client.onState(state => states.push(state))

    const connected = client.connect('ws://x')
    const socket = sockets[0]
    socket.open()
    await Promise.resolve()

    expect(client.connectionState).toBe('connecting')
    expect(states).not.toContain('open')

    socket.serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'gateway.ready',
        payload: {
          connection_id: 'connection-state-gated',
          multi_client: {
            protocol_version: 1,
            attachment_modes: ['observe', 'control'],
            methods: ['client.attach']
          }
        }
      }
    })
    await vi.waitFor(() => expect(socket.lastRequest().method).toBe('client.attach'))
    const request = socket.lastRequest()
    socket.serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['session.observe'],
        client_id: 'state-gated-client',
        connection_id: 'connection-state-gated',
        idempotent: false,
        protocol_version: 1,
        surface: 'desktop'
      }
    })

    await connected
    expect(client.connectionState).toBe('open')
    expect(states).toEqual(['idle', 'connecting', 'open'])
    client.close()
  })

  it('waits for ready and negotiates identity before connect resolves', async () => {
    const client = makeClient({
      clientAttachment: {
        client_id: 'stable-desktop',
        protocol_version: 1,
        surface: 'desktop',
        capabilities: ['session.observe', 'session.control', 'session.replay']
      }
    })

    let resolved = false

    const connected = client.connect('ws://x').then(() => {
      resolved = true
    })

    sockets[0].open()
    await Promise.resolve()
    expect(resolved).toBe(false)
    expect(sockets[0].sent).toHaveLength(0)

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'gateway.ready',
        payload: {
          connection_id: 'connection-1',
          replay_epoch: 'epoch-1',
          runtime_host_id: 'host-1',
          multi_client: {
            protocol_version: 1,
            attachment_modes: ['observe', 'control'],
            methods: ['client.attach', 'session.attach', 'session.events.since']
          }
        }
      }
    })
    await vi.waitFor(() => expect(sockets[0].lastRequest().method).toBe('client.attach'))
    expect(resolved).toBe(false)
    const request = sockets[0].lastRequest()
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['session.observe', 'session.control', 'session.replay'],
        client_id: 'stable-desktop',
        connection_id: 'connection-1',
        idempotent: false,
        protocol_version: 1,
        surface: 'desktop'
      }
    })

    await connected
    expect(resolved).toBe(true)
    client.close()
  })

  it('reattaches tracked sessions after identity negotiation on reconnect', async () => {
    const client = makeClient({
      clientAttachment: {
        client_id: 'stable-web',
        protocol_version: 1,
        surface: 'web',
        capabilities: ['session.observe', 'session.control', 'session.replay']
      }
    })

    const ready = (socket: FakeWebSocket, connectionId: string) => {
      socket.serverFrame({
        jsonrpc: '2.0',
        method: 'event',
        params: {
          type: 'gateway.ready',
          payload: {
            connection_id: connectionId,
            replay_epoch: 'epoch-1',
            runtime_host_id: 'host-1',
            multi_client: {
              protocol_version: 1,
              attachment_modes: ['observe', 'control'],
              methods: ['client.attach', 'session.attach', 'session.events.since']
            }
          }
        }
      })
    }

    const answerClientAttach = (socket: FakeWebSocket) => {
      const request = socket.lastRequest()
      socket.serverFrame({
        jsonrpc: '2.0',
        id: request.id,
        result: {
          capabilities: ['session.observe', 'session.control', 'session.replay'],
          client_id: 'stable-web',
          connection_id: 'connection',
          idempotent: false,
          protocol_version: 1,
          surface: 'web'
        }
      })
    }

    const first = client.connect('ws://x')
    let socket = sockets.at(-1)!
    socket.open()
    ready(socket, 'connection-1')
    await vi.waitFor(() => expect(socket.lastRequest().method).toBe('client.attach'))
    answerClientAttach(socket)
    await first

    const initialAttach = client.attachSession({
      session_id: 'session-1',
      mode: 'observe',
      last_seen_seq: 0
    })

    let request = socket.lastRequest()
    socket.serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['observe'],
        client_id: 'stable-web',
        epoch: 'epoch-1',
        events: [],
        latest_seq: 0,
        mode: 'observe',
        replay_epoch: 'epoch-1',
        session_id: 'session-1',
        truncated: false
      }
    })
    await initialAttach
    socket.serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: { type: 'message.delta', session_id: 'session-1', seq: 5, epoch: 'epoch-1' }
    })

    client.invalidate('drop')
    let reconnected = false

    const second = client.connect('ws://x').then(() => {
      reconnected = true
    })

    socket = sockets.at(-1)!
    socket.open()
    ready(socket, 'connection-2')
    await vi.waitFor(() => expect(socket.lastRequest().method).toBe('client.attach'))
    answerClientAttach(socket)
    await vi.waitFor(() => expect(socket.lastRequest().method).toBe('session.attach'))
    expect(reconnected).toBe(false)
    request = socket.lastRequest()
    expect(request.params).toMatchObject({
      session_id: 'session-1',
      mode: 'observe',
      last_seen_seq: 5
    })
    socket.serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['observe'],
        client_id: 'stable-web',
        epoch: 'epoch-1',
        events: [],
        latest_seq: 5,
        mode: 'observe',
        replay_epoch: 'epoch-1',
        session_id: 'session-1',
        truncated: false
      }
    })

    await second
    expect(reconnected).toBe(true)
    client.close()
  })

  it('requests durable reconstruction when attach replay is truncated', async () => {
    const resync = vi.fn()
    const client = makeClient({ onDurableResyncRequired: resync })
    const connected = client.connect('ws://x')
    sockets[0].open()
    await connected

    const attached = client.attachSession({
      session_id: 'truncated-session',
      mode: 'observe',
      last_seen_seq: 42
    })

    const request = sockets[0].lastRequest()

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['observe'],
        client_id: 'stable-client',
        epoch: 'epoch-1',
        events: [],
        latest_seq: 90,
        mode: 'observe',
        replay_epoch: 'epoch-1',
        session_id: 'truncated-session',
        truncated: true
      }
    })
    await attached

    expect(resync).toHaveBeenCalledOnce()
    expect(resync).toHaveBeenCalledWith({
      reason: 'replay_truncated',
      session_id: 'truncated-session'
    })
    client.close()
  })

  it('requests durable reconstruction when the canonical runtime owner is lost', async () => {
    const resync = vi.fn()
    const client = makeClient({ onDurableResyncRequired: resync })
    const connected = client.connect('ws://x')
    sockets[0].open()
    await connected

    const attached = client.attachSession({ session_id: 'owner-session', mode: 'observe' })
    const request = sockets[0].lastRequest()
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['observe'],
        client_id: 'stable-client',
        epoch: 'epoch-1',
        events: [],
        latest_seq: 0,
        mode: 'observe',
        replay_epoch: 'epoch-1',
        session_id: 'owner-session',
        truncated: false
      }
    })
    await attached
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'message.delta',
        session_id: 'owner-session',
        seq: 9,
        epoch: 'epoch-1'
      }
    })
    expect(client.getSeqWatermarks()).toEqual({ 'owner-session': 9 })

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'session.runtime_owner_lost',
        payload: { session_ids: ['owner-session', 'not-tracked'] }
      }
    })

    expect(client.getSeqWatermarks()).toEqual({})
    expect(resync).toHaveBeenCalledOnce()
    expect(resync).toHaveBeenCalledWith({
      reason: 'runtime_owner_lost',
      session_id: 'owner-session'
    })
    client.close()
  })

  it('reconstructs a production resume from its durable id after owner loss', async () => {
    const resync = vi.fn()
    const client = makeClient({ onDurableResyncRequired: resync })
    const connected = client.connect('ws://x')
    sockets[0].open()
    await connected

    const initial = client.request<{ session_id: string; session_key: string }>('session.resume', {
      session_id: 'stored-session'
    })

    let request = sockets[0].lastRequest()
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: { session_id: 'live-before-loss', session_key: 'stored-session', resumed: 'stored-session' }
    })
    await initial

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'session.runtime_owner_lost',
        payload: { session_ids: ['live-before-loss'] }
      }
    })

    await vi.waitFor(() => {
      request = sockets[0].lastRequest()
      expect(request.method).toBe('session.resume')
      expect(request.params).toEqual({ session_id: 'stored-session' })
    })
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: { session_id: 'live-after-loss', session_key: 'stored-session', resumed: 'stored-session' }
    })

    await vi.waitFor(() => {
      expect(resync).toHaveBeenCalledWith({
        durable_session_id: 'stored-session',
        reason: 'runtime_owner_lost',
        session_id: 'live-before-loss'
      })
    })
    client.close()
  })

  it('reconstructs a production create from stored_session_id after owner loss', async () => {
    const client = makeClient()
    const connected = client.connect('ws://x')
    sockets[0].open()
    await connected

    const initial = client.request('session.create', {})
    let request = sockets[0].lastRequest()
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: { session_id: 'live-created', stored_session_id: 'durable-created' }
    })
    await initial

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'session.runtime_owner_lost',
        payload: { session_ids: ['live-created'] }
      }
    })

    await vi.waitFor(() => {
      request = sockets[0].lastRequest()
      expect(request.method).toBe('session.resume')
      expect(request.params).toEqual({ session_id: 'durable-created' })
    })
    client.close()
  })

  it('surfaces production owner-loss reconstruction failure', async () => {
    const client = makeClient()
    const errors = vi.fn()
    client.on('error', errors)
    const connected = client.connect('ws://x')
    sockets[0].open()
    await connected

    const initial = client.request('session.resume', { session_id: 'stored-session' })
    let request = sockets[0].lastRequest()
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: { session_id: 'live-before-loss', session_key: 'stored-session' }
    })
    await initial

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'session.runtime_owner_lost',
        payload: { session_ids: ['live-before-loss'] }
      }
    })

    await vi.waitFor(() => {
      request = sockets[0].lastRequest()
      expect(request.method).toBe('session.resume')
    })
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      error: { code: -32072, message: 'takeover failed' }
    })

    await vi.waitFor(() => {
      expect(errors).toHaveBeenCalledWith(expect.objectContaining({
        type: 'error',
        session_id: 'live-before-loss',
        payload: expect.objectContaining({ code: 'runtime_recovery_failed' })
      }))
    })
    client.close()
  })

  it('requests durable reconstruction when runtime host changes', async () => {
    const resync = vi.fn()
    const client = makeClient({ onDurableResyncRequired: resync })
    const connected = client.connect('ws://x')
    sockets[0].open()
    await connected

    const attached = client.attachSession({ session_id: 'host-session', mode: 'observe' })
    const request = sockets[0].lastRequest()

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['observe'],
        client_id: 'stable-client',
        epoch: 'epoch-1',
        events: [],
        latest_seq: 0,
        mode: 'observe',
        replay_epoch: 'epoch-1',
        session_id: 'host-session',
        truncated: false
      }
    })
    await attached
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'gateway.ready',
        payload: { replay_epoch: 'epoch-1', runtime_host_id: 'host-a' }
      }
    })
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: { type: 'message.delta', session_id: 'host-session', seq: 9, epoch: 'epoch-1' }
    })
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'gateway.ready',
        payload: { replay_epoch: 'epoch-1', runtime_host_id: 'host-b' }
      }
    })

    expect(client.getSeqWatermarks()).toEqual({})
    expect(resync).toHaveBeenCalledWith({
      reason: 'runtime_host_changed',
      session_id: 'host-session'
    })

    resync.mockClear()
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'gateway.ready',
        payload: { replay_epoch: 'epoch-2', runtime_host_id: 'host-b' }
      }
    })
    expect(resync).toHaveBeenCalledOnce()
    expect(resync).toHaveBeenCalledWith({
      reason: 'replay_epoch_changed',
      session_id: 'host-session'
    })
    client.close()
  })

  it('reconstructs durable sessions before attachment after runtime host takeover', async () => {
    const client = makeClient({
      clientAttachment: {
        client_id: 'stable-web',
        protocol_version: 1,
        surface: 'web'
      }
    })

    const connectFirst = client.connect('ws://x')
    sockets[0].open()
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'gateway.ready',
        payload: {
          connection_id: 'first',
          replay_epoch: 'epoch-a',
          runtime_host_id: 'host-a',
          multi_client: {
            protocol_version: 1,
            attachment_modes: ['observe', 'control'],
            methods: ['client.attach', 'session.attach', 'session.events.since']
          }
        }
      }
    })
    await vi.waitFor(() => expect(sockets[0].lastRequest().method).toBe('client.attach'))
    let request = sockets[0].lastRequest()
    sockets[0].serverFrame({ jsonrpc: '2.0', id: request.id, result: { client_id: 'stable-web' } })
    await connectFirst

    const initial = client.request('session.resume', { session_id: 'stored-session' })
    request = sockets[0].lastRequest()
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: { session_id: 'live-old', session_key: 'stored-session' }
    })
    await initial
    client.close()

    const connectSecond = client.connect('ws://x')
    sockets[1].open()
    sockets[1].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: {
        type: 'gateway.ready',
        payload: {
          connection_id: 'second',
          replay_epoch: 'epoch-b',
          runtime_host_id: 'host-b',
          multi_client: {
            protocol_version: 1,
            attachment_modes: ['observe', 'control'],
            methods: ['client.attach', 'session.attach', 'session.events.since']
          }
        }
      }
    })
    await vi.waitFor(() => expect(sockets[1].lastRequest().method).toBe('client.attach'))
    request = sockets[1].lastRequest()
    sockets[1].serverFrame({ jsonrpc: '2.0', id: request.id, result: { client_id: 'stable-web' } })

    await vi.waitFor(() => expect(sockets[1].lastRequest().method).toBe('session.resume'))
    request = sockets[1].lastRequest()
    expect(request.params).toEqual({ session_id: 'stored-session' })
    sockets[1].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: { session_id: 'live-new', session_key: 'stored-session' }
    })

    await vi.waitFor(() => expect(sockets[1].lastRequest().method).toBe('session.attach'))
    request = sockets[1].lastRequest()
    expect(request.params.session_id).toBe('live-new')
    sockets[1].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['observe'],
        client_id: 'stable-web',
        epoch: 'epoch-b',
        events: [],
        latest_seq: 0,
        mode: 'control',
        replay_epoch: 'epoch-b',
        session_id: 'live-new',
        truncated: false
      }
    })
    await connectSecond
    client.close()
  })

  it('sends a typed client attachment handshake', async () => {
    const client = makeClient()
    const connected = client.connect('ws://x')
    sockets[0].open()
    await connected

    const attached = client.attachClient({
      client_id: 'desktop-window-7',
      protocol_version: 1,
      surface: 'desktop',
      capabilities: ['session.observe', 'session.control', 'session.replay']
    })

    const request = sockets[0].lastRequest()
    expect(request).toMatchObject({
      method: 'client.attach',
      params: {
        client_id: 'desktop-window-7',
        protocol_version: 1,
        surface: 'desktop'
      }
    })
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      id: request.id,
      result: {
        capabilities: ['session.observe', 'session.control', 'session.replay'],
        client_id: 'desktop-window-7',
        connection_id: 'connection-1',
        idempotent: false,
        protocol_version: 1,
        surface: 'desktop'
      }
    })

    await expect(attached).resolves.toMatchObject({
      client_id: 'desktop-window-7',
      connection_id: 'connection-1'
    })
    client.close()
  })

  it('records per-session seq watermarks from live events', async () => {
    const client = makeClient()
    const p = client.connect('ws://x')
    sockets[0].open()
    await p

    sockets[0].serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 4 } })
    sockets[0].serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 2 } }) // out of order / late
    sockets[0].serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'tool.start', session_id: 's2', seq: 9 } })
    sockets[0].serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'skin.changed' } }) // no sid/seq

    expect(client.getSeqWatermarks()).toEqual({ s1: 4, s2: 9 })
    client.close()
  })

  it('resets watermarks when a live event starts a new replay epoch', async () => {
    const client = makeClient()
    const p = client.connect('ws://x')
    sockets[0].open()
    await p

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: { type: 'gateway.ready', payload: { replay_epoch: 'epoch-a' } }
    })
    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: { type: 'message.delta', session_id: 's1', seq: 10, epoch: 'epoch-a' }
    })
    expect(client.getSeqWatermarks()).toEqual({ s1: 10 })

    sockets[0].serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: { type: 'message.delta', session_id: 's1', seq: 1, epoch: 'epoch-b' }
    })

    expect(client.getSeqWatermarks()).toEqual({ s1: 1 })
    client.close()
  })

  it('fetches replay on reconnect for sessions it has watermarks for', async () => {
    const client = makeClient()

    const first = client.connect('ws://x')
    let sock = sockets[sockets.length - 1]
    sock.open()
    await first

    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.start', session_id: 's1', seq: 1 } })
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 5 } })

    // Drop and reconnect.
    client.invalidate('drop')
    const second = client.connect('ws://x')
    sock = sockets[sockets.length - 1]
    sock.open()
    await second

    // The reconnect triggered a replay fetch — flush microtasks.
    await vi.waitFor(() => {
      const req = sock.lastRequest()
      expect(req.method).toBe('session.events.since')
      expect(req.params).toMatchObject({ session_id: 's1', last_seen: 5 })
    })

    client.close()
  })

  it('dispatches replayed events through the normal handler path', async () => {
    const client = makeClient()
    const seen: string[] = []
    client.on('tool.complete', e => seen.push(`live:${String((e.payload as { n?: number }).n)}`))

    const first = client.connect('ws://x')
    let sock = sockets[sockets.length - 1]
    sock.open()
    await first
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 3 } })

    client.invalidate('drop')
    const second = client.connect('ws://x')
    sock = sockets[sockets.length - 1]
    sock.open()
    await second

    await vi.waitFor(async () => {
      const req = sock.lastRequest()
      expect(req.method).toBe('session.events.since')
      // Answer the replay request with two missed events.
      sock.serverFrame({
        jsonrpc: '2.0',
        id: req.id,
        result: {
          events: [
            { type: 'tool.complete', session_id: 's1', seq: 4, payload: { n: 1 } },
            { type: 'tool.complete', session_id: 's1', seq: 5, payload: { n: 2 } }
          ],
          latest_seq: 5,
          truncated: false,
          count: 2
        }
      })
      await Promise.resolve()
      expect(seen).toEqual(['live:1', 'live:2'])
    })

    expect(client.getSeqWatermarks().s1).toBe(5)
    client.close()
  })

  it('does not attempt replay when nothing was ever observed', async () => {
    const client = makeClient()
    const p = client.connect('ws://x')
    sockets[0].open()
    await p
    // No events ever seen → close+reconnect must NOT fire a replay RPC.
    client.invalidate('drop')
    const p2 = client.connect('ws://x')
    sockets[sockets.length - 1].open()
    await p2
    await new Promise(r => setTimeout(r, 20))

    expect(sockets[sockets.length - 1].sent).toHaveLength(0)
    client.close()
  })

  it('replayed seqs advance watermarks but never regress them', async () => {
    const client = makeClient()
    const first = client.connect('ws://x')
    let sock = sockets[sockets.length - 1]
    sock.open()
    await first
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'status.update', session_id: 's1', seq: 10 } })

    client.invalidate('drop')
    const second = client.connect('ws://x')
    sock = sockets[sockets.length - 1]
    sock.open()
    await second

    await vi.waitFor(() => {
      expect(sock.lastRequest().method).toBe('session.events.since')
    })
    // Replay returns a STALE frame (seq 2 < watermark 10): watermark must hold.
    const req = sock.lastRequest()
    sock.serverFrame({
      jsonrpc: '2.0',
      id: req.id,
      result: { events: [{ type: 'status.update', session_id: 's1', seq: 2 }], latest_seq: 10, truncated: false, count: 1 }
    })
    await Promise.resolve()
    expect(client.getSeqWatermarks().s1).toBe(10)
    client.close()
  })

  it('rejects envelope-shaped replay elements (the #94219 server-shape bug)', async () => {
    const client = makeClient()
    const seen: string[] = []
    client.on('message.delta', () => seen.push('delta'))

    const first = client.connect('ws://x')
    let sock = sockets[sockets.length - 1]
    sock.open()
    await first
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 1 } })
    expect(seen).toEqual(['delta']) // the pre-drop live frame

    client.invalidate('drop')
    const second = client.connect('ws://x')
    sock = sockets[sockets.length - 1]
    sock.open()
    await second

    await vi.waitFor(() => {
      expect(sock.lastRequest().method).toBe('session.events.since')
    })
    const req = sock.lastRequest()
    // Pre-fix servers returned FULL JSON-RPC envelopes. The client must not
    // dispatch those blindly — and this documents why the server now sends
    // bare event objects.
    sock.serverFrame({
      jsonrpc: '2.0',
      id: req.id,
      result: {
        events: [{ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 2 } }],
        latest_seq: 2,
        truncated: false,
        count: 1
      }
    })
    await Promise.resolve()
    // Envelope-shaped replay elements must add nothing beyond the live frame.
    expect(seen).toEqual(['delta'])
    client.close()
  })

  it('holds live frames racing the replay fetch — no double dispatch, no skipped gap', async () => {
    const client = makeClient()
    const seen: number[] = []
    client.on('message.delta', e => seen.push((e as unknown as { seq: number }).seq))

    const first = client.connect('ws://x')
    let sock = sockets[sockets.length - 1]
    sock.open()
    await first
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 2 } })
    expect(seen).toEqual([2]) // pre-drop live frame dispatches normally

    client.invalidate('drop')
    const second = client.connect('ws://x')
    sock = sockets[sockets.length - 1]
    sock.open()
    await second

    await vi.waitFor(() => {
      expect(sock.lastRequest().method).toBe('session.events.since')
    })

    // LIVE frames 5 and 6 arrive while the replay (which carries 3,4,5) is
    // still in flight. They must be parked, not dispatched ahead of the gap.
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 5 } })
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 6 } })
    expect(seen).toEqual([2]) // still only the pre-drop frame — 5/6 parked

    const req = sock.lastRequest()
    sock.serverFrame({
      jsonrpc: '2.0',
      id: req.id,
      result: {
        events: [
          { type: 'message.delta', session_id: 's1', seq: 3 },
          { type: 'message.delta', session_id: 's1', seq: 4 },
          { type: 'message.delta', session_id: 's1', seq: 5 }
        ],
        latest_seq: 5,
        truncated: false,
        count: 3
      }
    })

    // In-order, exactly once: replayed 3,4,5 then the parked live 6 —
    // the parked duplicate of 5 is seq-gated out.
    await vi.waitFor(() => {
      expect(seen).toEqual([2, 3, 4, 5, 6])
    })
    expect(client.getSeqWatermarks().s1).toBe(6)
    client.close()
  })

  it('clears stale watermarks when the backend epoch changes (restart poisoning)', async () => {
    const client = makeClient()

    const first = client.connect('ws://x')
    let sock = sockets[sockets.length - 1]
    sock.open()
    await first
    // Learn epoch A and a high watermark.
    sock.serverFrame({
      jsonrpc: '2.0',
      method: 'event',
      params: { type: 'gateway.ready', payload: { replay_epoch: 'epoch-A' } }
    })
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 97 } })
    expect(client.getSeqWatermarks()).toEqual({ s1: 97 })

    // Backend restarts: reconnect, replay under a NEW epoch returns nothing
    // (fresh process, empty ring) — pre-fix the client kept watermark 97 and
    // silently believed it missed nothing, forever.
    client.invalidate('drop')
    const second = client.connect('ws://x')
    sock = sockets[sockets.length - 1]
    sock.open()
    await second

    await vi.waitFor(() => {
      expect(sock.lastRequest().method).toBe('session.events.since')
    })
    const req = sock.lastRequest()
    sock.serverFrame({
      jsonrpc: '2.0',
      id: req.id,
      result: { events: [], latest_seq: 0, truncated: false, count: 0, epoch: 'epoch-B' }
    })

    await vi.waitFor(() => {
      expect(client.getSeqWatermarks()).toEqual({})
    })

    // New-epoch events build fresh watermarks from scratch.
    sock.serverFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.delta', session_id: 's1', seq: 3 } })
    expect(client.getSeqWatermarks()).toEqual({ s1: 3 })
    client.close()
  })
})
