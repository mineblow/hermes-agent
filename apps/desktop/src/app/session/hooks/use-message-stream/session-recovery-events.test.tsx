import { act, cleanup } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { createClientSessionState } from '@/lib/chat-runtime'
import { $activeSessionId } from '@/store/session'
import { $sessionStates } from '@/store/session-states'
import type { RpcEvent } from '@/types/hermes'

import { renderMessageStream } from './test-harness'

beforeEach(() => {
  $activeSessionId.set(null)
  $sessionStates.set({})
})

afterEach(() => {
  cleanup()
  $activeSessionId.set(null)
  $sessionStates.set({})
})

describe('gateway session recovery presentation', () => {
  it('hydrates a truncated runtime from the durable session identified by shared replay', () => {
    const hydrate = vi.fn(async () => undefined)
    const stream = renderMessageStream('live-truncated', { hydrateFromStoredSession: hydrate })

    act(() =>
      stream.handleEvent({
        type: 'session.durable_resync_required',
        session_id: 'live-truncated',
        payload: {
          durable_session_id: 'stored-truncated',
          reason: 'replay_truncated',
          session_id: 'live-truncated'
        }
      } as RpcEvent)
    )

    expect(hydrate).toHaveBeenCalledWith(3, 'stored-truncated', 'live-truncated')
  })

  it('does not start an unidentifiable durable hydrate', () => {
    const hydrate = vi.fn(async () => undefined)
    const stream = renderMessageStream('live-truncated', { hydrateFromStoredSession: hydrate })

    act(() =>
      stream.handleEvent({
        type: 'session.durable_resync_required',
        session_id: 'live-truncated',
        payload: { reason: 'replay_truncated', session_id: 'live-truncated' }
      } as RpcEvent)
    )

    expect(hydrate).not.toHaveBeenCalled()
  })

  it('rebinds Desktop presentation state when shared replay reports a takeover rebound', () => {
    const cached = createClientSessionState('stored-session')
    const states = new Map([['live-old', cached]])
    const stream = renderMessageStream('live-old', { states })
    $activeSessionId.set('live-old')

    act(() =>
      stream.handleEvent({
        type: 'session.runtime_recovered',
        session_id: 'live-new',
        payload: {
          durable_session_id: 'stored-session',
          new_session_id: 'live-new',
          old_session_id: 'live-old'
        }
      } as RpcEvent)
    )

    expect(states.has('live-old')).toBe(false)
    expect(states.get('live-new')).toBe(cached)
    expect($activeSessionId.get()).toBe('live-new')
  })
})
