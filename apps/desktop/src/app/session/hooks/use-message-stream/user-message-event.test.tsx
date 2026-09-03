import { act, cleanup } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'

import { chatMessageText, textPart } from '@/lib/chat-messages'
import { createClientSessionState } from '@/lib/chat-runtime'

import { renderMessageStream } from './test-harness'

const SID = 'shared-session'

describe('durable user message events', () => {
  afterEach(cleanup)

  it('inserts the peer prompt before the live assistant tool turn', () => {
    const stream = renderMessageStream(SID)

    act(() => {
      // Gateway accepts the turn before the agent has persisted its user row.
      stream.handleEvent({ payload: { timestamp: 99 }, session_id: SID, type: 'message.start' })
      stream.handleEvent({
        payload: { message_id: 'user-pc1-123', text: 'search every file', timestamp: 100 },
        session_id: SID,
        type: 'message.user'
      })
      stream.handleEvent({
        payload: { args: { pattern: '*.ts' }, name: 'search_files', timestamp: 102, tool_id: 'call-1' },
        session_id: SID,
        type: 'tool.start'
      })
    })

    const messages = stream.state().messages

    expect(messages.map(message => message.role)).toEqual(['user', 'assistant'])
    expect(chatMessageText(messages[0])).toBe('search every file')
    expect(messages[0].id).toBe('user-pc1-123')
    expect(messages[1].parts.some(part => part.type === 'tool-call')).toBe(true)
  })

  it('reconciles the sender optimistic message and a watcher-hydrated equivalent without duplicates', () => {
    const optimistic = createClientSessionState()
    optimistic.messages = [
      { id: 'user-pc1-123', parts: [textPart('search every file')], role: 'user', timestamp: 100 }
    ]
    const hydrated = createClientSessionState()
    hydrated.messages = [
      { id: '100-0-user', parts: [textPart('search every file')], role: 'user', rowId: 42, timestamp: 100 }
    ]

    for (const state of [optimistic, hydrated]) {
      const states = new Map([[SID, state]])
      const stream = renderMessageStream(SID, { states })

      act(() =>
        stream.handleEvent({
          payload: { message_id: 'user-pc1-123', text: 'search every file', timestamp: 100 },
          session_id: SID,
          type: 'message.user'
        })
      )

      expect(stream.state().messages.filter(message => message.role === 'user')).toHaveLength(1)
    }
  })

  it('does not collapse an intentionally repeated prompt from an earlier turn', () => {
    const state = createClientSessionState()
    state.messages = [
      { id: 'old-user', parts: [textPart('search every file')], role: 'user', timestamp: 90 },
      { id: 'old-assistant', parts: [textPart('done')], role: 'assistant', timestamp: 91 }
    ]
    const stream = renderMessageStream(SID, { states: new Map([[SID, state]]) })

    act(() => {
      stream.handleEvent({ payload: { timestamp: 99 }, session_id: SID, type: 'message.start' })
      stream.handleEvent({
        payload: { message_id: 'new-user', text: 'search every file', timestamp: 100 },
        session_id: SID,
        type: 'message.user'
      })
    })

    expect(stream.state().messages.filter(message => message.role === 'user')).toHaveLength(2)
    expect(stream.state().messages.map(message => message.id)).toEqual(['old-user', 'old-assistant', 'new-user'])
  })
})