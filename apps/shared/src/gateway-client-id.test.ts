import { describe, expect, it } from 'vitest'

import { getOrCreateGatewayClientId } from './json-rpc-gateway'

class MemoryStorage {
  private readonly values = new Map<string, string>()

  getItem(key: string): string | null {
    return this.values.get(key) ?? null
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value)
  }
}

describe('getOrCreateGatewayClientId', () => {
  it('reuses one stable identity within a window storage', () => {
    const storage = new MemoryStorage()
    let sequence = 0
    const createId = () => `generated-${++sequence}`

    expect(getOrCreateGatewayClientId('desktop', storage, createId)).toBe('desktop:generated-1')
    expect(getOrCreateGatewayClientId('desktop', storage, createId)).toBe('desktop:generated-1')
    expect(sequence).toBe(1)
  })

  it('isolates identities across window stores', () => {
    let sequence = 0
    const createId = () => `generated-${++sequence}`

    expect(getOrCreateGatewayClientId('web', new MemoryStorage(), createId)).toBe('web:generated-1')
    expect(getOrCreateGatewayClientId('web', new MemoryStorage(), createId)).toBe('web:generated-2')
  })

  it('replaces malformed persisted identities', () => {
    const storage = new MemoryStorage()
    storage.setItem('hermes.gateway.client-id.web.v1', 'forged identity with spaces')

    expect(getOrCreateGatewayClientId('web', storage, () => 'replacement')).toBe('web:replacement')
  })
})
