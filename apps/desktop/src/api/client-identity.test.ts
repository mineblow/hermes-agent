import { beforeEach, describe, expect, it } from 'vitest'

import { desktopGatewayClientId } from './client'

describe('Desktop gateway client identity', () => {
  beforeEach(() => sessionStorage.clear())

  it('survives a renderer reload in the same window session', () => {
    const beforeReload = desktopGatewayClientId()
    const afterReload = desktopGatewayClientId()

    expect(afterReload).toBe(beforeReload)
    expect(afterReload).toMatch(/^desktop:/)
  })
})
