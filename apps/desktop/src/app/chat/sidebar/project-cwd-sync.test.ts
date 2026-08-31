import { describe, expect, it } from 'vitest'

import { shouldSyncProjectCwd } from './project-cwd-sync'

describe('shouldSyncProjectCwd', () => {
  it('keeps an explicit linked-worktree target when project discovery enters its parent project', () => {
    const worktree = '/repo/.worktrees/feature'

    expect(
      shouldSyncProjectCwd({
        currentCwd: worktree,
        newChatWorkspaceTarget: worktree,
        projectTarget: '/repo',
      }),
    ).toBe(false)
  })

  it('selects the project checkout without an explicit draft target', () => {
    expect(
      shouldSyncProjectCwd({
        currentCwd: '/other-repo',
        newChatWorkspaceTarget: undefined,
        projectTarget: '/repo',
      }),
    ).toBe(true)
  })

  it('selects the project checkout when the explicit target no longer owns the current cwd', () => {
    expect(
      shouldSyncProjectCwd({
        currentCwd: '/other-repo',
        newChatWorkspaceTarget: '/repo/.worktrees/feature',
        projectTarget: '/repo',
      }),
    ).toBe(true)
  })
})
