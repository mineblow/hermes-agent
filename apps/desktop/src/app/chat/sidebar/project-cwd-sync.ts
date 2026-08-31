import type { NewChatWorkspaceTarget } from '@/store/session'

interface ProjectCwdSyncInput {
  currentCwd: string
  newChatWorkspaceTarget: NewChatWorkspaceTarget | undefined
  projectTarget: string
}

/**
 * Entering a project normally selects its default checkout. A fresh draft with
 * an explicit workspace target is different: project discovery may enter the
 * parent project after creating a linked worktree, but that must not re-anchor
 * the draft from the worktree back to the project's default checkout.
 */
export function shouldSyncProjectCwd({
  currentCwd,
  newChatWorkspaceTarget,
  projectTarget,
}: ProjectCwdSyncInput): boolean {
  const target = projectTarget.trim()

  if (!target || target === currentCwd) {
    return false
  }

  const explicitDraftTarget = typeof newChatWorkspaceTarget === 'string' ? newChatWorkspaceTarget.trim() : ''

  return !explicitDraftTarget || explicitDraftTarget !== currentCwd
}
