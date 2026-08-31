/**
 * E2E contract for the compositor-only GlyphSpinner.
 *
 * The spinner's whole reason for existing in this shape is a CSS animation:
 * every frame is in the DOM from mount and a `transform` keyframes animation
 * scrolls between them, so there is no JS timer and no per-tick DOM mutation
 * scheduling document-scale style recalculation.
 *
 * None of that is observable in jsdom — it has no animation engine, no
 * cascade resolution for `steps()`, and no `Element.getAnimations()`. The
 * jsdom suite (src/components/ui/glyph-spinner.test.tsx) therefore pins the
 * DATA and WIRING, and this spec pins the RENDERED BEHAVIOUR in a real
 * browser, which is the only place the stylesheet actually runs.
 *
 * This replaces three tests that asserted on the TEXT of the stylesheet.
 * Reading source in a test is banned outright (AGENTS.md) and those tests
 * proved the point: a var()-fallback edit that changed no rendered pixel
 * broke one of them, while none of them had ever executed the CSS.
 *
 * Prerequisite: `npm run build` must have been run so dist/ exists.
 */

import { expect, type Locator, test } from '@playwright/test'

import { type MockBackendFixture, setupMockBackend, waitForAppReady } from './fixtures'
import { type BackgroundReleaseHandle, createBackgroundReleaseHandle, restartMockServer } from './mock-server'

const ACTIVE_SURFACE = '[data-composer-target]:not([data-pane-hidden] [data-composer-target])'
const ACTIVE_STRIP = `${ACTIVE_SURFACE} [data-slot="composer-status-stack"] [role="status"][aria-label="Running"] .glyph-spinner__strip`

/**
 * Send a message so a turn is in flight — the composer status stack mounts a
 * GlyphSpinner while the agent is working. Resolves once a frame strip is in
 * the DOM.
 */
async function mountSpinner(current: MockBackendFixture): Promise<Locator> {
  const { page } = current
  const cdp = await page.context().newCDPSession(page)
  await cdp.send('Emulation.setFocusEmulationEnabled', { enabled: true })
  await page.evaluate(() => window.dispatchEvent(new FocusEvent('focus')))

  const composer = page.locator('[contenteditable="true"]').first()
  await composer.waitFor({ state: 'visible', timeout: 10_000 })
  await composer.click()
  await composer.type('E2E_SIDEBAR_CROSS', { delay: 10 })
  await page.keyboard.press('Enter')

  const statusStack = page.locator(`${ACTIVE_SURFACE} [data-slot="composer-status-stack"]`)
  await expect(statusStack).toBeVisible({ timeout: 30_000 })
  const backgroundGroup = statusStack.locator('button').filter({ hasText: /Background/i }).first()
  await backgroundGroup.click()

  const strip = page.locator(ACTIVE_STRIP).last()
  await expect(strip).toBeVisible({ timeout: 20_000 })
  await expect(strip.locator('..')).not.toHaveAttribute('data-paused', 'true')
  await expect.poll(() => strip.evaluate(el => getComputedStyle(el).animationPlayState)).toBe('running')

  return strip
}

test.describe('GlyphSpinner (compositor animation)', () => {
  let fixture: MockBackendFixture
  let backgroundRelease: BackgroundReleaseHandle

  test.beforeEach(async () => {
    restartMockServer()
    backgroundRelease = createBackgroundReleaseHandle()
    fixture = await setupMockBackend({
      extraConfig: 'approvals:\n  mode: off',
      mockServer: { backgroundReleasePath: backgroundRelease.path },
    })
    await waitForAppReady(fixture)
  })

  test.afterEach(async () => {
    backgroundRelease?.release()
    await fixture?.cleanup()
    backgroundRelease?.cleanup()
  })

  test('animates with a steps() transform keyframes animation, one step per frame', async () => {
    const strip = await mountSpinner(fixture)

    const observed = await strip.evaluate(el => {
      const style = getComputedStyle(el)
      const animations = el.getAnimations()
      const frameCount = el.querySelectorAll('.glyph-spinner__frame').length
      const frameHeight = el.querySelector<HTMLElement>('.glyph-spinner__frame')?.getBoundingClientRect().height ?? 0
      const transforms = ((animations[0]?.effect as KeyframeEffect | undefined)?.getKeyframes() ?? [])
        .map(k => String((k as Keyframe & { transform?: string }).transform ?? ''))
      const travelPx = transforms.map(transform =>
        Number(transform.match(/translateY\((-?\d+(?:\.\d+)?)px\)/)?.[1] ?? Number.NaN),
      )

      return {
        frameCount,
        frameHeight,
        timingFunction: style.animationTimingFunction,
        iterationCount: style.animationIterationCount,
        durationMs: animations[0]?.effect?.getTiming().duration ?? null,
        names: animations.map(a => (a as CSSAnimation).animationName),
        transforms,
        travelPx,
      }
    })

    // The strip carries every frame; `steps(N)` parks on each one in turn.
    expect(observed.frameCount).toBeGreaterThan(1)
    // Chromium has serialized jump-end as both `steps(N)` and `steps(N, end)`.
    expect(observed.timingFunction).toMatch(new RegExp(`^steps\\(${observed.frameCount}\\b`))
    expect(observed.iterationCount).toBe('infinite')
    expect(observed.names).toContain('glyph-spinner-advance')
    // One full cycle is frames x interval, so the duration must be a positive
    // multiple of the frame count — not the single-frame interval.
    expect(observed.durationMs).toBeGreaterThan(0)
    // Absolute frame travel keeps the animation compositor-eligible and must
    // equal every rendered frame stacked in the strip.
    expect(observed.transforms.join(' | ')).not.toContain('%')
    expect(observed.travelPx.every(Number.isFinite)).toBe(true)
    const renderedTravel = observed.frameCount * observed.frameHeight
    const keyframeTravel = Math.abs(observed.travelPx.at(-1)! - observed.travelPx[0]!)
    expect(Math.abs(keyframeTravel - renderedTravel)).toBeLessThanOrEqual(1)
  })

  test('is promoted to a layer while running, and neither animates nor holds a layer when parked', async () => {
    const { page } = fixture
    const strip = await mountSpinner(fixture)
    const retainedStrip = await strip.elementHandle()
    expect(retainedStrip, 'The running production spinner should be mounted').toBeTruthy()

    const running = await strip.evaluate(el => ({
      playState: getComputedStyle(el).animationPlayState,
      willChange: getComputedStyle(el).willChange,
    }))

    expect(running.playState).toBe('running')
    // Scoped to active spinners — a permanently promoted layer per parked
    // spinner is pure memory at fan-out breadth.
    expect(running.willChange).toBe('transform')

    // 1. Exercise the real global focus wiring instead of mutating its output
    // attribute. A blurred renderer must pause every continuous spinner.
    const cdp = await page.context().newCDPSession(page)
    await cdp.send('Emulation.setFocusEmulationEnabled', { enabled: false })
    await page.evaluate(() => window.dispatchEvent(new FocusEvent('blur')))
    await expect(page.locator('html')).toHaveAttribute('data-renderer-animations-paused', '')
    await expect.poll(() => strip.evaluate(el => getComputedStyle(el).animationPlayState)).toBe('paused')

    // Restore focus, then exercise usePaneVisible by parking the running
    // surface behind a real new-session tab.
    await cdp.send('Emulation.setFocusEmulationEnabled', { enabled: true })
    await page.evaluate(() => window.dispatchEvent(new FocusEvent('focus')))
    await expect(page.locator('html')).not.toHaveAttribute('data-renderer-animations-paused', '')
    await page.locator('[data-slot="sidebar"] button[aria-label="New session"]').first().click()
    await expect.poll(() => retainedStrip!.evaluate(el => el.parentElement?.getAttribute('data-paused'))).toBe('true')
    expect(await retainedStrip!.evaluate(el => getComputedStyle(el).animationPlayState)).toBe('paused')
    expect(await retainedStrip!.evaluate(el => getComputedStyle(el).willChange)).toBe('auto')
  })

  test('advances in discrete frames and creates no timer-driven DOM churn', async () => {
    const strip = await mountSpinner(fixture)

    // Sample the resolved transform across one full cycle. A steps() animation
    // holds each value for a whole interval and jumps between them, so the
    // distinct values it visits must be bounded by the frame count — a linear
    // animation would produce a new value on every sample.
    const sampled = await strip.evaluate(async el => {
      const frames = el.querySelectorAll('.glyph-spinner__frame').length
      const duration = Number(el.getAnimations()[0]?.effect?.getTiming().duration ?? 0)
      const seen = new Set<string>()
      const textAtStart = el.textContent

      const deadline = performance.now() + duration

      while (performance.now() < deadline) {
        seen.add(getComputedStyle(el).transform)
        await new Promise(resolve => requestAnimationFrame(() => resolve(null)))
      }

      return { distinct: seen.size, frames, textUnchanged: el.textContent === textAtStart }
    })

    expect(sampled.distinct).toBeGreaterThan(1)
    expect(sampled.distinct).toBeLessThanOrEqual(sampled.frames + 1)
    // The old implementation rewrote textContent ~12x/second. Nothing may
    // mutate the DOM as this animates — that mutation is the whole incident.
    expect(sampled.textUnchanged).toBe(true)
  })
})
