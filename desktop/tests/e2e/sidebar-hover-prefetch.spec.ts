import { expect, test } from "@playwright/test";

import { installMockBridge } from "../helpers/bridge";

/**
 * Sidebar hover intent must warm the hovered channel's message window before
 * the click: dwelling on an unvisited channel row triggers exactly one
 * window fetch, so the subsequent click paints from cache instead of paying
 * the fetch on the switch path. Scrubbing across the row (enter → quick
 * leave) must NOT fetch.
 */

declare global {
  interface Window {
    __CHANNEL_WINDOW_HEAD_FETCH_COUNT__?: number;
  }
}

async function windowFetchCount(page: import("@playwright/test").Page) {
  return page.evaluate(() => window.__CHANNEL_WINDOW_HEAD_FETCH_COUNT__ ?? 0);
}

test("hover dwell prefetches the channel window; scrubbing does not", async ({
  page,
}) => {
  await installMockBridge(page);
  await page.goto("/");
  await expect(page.getByTestId("app-sidebar")).toBeVisible();
  const baseline = await windowFetchCount(page);

  // Scrub: enter and leave immediately — under the dwell, no fetch.
  const random = page.getByTestId("channel-random");
  await random.hover();
  await page.getByTestId("channel-general").hover({ force: true });
  await page.getByTestId("app-sidebar").hover({ position: { x: 4, y: 4 } });
  await page.waitForTimeout(300);
  const afterScrub = await windowFetchCount(page);

  // Dwell: hover and stay past the intent threshold — exactly one fetch for
  // the hovered channel, before any click.
  await random.hover();
  await expect
    .poll(() => windowFetchCount(page), { timeout: 2_000 })
    .toBe(afterScrub + 1);

  // The click then serves the timeline from the warmed cache. The
  // subscription-gap refresh may add its own fetch after mount; the paint
  // itself must not wait on one.
  await random.click();
  await expect(page.getByTestId("chat-title")).toHaveText("random");

  // Scrubbing earlier must not have fetched anything beyond the baseline.
  expect(afterScrub).toBe(baseline);
});
