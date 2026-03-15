import { test, expect } from '@playwright/test';

test('peer', async ({ page }) => {
  await page.goto('/examples/peer.html');

  // Expect a title "to contain" a substring.
  await expect(page).toHaveTitle(/Playwright/);
});
