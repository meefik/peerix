import { test } from '@playwright/test';
import scenario from './scenario.js';

// Enable debug logging from the page console
// Set to 'peerix:*' to enable debug logging
const { DEBUG = '' } = process.env;

for (const testCase of scenario) {
  test(`[${testCase.id}] ${testCase.title}`, async ({ page }) => {
    await page.goto('./tests/index.html');

    if (DEBUG) {
      page.on('console', (msg) => {
        console.log(`CONSOLE: ${msg.text()}`);
      });
    }

    await page.evaluate(async ({ debug, testCase }) => {
      const handler = async () => {
        const TestRunner = (window as any).TestRunner;
        const runner = new TestRunner({ debug });
        await runner.run(testCase as any);
      };
      document.addEventListener('click', handler, { once: true, capture: true });
    }, { debug: DEBUG, testCase });

    // User gesture is required in some browsers
    await page.click('body');
  });
}
