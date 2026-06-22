import { test } from "@playwright/test";
import scenario from "./scenario.js";

// Enable debug logging from the page console
// Set to 'peerix:*' to enable debug logging
const { DEBUG = "" } = process.env;

for (const testCase of scenario) {
  test(`[${testCase.id}] ${testCase.title}`, async ({ page }) => {
    await page.goto("./tests/index.html");

    if (DEBUG) {
      page.on("console", (msg) => {
        console.log(`CONSOLE: ${msg.text()}`);
      });
    }

    // User gesture is required in some browsers
    await page.click("body");

    await page.evaluate(
      async ({ debug, testCase }) => {
        const TestRunner = (window as any).TestRunner;
        const runner = new TestRunner({ debug });
        await runner.run(testCase as any);
      },
      { debug: DEBUG, testCase },
    );
  });
}
