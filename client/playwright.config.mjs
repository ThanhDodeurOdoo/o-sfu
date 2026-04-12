import { defineConfig } from "@playwright/test";

export default defineConfig({
    testDir: "./playwright",
    timeout: 30_000,
    outputDir: "./output/playwright/results",
    reporter: [["list"]],
    use: {
        baseURL: "http://127.0.0.1:4173",
        browserName: "chromium",
        headless: true,
        screenshot: "only-on-failure",
        trace: "retain-on-failure",
        video: "retain-on-failure"
    },
    webServer: {
        command: "node ./playwright/serve_client.mjs",
        reuseExistingServer: true,
        timeout: 10_000,
        url: "http://127.0.0.1:4173/playwright/fixtures/harness.html"
    }
});
