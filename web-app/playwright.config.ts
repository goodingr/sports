import { defineConfig, devices } from '@playwright/test';

const PORT = Number(process.env.PORT ?? 3100);
const BASE_URL = process.env.PLAYWRIGHT_BASE_URL ?? `http://localhost:${PORT}`;

export default defineConfig({
    testDir: './tests/e2e',
    timeout: 30_000,
    expect: { timeout: 5_000 },
    fullyParallel: true,
    reporter: [['list']],
    use: {
        baseURL: BASE_URL,
        trace: 'retain-on-failure',
    },
    webServer: {
        command: `npm run dev -- --port ${PORT}`,
        url: BASE_URL,
        reuseExistingServer: !process.env.CI,
        timeout: 120_000,
    },
    projects: [
        {
            name: 'chromium-desktop',
            use: { ...devices['Desktop Chrome'] },
        },
        {
            name: 'chromium-mobile-360',
            use: {
                ...devices['Desktop Chrome'],
                viewport: { width: 360, height: 740 },
            },
        },
        {
            name: 'chromium-mobile-390',
            use: {
                ...devices['Desktop Chrome'],
                viewport: { width: 390, height: 844 },
            },
        },
    ],
});
