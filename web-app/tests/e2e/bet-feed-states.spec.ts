import { test, expect } from '@playwright/test';
import { MOCK_HISTORY, mockBetsRoutes } from './_fixtures/mocks';

test.describe('Bet feed loading & error states', () => {
    test('shows error UI with retry when both APIs are down', async ({ page }) => {
        await page.route('**/api/bets/upcoming**', async (route) => {
            await route.fulfill({ status: 500, body: 'boom' });
        });
        await page.route('**/api/bets/history**', async (route) => {
            await route.fulfill({ status: 500, body: 'boom' });
        });
        await page.route('**/api/bets/stats**', async (route) => {
            await route.fulfill({ status: 500, body: 'boom' });
        });

        await page.goto('/');
        await expect(page.getByText(/Unable to load picks/i)).toBeVisible({ timeout: 15_000 });
        await expect(page.getByRole('button', { name: /Retry/i })).toBeVisible();
    });

    test('shows partial banner when only history is down', async ({ page }) => {
        await page.route('**/api/bets/upcoming**', async (route) => {
            await route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ data: [] }),
            });
        });
        await page.route('**/api/bets/history**', async (route) => {
            await route.fulfill({ status: 500, body: 'boom' });
        });
        await page.route('**/api/bets/stats**', async (route) => {
            await route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ roi: 0, win_rate: 0, total_profit: 0, total_bets: 0 }),
            });
        });

        await page.goto('/');
        await expect(page.getByText('Model history is temporarily unavailable.', { exact: true })).toBeVisible();
        await expect(page.getByText('No upcoming picks right now')).toBeVisible();
        await expect(page.getByText(/Unable to load picks/i)).toHaveCount(0);
    });

    test('keeps the history outage visible when upcoming picks recover independently', async ({ page }) => {
        await mockBetsRoutes(page);
        let upcomingAvailable = false;
        let historyAvailable = false;
        await page.route('**/api/bets/upcoming**', (route) => route.fulfill(upcomingAvailable
            ? { json: { data: [], is_premium: false } }
            : { status: 503 }));
        await page.route('**/api/bets/history**', (route) => route.fulfill(historyAvailable
            ? { json: { data: MOCK_HISTORY, total: MOCK_HISTORY.length } }
            : { status: 503 }));

        await page.goto('/');
        await expect(page.getByText('Unable to load picks')).toBeVisible();

        upcomingAvailable = true;
        await page.evaluate(() => window.dispatchEvent(new Event('focus')));
        await expect(page.getByText('No upcoming picks right now')).toBeVisible();
        await page.getByRole('tab', { name: 'Model history' }).click();
        await expect(page.getByText('Model history is temporarily unavailable.', { exact: true })).toBeVisible();
        await expect(page.getByText('No model results yet')).toHaveCount(0);

        historyAvailable = true;
        await page.getByRole('button', { name: 'Retry', exact: true }).click();
        await expect(page.getByText('Past Home')).toBeVisible();
        await expect(page.getByText('Model history is temporarily unavailable.', { exact: true })).toHaveCount(0);
    });
});
