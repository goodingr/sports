import { test, expect } from '@playwright/test';

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
        // Either a partial banner appears, or upcoming content / empty state appears
        // without the full "Unable to load picks" error overtaking the page.
        await expect(page.getByText(/Unable to load picks/i)).toHaveCount(0, { timeout: 15_000 });
        await expect(page.getByText(/Recent results are temporarily unavailable/i)).toBeVisible();
    });
});
