import { expect, test } from '@playwright/test';
import { mockBetsRoutes, MOCK_UPCOMING } from './_fixtures/mocks';

test('expires a paid quote and closes its open details at the server deadline', async ({ page }) => {
    const now = new Date();
    await page.clock.install({ time: now });
    await mockBetsRoutes(page, {
        premium: true,
        upcoming: [{ ...MOCK_UPCOMING[0], quote_expires_at: new Date(now.getTime() + 30_000).toISOString() }],
    });
    await page.goto('/');
    await page.getByText('Mock Home FC').click();
    await expect(page.getByRole('dialog')).toBeVisible();

    await page.clock.fastForward(30_001);

    await expect(page.getByRole('dialog')).toHaveCount(0);
    await expect(page.getByText('Mock Home FC')).toHaveCount(0);
    await expect(page.getByText('No upcoming picks right now')).toBeVisible();
});

test('revalidates a paid feed in the background without replacing it with loading UI', async ({ page }) => {
    const now = new Date();
    await page.clock.install({ time: now });
    await mockBetsRoutes(page, { premium: true });
    let requests = 0;
    let finishRefresh: () => void = () => {};
    const refreshing = new Promise<void>((resolve) => { finishRefresh = resolve; });
    await page.route('**/api/bets/upcoming**', async (route) => {
        requests += 1;
        if (requests > 1) await refreshing;
        await route.fulfill({ json: {
            is_premium: true,
            data: requests === 1 ? [{
                ...MOCK_UPCOMING[0], quote_expires_at: new Date(now.getTime() + 10 * 60_000).toISOString(),
            }] : [],
        } });
    });
    await page.goto('/');
    await expect(page.getByText('Mock Home FC')).toBeVisible();

    await page.clock.fastForward(60_001);
    await expect.poll(() => requests).toBe(2);
    await expect(page.getByText('Mock Home FC')).toBeVisible();
    await expect(page.getByText('Loading bets...')).toHaveCount(0);
    finishRefresh();

    await expect(page.getByText('No upcoming picks right now')).toBeVisible();
});

test('withdraws a legacy quote when the next required refresh fails', async ({ page }) => {
    await page.clock.install();
    await mockBetsRoutes(page, { premium: true });
    let requests = 0;
    await page.route('**/api/bets/upcoming**', async (route) => {
        requests += 1;
        await route.fulfill(requests === 1
            ? { json: { data: [MOCK_UPCOMING[0]], is_premium: true } }
            : { status: 503, body: 'unavailable' });
    });
    await page.goto('/');
    await expect(page.getByText('Mock Home FC')).toBeVisible();

    await page.clock.fastForward(60_001);

    await expect.poll(() => requests).toBe(2);
    await expect(page.getByText('Mock Home FC')).toHaveCount(0);
    await expect(page.getByText(/Upcoming picks are temporarily unavailable/)).toBeVisible();
});

test('revalidates free previews when the page regains focus', async ({ page }) => {
    await mockBetsRoutes(page);
    let withdrawn = false;
    await page.route('**/api/bets/upcoming**', async (route) => {
        await route.fulfill({ json: { data: withdrawn ? [] : MOCK_UPCOMING, is_premium: false } });
    });
    await page.goto('/');
    await expect(page.getByRole('link', { name: 'Subscribe to unlock this pick' }).first()).toBeVisible();

    withdrawn = true;
    await page.evaluate(() => window.dispatchEvent(new Event('focus')));

    await expect(page.getByText('No upcoming picks right now')).toBeVisible();
    await expect(page.getByRole('link', { name: 'Subscribe to unlock this pick' })).toHaveCount(0);
});
