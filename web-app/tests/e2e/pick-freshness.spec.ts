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

test('compares only fresh pregame prices from a matched totals snapshot', async ({ page }) => {
    const now = Date.now();
    await page.clock.install({ time: now });
    const at = (minutes: number) => new Date(now + minutes * 60_000).toISOString();
    const pair = (book: string, fetchedAt = at(-14.5), providerStart = at(5)) => {
        const quote = {
            book,
            book_url: 'https://sportsbook.example.invalid',
            market: 'totals',
            line: 220.5,
            snapshot_id: `${book}-snapshot`,
            fetched_at_utc: fetchedAt,
            observed_start_utc: providerStart,
        };
        return [
            { ...quote, outcome: 'Over', moneyline: -110 },
            { ...quote, outcome: 'Under', moneyline: 100 },
        ];
    };
    const [mixedOver, mixedUnder] = pair('Different Lines');
    const [splitOver, splitUnder] = pair('Different Snapshots');
    const [naiveCapture, naiveKickoff] = await page.evaluate((instants) => instants.map((instant) => {
        const date = new Date(instant);
        return new Date(date.getTime() - date.getTimezoneOffset() * 60_000).toISOString().slice(0, -1);
    }), [at(-1), at(5)]);
    await mockBetsRoutes(page, {
        premium: true,
        upcoming: [{
            ...MOCK_UPCOMING[0], market: 'totals', commence_time: at(5), quote_expires_at: at(2),
            odds_data: [
                ...pair('Matched Book'),
                mixedOver, { ...mixedUnder, line: 224.5 },
                splitOver, { ...splitUnder, snapshot_id: 'other-snapshot' },
                ...pair('Tied Snapshots'),
                ...pair('Tied Snapshots', at(-14.5).replace('Z', '+00:00'))
                    .map((quote) => ({ ...quote, snapshot_id: 'other-tied-snapshot', line: 224.5 })),
                ...pair('Stale Book', at(-16)),
                ...pair('In Play Book', at(-0.25), at(-0.5)),
                ...pair('Future Capture', at(1)),
                ...pair('Unknown Kickoff').map((quote) => ({ ...quote, observed_start_utc: undefined })),
                ...pair('Naive Capture', naiveCapture),
                ...pair('Naive Kickoff', at(-1), naiveKickoff),
            ],
        }],
    });
    await page.goto('/');
    await page.getByText('Mock Home FC').click();
    const dialog = page.getByRole('dialog');
    const comparison = dialog.getByRole('link', { name: /Matched Book/ });
    await expect(dialog.getByText('OVER 215.5', { exact: true })).toBeVisible();
    await expect(comparison).toContainText('220.5');
    await expect(comparison).toContainText('+100');
    await expect(dialog.getByRole('link', {
        name: /Different Lines|Different Snapshots|Tied Snapshots|Stale Book|In Play Book|Future Capture|Unknown Kickoff|Naive Capture|Naive Kickoff/,
    })).toHaveCount(0);

    await page.clock.fastForward(30_001);

    await expect(comparison).toHaveCount(0);
    await expect(dialog.getByText('OVER 215.5', { exact: true })).toBeVisible();
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

test('withdraws a paid quote when refresh fails and recovers on retry', async ({ page }) => {
    await page.clock.install();
    await mockBetsRoutes(page, { premium: true });
    let requests = 0;
    await page.route('**/api/bets/upcoming**', async (route) => {
        requests += 1;
        await route.fulfill(requests === 2
            ? { status: 503, body: 'unavailable' }
            : { json: { data: [MOCK_UPCOMING[0]], is_premium: true } });
    });
    await page.goto('/');
    await expect(page.getByText('Mock Home FC')).toBeVisible();

    await page.clock.fastForward(60_001);

    await expect.poll(() => requests).toBe(2);
    await expect(page.getByText('Mock Home FC')).toHaveCount(0);
    await expect(page.getByText(/Upcoming picks are temporarily unavailable/)).toBeVisible();
    await expect(page.getByText('No upcoming picks right now')).toHaveCount(0);

    await page.getByRole('button', { name: 'Retry', exact: true }).click();
    await expect(page.getByText('Mock Home FC')).toBeVisible();
    await expect(page.getByText(/Upcoming picks are temporarily unavailable/)).toHaveCount(0);
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
