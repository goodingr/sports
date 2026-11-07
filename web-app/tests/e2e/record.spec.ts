import { expect, test } from '@playwright/test';

const emptyRecord = {
    data: [], total: 0, page: 1, limit: 20, graded_picks: 0,
    wins: 0, losses: 0, pushes: 0, voids: 0,
    profit_units: 0, roi_percent: null, win_rate_percent: null,
    basis: 'One unit per published pick at its original quote',
};

const pick = {
    game_id: 'issued-1', league: 'NBA', home_team: 'Record Home', away_team: 'Record Away',
    start_time_utc: '2026-11-06T23:00:00Z', published_at: '2026-11-06T22:00:00Z',
    settled_at: '2026-11-07T02:00:00Z',
    market: 'totals', side: 'over', moneyline: -110, total_line: 210.5,
    book: 'Original Book', rule_id: 'approved_totals_rule', outcome: 'win',
    profit_units: 0.9090909, home_score: 110, away_score: 105, reason: null,
};

test('empty issued record remains separate from historical model performance', async ({ page }) => {
    await page.route('**/api/bets/record?**', (route) => route.fulfill({ json: emptyRecord }));
    await page.goto('/record');

    await expect(page.getByRole('heading', { name: 'Published pick record', exact: true })).toBeVisible();
    await expect(page.getByText('No settled published picks yet')).toBeVisible();
    await expect(page.getByText('ROI', { exact: true })).toHaveCount(0);
    await expect(page.getByRole('link', { name: 'View separate historical model tracking' })).toHaveAttribute('href', '/#model-history');
    await expect(page.getByRole('button', { name: /subscribe/i })).toHaveCount(0);
});

test('shows published quotes, unit returns, timestamps, and coherent pagination', async ({ page }) => {
    await page.route('**/api/bets/record?**', (route) => {
        const currentPage = Number(new URL(route.request().url()).searchParams.get('page'));
        return route.fulfill({ json: {
            ...emptyRecord, total: 21, page: currentPage, graded_picks: 20,
            wins: 11, losses: 8, pushes: 1, voids: 1, profit_units: 2,
            roi_percent: 10, win_rate_percent: 57.8947,
            data: currentPage === 1 ? [pick] : [{
                ...pick, game_id: 'issued-21', home_team: 'Last Home', outcome: 'void',
                profit_units: 0, reason: 'fixture_identity_changed', home_score: 115, away_score: 110,
            }],
        } });
    });
    await page.goto('/record');

    await expect(page.getByText('10.00%', { exact: true })).toBeVisible();
    await expect(page.getByText('57.9%', { exact: true })).toBeVisible();
    const list = page.getByRole('list', { name: 'Settled published picks' });
    await expect(list.getByText('OVER 210.5 · -110')).toBeVisible();
    await expect(list.getByText('+0.91u', { exact: true })).toBeVisible();
    await expect(list.getByText('Original Book')).toBeVisible();
    await expect(list.locator('time').first()).toHaveAttribute('datetime', '2026-11-06T22:00:00Z');
    await expect(list.locator('time[datetime="2026-11-07T02:00:00Z"]')).toBeVisible();
    await expect(list.getByText('Final score')).toBeVisible();
    await expect(page.getByText('Times shown in UTC.', { exact: false })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Previous' })).toBeDisabled();
    await page.getByRole('button', { name: 'Next', exact: true }).click();
    await expect(page.getByText('Page 2 of 2')).toBeVisible();
    await expect(list.getByText('fixture identity changed')).toBeVisible();
    await expect(list.getByText('Final score')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Next', exact: true })).toBeDisabled();
});

test('retries a failed ledger request without inventing a record', async ({ page }) => {
    let failed = true;
    await page.route('**/api/bets/record?**', (route) => failed
        ? route.fulfill({ status: 503, body: 'unavailable' })
        : route.fulfill({ json: emptyRecord }));
    await page.goto('/record');

    await expect(page.getByText('Published results are temporarily unavailable.')).toBeVisible();
    await expect(page.getByText('No settled published picks yet')).toHaveCount(0);
    failed = false;
    await page.getByRole('button', { name: 'Retry' }).click();
    await expect(page.getByText('No settled published picks yet')).toBeVisible();
});
