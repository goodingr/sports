import { test, expect } from '@playwright/test';
import { mockBetsRoutes } from './_fixtures/mocks';

const emptyPublishedRecord = {
    data: [], total: 0, page: 1, limit: 1, published_picks: 0, pending_picks: 0, graded_picks: 0,
    wins: 0, losses: 0, pushes: 0, voids: 0, profit_units: 0, roi_percent: null,
    win_rate_percent: null, basis: 'One unit per published pick at its original quote',
};

test.describe('Homepage', () => {
    test('loads with brand, nav links, and footer', async ({ page }) => {
        await page.goto('/');
        await expect(page).toHaveTitle(/Sports Analytics/i);
        await expect(page.getByRole('link', { name: /Sports Analytics/i }).first()).toBeVisible();
        // Footer copyright is always rendered.
        await expect(page.getByText(/All rights reserved/i)).toBeVisible();
    });

    test('renders bet feed area (loading or content) without errors', async ({ page }) => {
        await page.goto('/');
        // Either a loading skeleton, empty state, error state, or actual feed should be rendered.
        const feed = page.locator('#model-history');
        const candidates = [
            feed.getByText(/Upcoming Picks/i),
            feed.getByText(/No upcoming picks/i),
            feed.getByText(/Unable to load picks/i),
            feed.getByRole('status'),
        ];
        await Promise.race(candidates.map((loc) => loc.first().waitFor({ state: 'visible' })));
    });

    test('keeps positive simulated results out of an empty issued record', async ({ page }) => {
        await mockBetsRoutes(page);
        await page.route('**/api/bets/record?**', route => route.fulfill({ json: emptyPublishedRecord }));
        await page.goto('/');

        const published = page.getByRole('region', { name: 'Published pick record' });
        await expect(published.getByText('No picks have been published yet.')).toBeVisible();
        await expect(published.getByText('Published ROI')).toHaveCount(0);
        await expect(page.getByText('$12,450.00', { exact: true })).toHaveCount(0);

        await page.getByRole('tab', { name: 'Model history' }).click();
        await expect(page.getByText('Simulated P&L', { exact: true })).toBeVisible();
        await expect(page.getByText('$12,450.00', { exact: true })).toBeVisible();
        await expect(page.getByRole('button', { name: /simulated model result/i })).toBeVisible();
        await expect(published.getByText('No picks have been published yet.')).toBeVisible();
    });

    test('shows issued returns from the published ledger and supports a model-history deep link', async ({ page }) => {
        await mockBetsRoutes(page);
        await page.route('**/api/bets/record?**', route => route.fulfill({ json: {
            ...emptyPublishedRecord, published_picks: 4, pending_picks: 1, total: 3, graded_picks: 3,
            wins: 1, losses: 2, profit_units: -1.05, roi_percent: -35,
        } }));
        await page.goto('/#model-history');

        const published = page.getByRole('region', { name: 'Published pick record' });
        await expect(published.getByText('-35.00%', { exact: true })).toBeVisible();
        await expect(published.getByRole('link', { name: 'View the full published pick record' })).toHaveAttribute('href', '/record');
        await expect(page.getByRole('tab', { name: 'Model history' })).toHaveAttribute('aria-selected', 'true');
        await expect(page.getByText('4.8%', { exact: true })).toBeVisible();
        await expect(published.getByText('4.8%', { exact: true })).toHaveCount(0);
    });

    test('distinguishes an unavailable issued record from an empty record', async ({ page }) => {
        await mockBetsRoutes(page);
        await page.route('**/api/bets/record?**', route => route.fulfill({ status: 503, body: 'Unavailable' }));
        await page.goto('/');

        const published = page.getByRole('region', { name: 'Published pick record' });
        await expect(published.getByRole('alert')).toContainText('Published results are temporarily unavailable.');
        await expect(published.getByText('No picks have been published yet.')).toHaveCount(0);
    });

    test('mobile nav toggle reveals nav links', async ({ page }, testInfo) => {
        if (testInfo.project.name === 'chromium-desktop') test.skip();
        await page.goto('/');
        await page.getByRole('button', { name: /Open navigation menu/i }).click();
        await expect(page.locator('#mobile-nav').getByRole('link', { name: 'Pricing' })).toBeVisible();
    });
});
