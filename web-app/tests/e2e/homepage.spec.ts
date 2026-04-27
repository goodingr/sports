import { test, expect } from '@playwright/test';

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
        const candidates = [
            page.getByText(/Upcoming Picks/i),
            page.getByText(/No upcoming picks/i),
            page.getByText(/Unable to load picks/i),
            page.getByRole('status'),
        ];
        await Promise.race(candidates.map((loc) => loc.first().waitFor({ state: 'visible' })));
    });

    test('mobile nav toggle reveals nav links', async ({ page }, testInfo) => {
        if (testInfo.project.name === 'chromium-desktop') test.skip();
        await page.goto('/');
        await page.getByRole('button', { name: /Open navigation menu/i }).click();
        await expect(page.locator('#mobile-nav').getByRole('link', { name: 'Pricing' })).toBeVisible();
    });
});
