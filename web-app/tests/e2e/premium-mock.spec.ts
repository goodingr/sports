import { test, expect } from '@playwright/test';
import { mockBetsRoutes, MOCK_HISTORY } from './_fixtures/mocks';

test.describe('Premium-mock view (history tab)', () => {
    test.beforeEach(async ({ page }) => {
        await mockBetsRoutes(page, { history: MOCK_HISTORY });
    });

    test('completed picks show team names, prediction, and profit (never blank)', async ({ page }) => {
        await page.goto('/');
        // Switch to the history tab
        await page.getByRole('tab', { name: /Past/i }).click();

        await expect(page.getByText('Past Home')).toBeVisible({ timeout: 15_000 });
        await expect(page.getByText('Past Away')).toBeVisible();
        // Prediction text on completed cards is always shown.
        await expect(page.getByText('OVER 210.5')).toBeVisible();
        // Profit money line.
        await expect(page.getByText(/\+\$90\.91/)).toBeVisible();
    });

    test('completed card opens modal without paywall overlay', async ({ page }) => {
        await page.goto('/');
        await page.getByRole('tab', { name: /Past/i }).click();
        await page.getByText('Past Home').first().click();
        await expect(page.getByRole('dialog')).toBeVisible();
        // Premium Only headline should NOT be visible for completed picks.
        await expect(page.getByText('Premium Only').first()).toHaveCount(0);
    });
});
