import { test, expect } from '@playwright/test';
import { mockBetsRoutes, MOCK_UPCOMING, MOCK_HISTORY } from './_fixtures/mocks';

test.describe('Locked (free) view', () => {
    test.beforeEach(async ({ page }) => {
        await mockBetsRoutes(page, {
            upcoming: MOCK_UPCOMING,
            history: MOCK_HISTORY,
        });
    });

    test('shows Unlock CTA on upcoming cards for non-premium users', async ({ page }) => {
        await page.goto('/');
        const unlockCta = page.getByRole('link', { name: /Subscribe to unlock/i }).first();
        await expect(unlockCta).toBeVisible({ timeout: 15_000 });
        await expect(unlockCta).toHaveAttribute('href', '/pricing');
    });

    test('locked cards still show team names so they never look blank', async ({ page }) => {
        await page.goto('/');
        // From mock fixture
        await expect(page.getByText('Mock Home FC')).toBeVisible({ timeout: 15_000 });
        await expect(page.getByText('Mock Away FC')).toBeVisible();
    });

    test('clicking a locked card opens modal with paywall overlay', async ({ page }) => {
        await page.goto('/');
        const teamCell = page.getByText('Mock Home FC').first();
        await teamCell.click();
        await expect(page.getByRole('dialog')).toBeVisible();
        await expect(page.getByText(/Premium Only/i)).toBeVisible();
        await expect(page.getByRole('link', { name: /Subscribe to Unlock/i })).toBeVisible();
    });
});
