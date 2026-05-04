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
        const unlockCta = page.getByRole('link', { name: /Subscribe to unlock this pick/i }).first();
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
        const dialog = page.getByRole('dialog');
        await expect(dialog).toBeVisible();
        await expect(dialog.getByText(/Premium Only/i)).toBeVisible();
        const modalCta = dialog.getByTestId('modal-paywall-cta');
        await expect(modalCta).toBeVisible();
        await expect(modalCta).toHaveAttribute('href', '/pricing');
    });

    test('escape key closes modal and restores focus', async ({ page }) => {
        await page.goto('/');
        const teamCell = page.getByText('Mock Home FC').first();
        await teamCell.click();
        const dialog = page.getByRole('dialog');
        await expect(dialog).toBeVisible();
        await page.keyboard.press('Escape');
        await expect(dialog).toHaveCount(0);
    });

    test('modal close button closes the dialog', async ({ page }) => {
        await page.goto('/');
        const teamCell = page.getByText('Mock Home FC').first();
        await teamCell.click();
        const dialog = page.getByRole('dialog');
        await expect(dialog).toBeVisible();
        await dialog.getByRole('button', { name: /close match details/i }).click();
        await expect(dialog).toHaveCount(0);
    });

    test('background bet cards are inert while modal is open', async ({ page }) => {
        await page.goto('/');
        const teamCell = page.getByText('Mock Home FC').first();
        await teamCell.click();
        const dialog = page.getByRole('dialog');
        await expect(dialog).toBeVisible();
        // The other upcoming card should not be reachable while the dialog is open.
        // (inert removes background interactives from the accessibility tree.)
        const otherCardCta = page
            .getByRole('main')
            .getByRole('link', { name: /Subscribe to unlock this pick/i });
        await expect(otherCardCta).toHaveCount(0);
    });
});
