import { test, expect } from '@playwright/test';
import { mockBetsRoutes, MOCK_HISTORY, MOCK_UPCOMING } from './_fixtures/mocks';

test('renders the gated market selection and quoted price without a legacy description', async ({ page }) => {
    const [totals, moneyline] = MOCK_UPCOMING;
    await mockBetsRoutes(page, {
        premium: true,
        upcoming: [
            { ...totals, prediction: undefined, market: 'totals', moneyline: -110 },
            {
                ...moneyline, prediction: undefined, bet_type: undefined, market: 'moneyline',
                side: 'away', moneyline: 125, predicted_total_points: 223,
            },
        ],
    });

    await page.goto('/');
    await expect(page.getByText('OVER 215.5')).toBeVisible();
    await page.getByRole('button', { name: 'Demo Wolves at Demo Hawks, Demo Wolves, odds +125' }).click();
    const dialog = page.getByRole('dialog');
    await expect(dialog.getByText('+125')).toBeVisible();
    await expect(dialog.getByText('Predicted Total')).toHaveCount(0);
});

test.describe('Premium-mock view (history tab)', () => {
    test.beforeEach(async ({ page }) => {
        await mockBetsRoutes(page, { history: MOCK_HISTORY });
    });

    test('completed picks show team names, prediction, and profit (never blank)', async ({ page }) => {
        await page.goto('/');
        // Switch to the history tab
        await page.getByRole('tab', { name: 'Model history' }).click();

        await expect(page.getByText('Past Home')).toBeVisible({ timeout: 15_000 });
        await expect(page.getByText('Past Away')).toBeVisible();
        // Prediction text on completed cards is always shown.
        await expect(page.getByText('OVER 210.5')).toBeVisible();
        // Profit money line.
        await expect(page.getByText(/\+\$90\.91/)).toBeVisible();
    });

    test('completed card opens modal without paywall overlay', async ({ page }) => {
        await page.goto('/');
        await page.getByRole('tab', { name: 'Model history' }).click();
        await page.getByText('Past Home').first().click();
        await expect(page.getByRole('dialog')).toBeVisible();
        // Premium Only headline should NOT be visible for completed picks.
        await expect(page.getByText('Premium Only').first()).toHaveCount(0);
    });
});
