import { test, expect } from '@playwright/test';

test.describe('Pricing page', () => {
    test('shows headline, price, features, and CTA', async ({ page }) => {
        await page.goto('/pricing');
        await expect(
            page.getByRole('heading', { name: /Simple, Transparent Pricing/i })
        ).toBeVisible();
        await expect(page.getByText(/\$29/)).toBeVisible();
        await expect(page.getByText(/Cancel Anytime/i)).toBeVisible();
        await expect(page.getByRole('button', { name: /(Subscribe|Sign Up & Subscribe)/i })).toBeVisible();
    });

    test('legal links resolve to real pages', async ({ page }) => {
        await page.goto('/pricing');
        const termsLink = page.locator('a[href="/terms"]').first();
        await expect(termsLink).toBeVisible();
        await termsLink.click();
        await expect(page).toHaveURL(/\/terms$/);
        await expect(page.getByRole('heading', { name: /Terms of Service/i })).toBeVisible();
    });
});
