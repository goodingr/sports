import { test, expect } from '@playwright/test';
import { mockCheckoutAvailability } from './_fixtures/mocks';

test.describe('Pricing page', () => {
    test('shows headline, price, features, and CTA', async ({ page }) => {
        await mockCheckoutAvailability(page, true);
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

    test('signed-out CTA routes to signup with redirect back to pricing', async ({ page }) => {
        await mockCheckoutAvailability(page, true);
        await page.goto('/pricing');
        const cta = page.getByRole('button', { name: /Sign Up & Subscribe/i });
        await expect(cta).toBeVisible();
        await cta.click();
        await expect(page).toHaveURL(/\/signup\?redirect_url=(%2F|\/)pricing/);
    });

    test('shows paused enrollment when there are no validated picks', async ({ page }) => {
        await mockCheckoutAvailability(page, false);
        await page.goto('/pricing');
        await expect(page.getByText('Subscriptions are currently paused')).toBeVisible();
        await expect(page.getByRole('button', { name: /subscribe/i })).toHaveCount(0);
        await expect(page.getByRole('link', { name: 'Explore model tracking' })).toHaveAttribute('href', '/#model-history');
        await expect(page.getByRole('link', { name: 'View the published pick record' })).toHaveAttribute('href', '/record');
    });

    test('canceled checkout returns to pricing without errors', async ({ page }) => {
        await page.goto('/pricing?canceled=true');
        await expect(
            page.getByRole('heading', { name: /Simple, Transparent Pricing/i })
        ).toBeVisible();
    });

    test('navbar pricing link points to /pricing', async ({ page }) => {
        await page.goto('/');
        const link = page.getByRole('link', { name: 'Pricing' }).first();
        await expect(link).toHaveAttribute('href', '/pricing');
    });
});
