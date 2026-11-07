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
        await expect(page.getByRole('link', { name: 'Record price interest' })).toHaveCount(0);
        await page.getByRole('button', { name: 'Check availability again' }).click();
        await expect(page.getByRole('button', { name: 'Check availability again' })).toBeEnabled();
        await expect(page.getByText('Subscriptions are currently paused')).toBeVisible();
        await expect(page.getByRole('button', { name: /subscribe/i })).toHaveCount(0);
        await expect(page.getByRole('link', { name: 'Explore model tracking' })).toHaveAttribute('href', '/#model-history');
        await expect(page.getByRole('link', { name: 'View the published pick record' })).toHaveAttribute('href', '/record');
    });

    test('recovers from an availability outage without reloading or starting checkout', async ({ page }) => {
        let available = false;
        let checkoutRequests = 0;
        await page.route('**/api/bets/access', (route) => available
            ? route.fulfill({ json: { checkout_available: true } })
            : route.fulfill({ status: 503, body: 'Temporary API outage' }));
        await page.route('**/api/checkout', (route) => {
            checkoutRequests++;
            return route.abort();
        });

        await page.goto('/pricing');
        await expect(page.getByText('We couldn’t check subscription availability.')).toBeVisible();
        await expect(page.getByText('Subscriptions are currently paused')).toHaveCount(0);
        await expect(page.getByRole('button', { name: 'Sign Up & Subscribe' })).toHaveCount(0);

        available = true;
        await page.getByRole('button', { name: 'Check availability again' }).click();

        await expect(page.getByRole('button', { name: 'Sign Up & Subscribe' })).toBeEnabled();
        await expect(page).toHaveURL(/\/pricing$/);
        expect(checkoutRequests).toBe(0);
    });

    test('rechecks a paused offer once when focus and visibility change together', async ({ page }) => {
        await page.clock.install();
        let available = false;
        let reads = 0;
        await page.route('**/api/bets/access', (route) => {
            reads++;
            return route.fulfill({ json: { checkout_available: available } });
        });
        await page.goto('/pricing');
        await expect(page.getByText('Subscriptions are currently paused')).toBeVisible();
        const initialReads = reads;

        available = true;
        await page.clock.fastForward(1001);
        await page.evaluate(() => {
            window.dispatchEvent(new Event('focus'));
            document.dispatchEvent(new Event('visibilitychange'));
        });

        await expect(page.getByRole('button', { name: 'Sign Up & Subscribe' })).toBeEnabled();
        expect(reads).toBe(initialReads + 1);
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
