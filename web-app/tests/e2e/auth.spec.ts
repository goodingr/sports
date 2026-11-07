import { test, expect } from '@playwright/test';

test.describe('Signed-out auth surfaces', () => {
    test('login renders the sign-in surface', async ({ page }) => {
        await page.goto('/login');
        await expect(page.locator('body')).toContainText(/sign in/i, { timeout: 15_000 });
    });

    test('signup renders the sign-up surface', async ({ page }) => {
        await page.goto('/signup');
        await expect(page.locator('body')).toContainText(/sign up|create your account/i, {
            timeout: 15_000,
        });
    });

    test('navbar Get Started CTA links to signup', async ({ page }) => {
        await page.goto('/');
        const cta = page.getByRole('link', { name: /Get Started/i }).first();
        await expect(cta).toHaveAttribute('href', '/signup');
    });

    test('signed-out checkout is rejected before billing', async ({ request }) => {
        const response = await request.post('/api/checkout');
        expect(response.status()).toBe(401);
    });

    test('identity fixtures reject spoofed session input on protected routes', async ({ request }) => {
        const headers = {
            Authorization: 'Bearer invalid-browser-test-token',
            Cookie: '__session=invalid-browser-test-token; __client_uat=9999999999',
        };
        const account = await request.get('/account?success=true', { headers, maxRedirects: 0 });
        expect(account.status()).toBe(307);
        const login = new URL(account.headers().location, account.url());
        expect(login.pathname).toBe('/login');
        expect(login.searchParams.get('redirect_url')).toBe('/account');

        const checkout = await request.post('/api/checkout', { headers });
        expect(checkout.status()).toBe(401);
    });
});
