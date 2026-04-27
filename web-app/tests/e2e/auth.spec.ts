import { test, expect } from '@playwright/test';

test.describe('Auth pages', () => {
    test('login renders Clerk sign-in form', async ({ page }) => {
        await page.goto('/login');
        // Clerk renders an iframe-free form; either the form or its skeleton must appear.
        await expect(page.locator('body')).toContainText(/sign in/i, { timeout: 15_000 });
    });

    test('signup renders Clerk sign-up form', async ({ page }) => {
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
});
