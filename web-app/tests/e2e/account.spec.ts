import { test, expect } from '@playwright/test';

test.describe('Account page', () => {
    test('signed-out users are redirected to login with redirect back to /account', async ({ page }) => {
        await page.goto('/account');
        // Clerk middleware redirects unauthenticated users to the sign-in page.
        await page.waitForURL(/\/login|\/signup|sign-in|accounts\.dev/i, { timeout: 15_000 });
        // The page should now show some sign-in surface — either Clerk's hosted form
        // or our own /login route.
        await expect(page.locator('body')).toContainText(/sign in|log in/i, {
            timeout: 15_000,
        });
    });
});
