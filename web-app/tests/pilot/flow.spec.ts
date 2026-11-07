import { expect, test } from '@playwright/test';

test('the pilot entry preserves source attribution and serves legal contact pages without the picks API', async ({ page }) => {
    const picksRequests: string[] = [];
    page.on('request', (request) => {
        if (new URL(request.url()).pathname.startsWith('/api/bets/')) picksRequests.push(request.url());
    });

    await page.goto('/?source=referral');
    await expect(page).toHaveURL(/\/interest\?source=referral$/);
    await expect(page.getByText('$29/month')).toBeVisible();
    await expect(page.getByRole('link', { name: 'Sign in to record interest' })).toHaveAttribute(
        'href', '/login?redirect_url=%2Finterest%3Fsource%3Dreferral',
    );
    await expect(page.getByText(/first signed-in offer visit and link source/)).toBeVisible();
    await page.getByRole('link', { name: 'Read how we use pilot data' }).click();
    await expect(page).toHaveURL(/\/privacy#pilot-data$/);
    await expect(page.getByRole('heading', { name: 'Privacy Policy' })).toBeVisible();
    await expect(page.getByText(/first signed-in view time/)).toBeVisible();
    await expect(page.getByRole('link', { name: 'pilot-contact@example.invalid' }).first()).toHaveAttribute(
        'href', 'mailto:pilot-contact@example.invalid',
    );
    await page.getByRole('link', { name: 'Terms of Service' }).click();
    await expect(page.getByRole('heading', { name: 'Terms of Service' })).toBeVisible();
    await expect(page.getByRole('link', { name: 'pilot-contact@example.invalid' })).toBeVisible();
    expect(picksRequests).toEqual([]);
});

test('former sales pages return to interest and billing endpoints stay closed', async ({ request }) => {
    for (const path of ['/pricing', '/record', '/account?success=true']) {
        const response = await request.get(path, { maxRedirects: 0 });
        expect(response.status()).toBe(307);
        expect(new URL(response.headers().location, response.url()).pathname).toBe('/interest');
    }

    for (const path of ['/api/checkout', '/api/billing/portal', '/api/webhooks/stripe']) {
        const response = await request.post(path, {
            headers: { Authorization: 'Bearer invalid-pilot-test-token' },
        });
        expect(response.status()).toBe(503);
        expect(response.headers()['cache-control']).toBe('no-store');
    }
});
