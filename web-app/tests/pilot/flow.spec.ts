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

test('an expired pilot session can sign in again and explicitly retry withdrawal', async ({ page, baseURL }) => {
    await page.context().addCookies([{ name: 'playwright-pilot-session', value: 'active', url: baseURL! }]);
    let withdrawals = 0;
    await page.route('**/api/interest', (route) => {
        const { action, source } = route.request().postDataJSON();
        expect(source).toBe('referral');
        if (action === 'view') return route.fulfill({ json: { joined: true, contactAllowed: true } });
        expect(action).toBe('leave');
        withdrawals++;
        return withdrawals === 1
            ? route.fulfill({ status: 401, body: 'Sign in required' })
            : route.fulfill({ json: { joined: false, contactAllowed: false } });
    });

    await page.goto('/interest?source=referral');
    const withdraw = page.getByRole('button', { name: 'Withdraw interest' });
    await withdraw.click();
    await expect(page.getByRole('alert').filter({ hasText: 'Your sign-in has expired' })).toBeVisible();

    const signIn = page.getByRole('link', { name: 'Sign in again' });
    await expect(signIn).toHaveAttribute('href', '/login?redirect_url=%2Finterest%3Fsource%3Dreferral');
    await expect(withdraw).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Withdraw email consent' })).toHaveCount(0);
    await signIn.click();
    await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();

    // Complete the return locally without a Clerk sign-in.
    const returnUrl = new URL(page.url()).searchParams.get('redirect_url');
    expect(returnUrl).toBe('/interest?source=referral');
    await page.goto(returnUrl!);
    await expect(withdraw).toBeEnabled();
    expect(withdrawals).toBe(1);

    await withdraw.click();
    await expect(page.getByRole('status')).toContainText('Your interest has been withdrawn.');
    expect(withdrawals).toBe(2);
});
