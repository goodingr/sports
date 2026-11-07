import { renderToStaticMarkup } from 'react-dom/server';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import PricingPage from './page';

vi.mock('@clerk/nextjs/server', () => ({ currentUser: vi.fn() }));
vi.mock('@/lib/billing', () => ({ configuredStripeScope: () => null, hasScopedPremium: () => false }));
vi.mock('./CheckoutButton', () => ({
    CheckoutButton: ({ interestAvailable }: { interestAvailable: boolean }) => (
        interestAvailable ? 'Interest offer available' : 'Interest offer unavailable'
    ),
}));

describe('pricing interest handoff', () => {
    beforeEach(() => {
        vi.stubEnv('NODE_ENV', 'production');
        vi.stubEnv('COMMERCIAL_PILOT_ONLY', 'false');
        vi.stubEnv('COMMERCIAL_PILOT_MODE', 'live');
        vi.stubEnv('COMMERCIAL_INTEREST_ENABLED', 'true');
        vi.stubEnv('COMMERCIAL_INTEREST_ENROLLMENT_CLOSED', 'false');
        vi.stubEnv('NEXT_PUBLIC_APP_URL', 'https://app.example.invalid');
        vi.stubEnv('NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY', 'pk_live_Y2xlcmstY2kuZXhhbXBsZS5pbnZhbGlkJA==');
        vi.stubEnv('CLERK_SECRET_KEY', 'sk_live_fixture');
    });

    afterEach(() => vi.unstubAllEnvs());

    it('offers interest on the full app when enrollment and configuration allow it', async () => {
        expect(renderToStaticMarkup(await PricingPage())).toContain('Interest offer available');
    });

    it.each([
        ['COMMERCIAL_INTEREST_ENABLED', 'false'],
        ['COMMERCIAL_INTEREST_ENROLLMENT_CLOSED', 'true'],
        ['NEXT_PUBLIC_APP_URL', 'http://app.example.invalid'],
        ['COMMERCIAL_PILOT_ONLY', 'true'],
    ])('does not offer interest when %s is %s', async (name, value) => {
        vi.stubEnv(name, value);
        expect(renderToStaticMarkup(await PricingPage())).toContain('Interest offer unavailable');
    });
});
