import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { auth, currentUser } from '@clerk/nextjs/server';
import { getOrCreateCustomer, getStripe } from '@/lib/billing';
import { POST } from './route';

vi.mock('@clerk/nextjs/server', () => ({ auth: vi.fn(), currentUser: vi.fn() }));
vi.mock('@/lib/billing', async (importOriginal) => ({
    ...await importOriginal<typeof import('@/lib/billing')>(),
    getAppUrl: () => 'https://app.example.test',
    getOrCreateCustomer: vi.fn(),
    getStripe: vi.fn(),
}));

const fetchAccess = vi.fn<typeof fetch>();
const retrievePrice = vi.fn();
const createSession = vi.fn();
const validPrice = {
    id: 'price_test', active: true, product: { id: 'prod_test', active: true },
    currency: 'usd', unit_amount: 2900, unit_amount_decimal: '2900',
    type: 'recurring', billing_scheme: 'per_unit', custom_unit_amount: null, transform_quantity: null,
    recurring: { interval: 'month', interval_count: 1, usage_type: 'licensed', meter: null, trial_period_days: null },
};

beforeEach(() => {
    vi.resetAllMocks();
    vi.stubGlobal('fetch', fetchAccess);
    vi.stubEnv('STRIPE_PRICE_ID', validPrice.id);
    vi.mocked(auth).mockResolvedValue({ userId: 'user_1' } as Awaited<ReturnType<typeof auth>>);
    vi.mocked(currentUser).mockResolvedValue({
        emailAddresses: [{ emailAddress: 'buyer@example.test' }],
        publicMetadata: {},
    } as Awaited<ReturnType<typeof currentUser>>);
    vi.mocked(getOrCreateCustomer).mockResolvedValue('cus_test');
    vi.mocked(getStripe).mockReturnValue({
        prices: { retrieve: retrievePrice },
        checkout: { sessions: { create: createSession } },
    } as unknown as ReturnType<typeof getStripe>);
    retrievePrice.mockResolvedValue(validPrice);
    createSession.mockResolvedValue({ url: 'https://checkout.stripe.com/test' });
});

afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
});

describe('POST /api/checkout', () => {
    it('closes pilot billing before auth, pick availability, or Stripe', async () => {
        vi.stubEnv('COMMERCIAL_PILOT_ONLY', 'true');
        fetchAccess.mockResolvedValue(Response.json({ checkout_available: true }));

        expect((await POST()).status).toBe(503);
        expect(auth).not.toHaveBeenCalled();
        expect(fetchAccess).not.toHaveBeenCalled();
        expect(getStripe).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it.each([false, 'true', undefined])('blocks Stripe when availability is %s', async (checkout_available) => {
        fetchAccess.mockResolvedValue(Response.json({ checkout_available }));

        const response = await POST();

        expect(response.status).toBe(503);
        expect(currentUser).not.toHaveBeenCalled();
        expect(getOrCreateCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it('blocks Stripe when the picks API is unreachable', async () => {
        fetchAccess.mockRejectedValue(new Error('unreachable'));

        expect((await POST()).status).toBe(503);
        expect(getStripe).not.toHaveBeenCalled();
    });

    it('creates the checkout only after verifying the configured price', async () => {
        fetchAccess.mockResolvedValue(Response.json({ checkout_available: true }));

        const response = await POST();

        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({ url: 'https://checkout.stripe.com/test' });
        expect(fetchAccess).toHaveBeenCalledWith(expect.stringContaining('/api/bets/access'), expect.objectContaining({ cache: 'no-store' }));
        expect(retrievePrice).toHaveBeenCalledExactlyOnceWith(validPrice.id, { expand: ['product'] });
        expect(retrievePrice).toHaveBeenCalledBefore(getOrCreateCustomer);
        expect(getOrCreateCustomer).toHaveBeenCalledWith('user_1', 'buyer@example.test');
        expect(createSession).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({
            customer: 'cus_test', mode: 'subscription', currency: 'usd',
            adaptive_pricing: { enabled: false },
            line_items: [{ price: validPrice.id, quantity: 1 }],
        }));
    });

    it('blocks customer and session creation when the configured price differs from the offer', async () => {
        fetchAccess.mockResolvedValue(Response.json({ checkout_available: true }));
        retrievePrice.mockResolvedValue({ ...validPrice, unit_amount: 5900, unit_amount_decimal: '5900' });

        expect((await POST()).status).toBe(503);
        expect(getOrCreateCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it('blocks customer and session creation when Stripe cannot verify the price', async () => {
        fetchAccess.mockResolvedValue(Response.json({ checkout_available: true }));
        retrievePrice.mockRejectedValue(new Error('Price lookup unavailable'));

        expect((await POST()).status).toBe(503);
        expect(getOrCreateCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it('blocks a second checkout for an existing subscriber', async () => {
        fetchAccess.mockResolvedValue(Response.json({ checkout_available: true }));
        vi.mocked(currentUser).mockResolvedValue({
            publicMetadata: { is_premium: true },
        } as Awaited<ReturnType<typeof currentUser>>);

        expect((await POST()).status).toBe(409);
        expect(createSession).not.toHaveBeenCalled();
    });
});
