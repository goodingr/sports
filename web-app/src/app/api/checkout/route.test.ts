import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { auth, currentUser } from '@clerk/nextjs/server';
import { getOrCreateCustomer, getStripe } from '@/lib/billing';
import { POST } from './route';

vi.mock('@clerk/nextjs/server', () => ({ auth: vi.fn(), currentUser: vi.fn() }));
vi.mock('@/lib/billing', () => ({
    getAppUrl: () => 'https://app.example.test',
    getOrCreateCustomer: vi.fn(),
    getStripe: vi.fn(),
    getStripePriceId: () => 'price_test',
}));

const fetchAccess = vi.fn<typeof fetch>();
const createSession = vi.fn();

beforeEach(() => {
    vi.resetAllMocks();
    vi.stubGlobal('fetch', fetchAccess);
    vi.mocked(auth).mockResolvedValue({ userId: 'user_1' } as Awaited<ReturnType<typeof auth>>);
    vi.mocked(currentUser).mockResolvedValue({
        emailAddresses: [{ emailAddress: 'buyer@example.test' }],
        publicMetadata: {},
    } as Awaited<ReturnType<typeof currentUser>>);
    vi.mocked(getOrCreateCustomer).mockResolvedValue('cus_test');
    vi.mocked(getStripe).mockReturnValue({
        checkout: { sessions: { create: createSession } },
    } as unknown as ReturnType<typeof getStripe>);
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

    it('creates the checkout only with confirmed availability', async () => {
        fetchAccess.mockResolvedValue(Response.json({ checkout_available: true }));

        const response = await POST();

        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({ url: 'https://checkout.stripe.com/test' });
        expect(fetchAccess).toHaveBeenCalledWith(expect.stringContaining('/api/bets/access'), expect.objectContaining({ cache: 'no-store' }));
        expect(getOrCreateCustomer).toHaveBeenCalledWith('user_1', 'buyer@example.test');
        expect(createSession).toHaveBeenCalledOnce();
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
