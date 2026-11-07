import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { auth, currentUser } from '@clerk/nextjs/server';
import { getClerk, getStripe } from '@/lib/billing';
import { CheckoutStore } from '@/lib/checkout-store';
import { POST } from './route';

vi.mock('@clerk/nextjs/server', () => ({ auth: vi.fn(), currentUser: vi.fn() }));
vi.mock('server-only', () => ({}));
vi.mock('@/lib/billing', async (importOriginal) => ({
    ...await importOriginal<typeof import('@/lib/billing')>(),
    getAppUrl: () => 'https://app.example.test',
    getClerk: vi.fn(),
    getStripe: vi.fn(),
}));

const scope = { account: 'acct_test', livemode: false, userId: 'user_1' };
const validPrice = {
    id: 'price_test', active: true, product: { id: 'prod_test', active: true },
    currency: 'usd', unit_amount: 2900, unit_amount_decimal: '2900', livemode: false,
    type: 'recurring', billing_scheme: 'per_unit', custom_unit_amount: null, transform_quantity: null,
    recurring: { interval: 'month', interval_count: 1, usage_type: 'licensed', meter: null, trial_period_days: null },
};

type Session = {
    id: string;
    mode: string;
    status: string;
    url: string;
    customer: string;
    metadata: { userId: string };
    livemode: boolean;
    expires_at: number;
    subscription: string | null;
};

const fetchAccess = vi.fn<typeof fetch>();
const retrievePrice = vi.fn();
const retrieveAccount = vi.fn();
const createCustomer = vi.fn();
const retrieveCustomer = vi.fn();
const createSession = vi.fn();
const retrieveSession = vi.fn();
const listSessions = vi.fn();
const listSubscriptions = vi.fn();
const retrieveSubscription = vi.fn();
const updateMetadata = vi.fn();
const sessions = new Map<string, Session>();
const sessionKeys = new Map<string, string>();
const customerKeys = new Set<string>();
let directory: string;
let path: string;
let metadata: Record<string, unknown>;

async function* records<T>(rows: Iterable<T>) {
    yield* rows;
}

function sessionFor(params: Pick<Session, 'customer' | 'metadata'>, key: string): Session {
    let id = sessionKeys.get(key);
    if (!id) {
        id = `cs_test_${sessionKeys.size + 1}`;
        sessionKeys.set(key, id);
        sessions.set(id, {
            ...params, id, livemode: false, status: 'open', mode: 'subscription',
            url: `https://checkout.stripe.com/${id}`,
            expires_at: Math.floor(Date.now() / 1000) + 3600,
            subscription: null,
        });
    }
    return { ...sessions.get(id)! };
}

beforeEach(() => {
    vi.resetAllMocks();
    directory = mkdtempSync(join(tmpdir(), 'sports-checkout-route-'));
    path = join(directory, 'intents.db');
    new CheckoutStore(path).close();
    vi.stubEnv('SPORTS_CHECKOUT_DB_PATH', path);
    vi.stubEnv('STRIPE_PRICE_ID', validPrice.id);
    vi.stubGlobal('fetch', fetchAccess);
    fetchAccess.mockImplementation(async () => Response.json({ checkout_available: true }));
    vi.mocked(auth).mockResolvedValue({ userId: scope.userId } as Awaited<ReturnType<typeof auth>>);
    metadata = {};
    vi.mocked(currentUser).mockImplementation(async () => ({
        id: scope.userId, publicMetadata: { ...metadata },
    } as Awaited<ReturnType<typeof currentUser>>));
    vi.mocked(getClerk).mockResolvedValue({
        users: { updateUserMetadata: updateMetadata },
    } as unknown as Awaited<ReturnType<typeof getClerk>>);
    updateMetadata.mockImplementation(async (_, patch) => Object.assign(metadata, patch.publicMetadata));
    vi.mocked(getStripe).mockReturnValue({
        accounts: { retrieve: retrieveAccount },
        prices: { retrieve: retrievePrice },
        customers: { create: createCustomer, retrieve: retrieveCustomer },
        checkout: { sessions: { create: createSession, retrieve: retrieveSession, list: listSessions } },
        subscriptions: { list: listSubscriptions, retrieve: retrieveSubscription },
    } as unknown as ReturnType<typeof getStripe>);
    sessions.clear();
    sessionKeys.clear();
    customerKeys.clear();
    retrieveAccount.mockResolvedValue({ id: scope.account, charges_enabled: true });
    retrievePrice.mockResolvedValue(validPrice);
    createCustomer.mockImplementation(async (params, { idempotencyKey }) => {
        customerKeys.add(idempotencyKey);
        return { id: 'cus_test', livemode: false, metadata: params.metadata };
    });
    retrieveCustomer.mockImplementation(async (id) => ({
        id, livemode: false, metadata: { userId: scope.userId },
    }));
    createSession.mockImplementation(async (params, { idempotencyKey }) => sessionFor(params, idempotencyKey));
    retrieveSession.mockImplementation(async (id) => sessions.get(id));
    listSessions.mockImplementation(() => records(sessions.values()));
    listSubscriptions.mockImplementation(() => records([]));
});

afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
    rmSync(directory, { recursive: true, force: true });
});

describe('POST /api/checkout', () => {
    it('closes pilot billing before auth, pick availability, or Stripe', async () => {
        vi.stubEnv('COMMERCIAL_PILOT_ONLY', 'true');

        expect((await POST()).status).toBe(503);
        expect(auth).not.toHaveBeenCalled();
        expect(fetchAccess).not.toHaveBeenCalled();
        expect(getStripe).not.toHaveBeenCalled();
    });

    it.each([false, 'true', undefined])('blocks Stripe when availability is %s', async (checkout_available) => {
        fetchAccess.mockResolvedValue(Response.json({ checkout_available }));

        expect((await POST()).status).toBe(503);
        expect(currentUser).not.toHaveBeenCalled();
        expect(createCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it('blocks Stripe when the picks API is unreachable', async () => {
        fetchAccess.mockRejectedValue(new Error('unreachable'));

        expect((await POST()).status).toBe(503);
        expect(getStripe).not.toHaveBeenCalled();
    });

    it('creates the checkout only after verifying the configured price', async () => {
        const response = await POST();

        expect(response.status).toBe(200);
        expect(response.headers.get('cache-control')).toBe('no-store');
        expect(await response.json()).toEqual({ url: 'https://checkout.stripe.com/cs_test_1' });
        expect(fetchAccess).toHaveBeenCalledWith(expect.stringContaining('/api/bets/access'), expect.objectContaining({ cache: 'no-store' }));
        expect(retrievePrice).toHaveBeenCalledExactlyOnceWith(validPrice.id, { expand: ['product'] });
        expect(retrievePrice).toHaveBeenCalledBefore(createCustomer);
        expect(createCustomer).toHaveBeenCalledExactlyOnceWith({ metadata: { userId: scope.userId } }, expect.objectContaining({ idempotencyKey: expect.any(String) }));
        expect(createSession).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({
            customer: 'cus_test', mode: 'subscription', currency: 'usd',
            adaptive_pricing: { enabled: false }, line_items: [{ price: validPrice.id, quantity: 1 }],
        }), expect.objectContaining({ idempotencyKey: expect.any(String) }));
    });

    it('blocks an incorrect price or missing price verification', async () => {
        retrievePrice.mockResolvedValueOnce({ ...validPrice, unit_amount: 5900 });
        retrievePrice.mockRejectedValueOnce(new Error('Price lookup unavailable'));

        expect((await POST()).status).toBe(503);
        expect((await POST()).status).toBe(503);
        expect(createCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it('refuses a missing checkout store without creating anything in Stripe', async () => {
        vi.stubEnv('SPORTS_CHECKOUT_DB_PATH', join(directory, 'missing.db'));

        expect((await POST()).status).toBe(503);
        expect(createCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it('blocks a second checkout for an existing Clerk subscriber', async () => {
        metadata.is_premium = true;

        expect((await POST()).status).toBe(409);
        expect(createSession).not.toHaveBeenCalled();
    });

    it('converges racing requests and later requests on one customer and session', async () => {
        const responses = await Promise.all([POST(), POST(), POST()]);
        const later = await POST();

        expect(responses.map(r => r.status)).toEqual([200, 200, 200]);
        expect(later.status).toBe(200);
        expect(customerKeys.size).toBe(1);
        expect(sessionKeys.size).toBe(1);
        expect(await later.json()).toEqual({ url: 'https://checkout.stripe.com/cs_test_1' });
    });

    it('resumes a lost customer response and a lost Clerk update without another customer', async () => {
        createCustomer.mockImplementationOnce(async (_, { idempotencyKey }) => {
            customerKeys.add(idempotencyKey);
            throw new Error('Customer response lost');
        });
        updateMetadata.mockRejectedValueOnce(new Error('Clerk update failed'));

        expect((await POST()).status).toBe(503);
        expect((await POST()).status).toBe(503);
        expect((await POST()).status).toBe(200);
        expect(createCustomer).toHaveBeenCalledTimes(2);
        expect(customerKeys.size).toBe(1);
        expect(sessionKeys.size).toBe(1);
    });

    it('rechecks Stripe state after recovering a lost session creation response', async () => {
        createSession.mockImplementationOnce(async (params, { idempotencyKey }) => {
            sessionFor(params, idempotencyKey);
            throw new Error('Session response lost');
        });

        expect((await POST()).status).toBe(503);
        const session = sessions.get('cs_test_1')!;
        session.status = 'complete';
        session.subscription = 'sub_test';
        retrieveSubscription.mockResolvedValue({ id: 'sub_test', customer: 'cus_test', status: 'active', livemode: false });

        expect((await POST()).status).toBe(409);
        expect(sessionKeys.size).toBe(1);
        expect(retrieveSession).toHaveBeenCalledWith('cs_test_1');
    });

    it('rotates after provider-confirmed expiry and converges again', async () => {
        expect((await POST()).status).toBe(200);
        sessions.get('cs_test_1')!.status = 'expired';

        const responses = await Promise.all([POST(), POST()]);

        expect(responses.map(r => r.status)).toEqual([200, 200]);
        expect(await responses[0].json()).toEqual({ url: 'https://checkout.stripe.com/cs_test_2' });
        expect(sessionKeys.size).toBe(2);
        expect(customerKeys.size).toBe(1);
    });

    it('checks a legacy customer and blocks while Stripe has a nonterminal subscription', async () => {
        metadata.stripe_customer_id = 'cus_legacy';
        listSubscriptions.mockImplementation(() => records([{ status: 'past_due' }]));

        expect((await POST()).status).toBe(409);
        expect(retrieveCustomer).toHaveBeenCalledWith('cus_legacy');
        expect(createCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it('blocks a legacy open subscription checkout before reserving a replacement', async () => {
        metadata.stripe_customer_id = 'cus_legacy';
        listSessions.mockImplementation(() => records([{ mode: 'subscription', status: 'open' }]));

        expect((await POST()).status).toBe(409);
        expect(listSessions).toHaveBeenCalledWith({ customer: 'cus_legacy', limit: 100 });
        expect(createSession).not.toHaveBeenCalled();
    });

    it('blocks a legacy checkout that completed before its subscription was known', async () => {
        metadata.stripe_customer_id = 'cus_legacy';
        listSessions.mockImplementation(() => records([
            { id: 'cs_legacy', mode: 'subscription', status: 'complete', subscription: null },
        ]));

        expect((await POST()).status).toBe(409);
        expect(createSession).not.toHaveBeenCalled();
    });

    it('replaces a completed checkout only after the provider confirms its subscription ended', async () => {
        expect((await POST()).status).toBe(200);
        const session = sessions.get('cs_test_1')!;
        session.status = 'complete';
        expect((await POST()).status).toBe(409);
        session.subscription = 'sub_test';
        retrieveSubscription.mockResolvedValue({ id: 'sub_test', customer: 'cus_test', status: 'canceled', livemode: false });

        const response = await POST();

        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({ url: 'https://checkout.stripe.com/cs_test_2' });
    });

    it('does not adopt an unrelated Clerk customer reference', async () => {
        metadata.stripe_customer_id = 'cus_legacy';
        retrieveCustomer.mockResolvedValue({ id: 'cus_legacy', livemode: false, metadata: { userId: 'user_other' } });

        expect((await POST()).status).toBe(503);
        expect(createCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });
});
