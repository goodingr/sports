import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { auth, currentUser } from '@clerk/nextjs/server';
import Stripe from 'stripe';
import { reconcileCheckoutReturn } from '@/app/account/checkout-return';
import { POST as billingPortal } from '@/app/api/billing/portal/route';
import { POST as stripeWebhook } from '@/app/api/webhooks/stripe/route';
import { configuredStripeScope, getClerk, getStripe, hasScopedPremium, setClerkForTests } from '@/lib/billing';
import { CheckoutStore, type CheckoutSessionParams } from '@/lib/checkout-store';
import { POST } from './route';

vi.mock('@clerk/nextjs/server', () => ({ auth: vi.fn(), currentUser: vi.fn() }));
vi.mock('server-only', () => ({}));
vi.mock('next/headers', () => ({ headers: async () => new Headers({ 'stripe-signature': signature }) }));
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
    currency: string;
    amount_total: number;
    payment_status: 'paid' | 'unpaid';
    line_items: { has_more: boolean; data: { price: { id: string }; quantity: number }[] };
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
const createPortalSession = vi.fn();
const updateMetadata = vi.fn();
const sessions = new Map<string, Session>();
const sessionKeys = new Map<string, string>();
const customerKeys = new Set<string>();
let directory: string;
let path: string;
let metadata: Record<string, unknown>;
let privateMetadata: Record<string, unknown>;
let signature: string;
const webhookSecret = 'whsec_checkout_return_fixture';
const webhooks = new Stripe('sk_test_checkout_return_fixture').webhooks;

async function* records<T>(rows: Iterable<T>) {
    yield* rows;
}

function sessionFor(params: CheckoutSessionParams, key: string): Session {
    let id = sessionKeys.get(key);
    if (!id) {
        const livemode = configuredStripeScope()!.livemode;
        id = `cs_${livemode ? 'live' : 'test'}_${sessionKeys.size + 1}`;
        sessionKeys.set(key, id);
        sessions.set(id, {
            id, customer: params.customer, metadata: params.metadata, currency: params.currency,
            livemode, status: 'open', mode: 'subscription',
            url: `https://checkout.stripe.com/${id}`,
            expires_at: Math.floor(Date.now() / 1000) + 3600,
            subscription: null,
            amount_total: 2900,
            payment_status: 'unpaid',
            line_items: {
                has_more: false,
                data: params.line_items.map(({ price, quantity }) => ({ price: { id: price }, quantity })),
            },
        });
    }
    return { ...sessions.get(id)! };
}

async function completeCheckout() {
    expect((await POST()).status).toBe(200);
    const session = sessions.get('cs_test_1')!;
    const sub = {
        id: 'sub_test', customer: session.customer, status: 'active', livemode: session.livemode,
        created: 1_800_000_000, metadata: session.metadata,
        items: { data: [{ price: { id: validPrice.id }, current_period_end: 1_803_000_000 }] },
    };
    Object.assign(session, { status: 'complete', subscription: sub.id, payment_status: 'paid' });
    retrieveSubscription.mockResolvedValue(sub);
    listSubscriptions.mockImplementation(({ customer }) => records(customer === sub.customer ? [sub] : []));
    return { session, sub };
}

beforeEach(() => {
    vi.resetAllMocks();
    directory = mkdtempSync(join(tmpdir(), 'sports-checkout-route-'));
    path = join(directory, 'intents.db');
    new CheckoutStore(path).close();
    vi.stubEnv('SPORTS_CHECKOUT_DB_PATH', path);
    vi.stubEnv('STRIPE_PRICE_ID', validPrice.id);
    vi.stubEnv('STRIPE_ACCOUNT_ID', scope.account);
    vi.stubEnv('STRIPE_SECRET_KEY', 'sk_test_checkout_return_fixture');
    vi.stubEnv('STRIPE_WEBHOOK_SECRET', webhookSecret);
    vi.stubGlobal('fetch', fetchAccess);
    fetchAccess.mockImplementation(async () => Response.json({ checkout_available: true }));
    vi.mocked(auth).mockResolvedValue({ userId: scope.userId } as Awaited<ReturnType<typeof auth>>);
    metadata = {};
    privateMetadata = {};
    signature = '';
    vi.mocked(currentUser).mockImplementation(async () => ({
        id: scope.userId, publicMetadata: { ...metadata },
    } as Awaited<ReturnType<typeof currentUser>>));
    const clerk = {
        users: {
            getUser: async () => ({ publicMetadata: { ...metadata }, privateMetadata: { ...privateMetadata } }),
            updateUserMetadata: updateMetadata,
        },
    } as unknown as Awaited<ReturnType<typeof getClerk>>;
    vi.mocked(getClerk).mockResolvedValue(clerk);
    setClerkForTests(clerk);
    updateMetadata.mockImplementation(async (_, patch) => {
        Object.assign(metadata, patch.publicMetadata);
        Object.assign(privateMetadata, patch.privateMetadata);
    });
    vi.mocked(getStripe).mockReturnValue({
        accounts: { retrieve: retrieveAccount },
        prices: { retrieve: retrievePrice },
        customers: { create: createCustomer, retrieve: retrieveCustomer },
        checkout: { sessions: { create: createSession, retrieve: retrieveSession, list: listSessions } },
        subscriptions: { list: listSubscriptions, retrieve: retrieveSubscription },
        billingPortal: { sessions: { create: createPortalSession } },
        webhooks,
    } as unknown as ReturnType<typeof getStripe>);
    sessions.clear();
    sessionKeys.clear();
    customerKeys.clear();
    retrieveAccount.mockResolvedValue({ id: scope.account, charges_enabled: true });
    retrievePrice.mockResolvedValue(validPrice);
    createCustomer.mockImplementation(async (params, { idempotencyKey }) => {
        customerKeys.add(idempotencyKey);
        const livemode = configuredStripeScope()!.livemode;
        return { id: livemode ? 'cus_live' : 'cus_test', livemode, metadata: params.metadata };
    });
    retrieveCustomer.mockImplementation(async (id) => ({
        id, livemode: configuredStripeScope()!.livemode, metadata: { userId: scope.userId },
    }));
    createSession.mockImplementation(async (params, { idempotencyKey }) => sessionFor(params, idempotencyKey));
    retrieveSession.mockImplementation(async (id) => sessions.get(id));
    listSessions.mockImplementation(({ customer }) => records([...sessions.values()].filter((session) => session.customer === customer)));
    listSubscriptions.mockImplementation(() => records([]));
    createPortalSession.mockResolvedValue({ url: 'https://billing.stripe.com/session/fixture' });
});

afterEach(() => {
    setClerkForTests(null);
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
            success_url: 'https://app.example.test/account?success=true&session_id={CHECKOUT_SESSION_ID}',
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
        Object.assign(metadata, {
            is_premium: true, stripe_customer_id: 'cus_test',
            stripe_customer_account_id: scope.account, stripe_customer_livemode: scope.livemode,
            stripe_entitlement: {
                customer_id: 'cus_test', account_id: scope.account, livemode: scope.livemode, price_id: validPrice.id,
            },
        });

        expect((await POST()).status).toBe(409);
        expect(createSession).not.toHaveBeenCalled();
    });

    it('lets the same Clerk user enter live checkout after an actual test entitlement', async () => {
        const { session } = await completeCheckout();
        expect(await reconcileCheckoutReturn(session.id)).toBe(true);
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(true);

        vi.stubEnv('STRIPE_SECRET_KEY', 'rk_live_checkout_fixture');
        vi.stubEnv('STRIPE_PRICE_ID', 'price_live');
        retrievePrice.mockResolvedValue({ ...validPrice, id: 'price_live', livemode: true });
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);
        retrieveCustomer.mockClear();
        retrieveSession.mockClear();

        const response = await POST();
        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({ url: 'https://checkout.stripe.com/cs_live_2' });
        expect(metadata).toMatchObject({
            stripe_customer_id: 'cus_live', stripe_customer_account_id: scope.account, stripe_customer_livemode: true,
        });
        expect(retrieveCustomer).not.toHaveBeenCalledWith('cus_test');
        expect(await reconcileCheckoutReturn(session.id)).toBe(false);
        expect(retrieveSession).not.toHaveBeenCalledWith(session.id, { expand: ['line_items'] });
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);
    });

    it('verifies the account and price mode before creating a customer or checkout', async () => {
        retrieveAccount.mockResolvedValueOnce({ id: 'acct_other', charges_enabled: true });
        expect((await POST()).status).toBe(503);
        retrievePrice.mockResolvedValueOnce({ ...validPrice, livemode: true });
        expect((await POST()).status).toBe(503);
        expect(createCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
    });

    it('keeps the canonical customer and duplicate guard when only the price changes', async () => {
        const { session } = await completeCheckout();
        expect(await reconcileCheckoutReturn(session.id)).toBe(true);
        vi.stubEnv('STRIPE_PRICE_ID', 'price_new');
        retrievePrice.mockResolvedValue({ ...validPrice, id: 'price_new' });
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);

        const response = await POST();
        expect(response.status).toBe(409);
        expect(await response.text()).toBe('Subscription or payment already in progress');
        expect(createCustomer).toHaveBeenCalledTimes(1);
        expect(createSession).toHaveBeenCalledTimes(1);
        expect(metadata.stripe_customer_id).toBe(session.customer);
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);
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

    it('upgrades a matching unscoped live subscriber with provider evidence and creates no duplicate', async () => {
        vi.stubEnv('STRIPE_SECRET_KEY', 'sk_live_existing_fixture');
        vi.stubEnv('STRIPE_PRICE_ID', 'price_live');
        retrievePrice.mockResolvedValue({ ...validPrice, id: 'price_live', livemode: true });
        Object.assign(metadata, { stripe_customer_id: 'cus_legacy', is_premium: true });
        const sub = {
            id: 'sub_existing', customer: 'cus_legacy', status: 'active', livemode: true,
            created: 1_800_000_000, metadata: { userId: scope.userId },
            items: { data: [{ price: { id: 'price_live' }, current_period_end: 1_803_000_000 }] },
        };
        listSubscriptions.mockImplementation(() => records([sub]));

        expect((await POST()).status).toBe(409);
        expect(retrieveCustomer).toHaveBeenCalledWith('cus_legacy');
        expect(createCustomer).not.toHaveBeenCalled();
        expect(createSession).not.toHaveBeenCalled();
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(true);
    });

    it('rebinds an unscoped customer only after Stripe confirms it is missing', async () => {
        Object.assign(metadata, { stripe_customer_id: 'cus_legacy', is_premium: true });
        retrieveCustomer.mockRejectedValueOnce(new Error('Stripe unavailable'));
        expect((await POST()).status).toBe(503);
        expect(createCustomer).not.toHaveBeenCalled();
        expect(updateMetadata).not.toHaveBeenCalled();

        retrieveCustomer.mockRejectedValueOnce(new Stripe.errors.StripeInvalidRequestError({
            type: 'invalid_request_error', code: 'resource_missing', statusCode: 404,
            message: 'No such customer',
        }));
        expect((await POST()).status).toBe(200);
        expect(createCustomer).toHaveBeenCalledTimes(1);
        expect(metadata).toMatchObject({
            stripe_customer_id: 'cus_test', stripe_customer_account_id: scope.account, stripe_customer_livemode: false,
        });
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);
    });

    it.each([
        { stripe_customer_id: 'cus_legacy', stripe_customer_account_id: 'acct_other', stripe_customer_livemode: 'false' },
        { stripe_customer_id: 'cus_legacy', stripe_customer_account_id: 'invalid', stripe_customer_livemode: true },
        { stripe_customer_id: 'invalid', stripe_customer_account_id: 'acct_other', stripe_customer_livemode: true },
    ])('fails closed on a malformed canonical customer binding', async (binding) => {
        Object.assign(metadata, binding);
        expect((await POST()).status).toBe(503);
        expect(retrieveCustomer).not.toHaveBeenCalled();
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

    it('replays an older reservation with its original success URL and idempotency key', async () => {
        const params: CheckoutSessionParams = {
            customer: 'cus_test', mode: 'subscription', currency: 'usd', adaptive_pricing: { enabled: false },
            success_url: 'https://app.example.test/account?success=true',
            cancel_url: 'https://app.example.test/pricing?canceled=true',
            payment_method_types: ['card'], billing_address_collection: 'auto',
            line_items: [{ price: validPrice.id, quantity: 1 }],
            metadata: { userId: scope.userId }, subscription_data: { metadata: { userId: scope.userId } },
        };
        const store = new CheckoutStore(path, { requireExisting: true });
        store.reserveCustomer(scope, params, params.customer);
        const attempt = store.reserveSession(scope, params);
        store.close();
        metadata.stripe_customer_id = params.customer;

        expect((await POST()).status).toBe(200);
        expect(createSession).toHaveBeenCalledExactlyOnceWith(params, { idempotencyKey: attempt.key });
    });
});

describe('authenticated Checkout return', () => {
    it('reconciles the exact paid session before any webhook and retains signed cancellation handling', async () => {
        const { session, sub } = await completeCheckout();
        expect(metadata.is_premium).toBeUndefined();

        expect(await reconcileCheckoutReturn(session.id)).toBe(true);
        expect(metadata).toMatchObject({ is_premium: true, subscription_status: 'active', stripe_customer_id: session.customer });
        expect(retrieveSession).toHaveBeenCalledWith(session.id, { expand: ['line_items'] });
        const writes = updateMetadata.mock.calls.length;
        expect(await reconcileCheckoutReturn(session.id)).toBe(true);
        expect(updateMetadata).toHaveBeenCalledTimes(writes);
        expect((await POST()).status).toBe(409);
        expect(sessionKeys.size).toBe(1);

        sub.status = 'canceled';
        const body = JSON.stringify({
            id: 'evt_cancel_return_fixture', livemode: false, type: 'customer.subscription.deleted', data: { object: sub },
        });
        signature = webhooks.generateTestHeaderString({ payload: body, secret: webhookSecret });
        expect((await stripeWebhook(new Request('https://app.example.test/api/webhooks/stripe', { method: 'POST', body }))).status).toBe(200);
        expect(metadata).toMatchObject({ is_premium: false, subscription_status: 'canceled' });
        expect(await reconcileCheckoutReturn(session.id)).toBe(false);
    });

    it.each<[string, Partial<Session>]>([
        ['a different customer', { customer: 'cus_other' }],
        ['a session tagged for another user', { metadata: { userId: 'user_other' } }],
        ['an open session', { status: 'open' }],
        ['an unpaid session', { payment_status: 'unpaid' }],
        ['a zero-dollar session', { amount_total: 0 }],
        ['an unresolved subscription', { subscription: null }],
        ['an unrelated price', { line_items: { has_more: false, data: [{ price: { id: 'price_other' }, quantity: 1 }] } }],
    ])('leaves entitlements untouched for %s', async (_, overrides) => {
        const { session } = await completeCheckout();
        Object.assign(session, overrides);
        const writes = updateMetadata.mock.calls.length;

        expect(await reconcileCheckoutReturn(session.id)).toBe(false);
        expect(updateMetadata).toHaveBeenCalledTimes(writes);
        expect(metadata.is_premium).toBeUndefined();
    });

    it('waits until Stripe confirms the attached subscription in current account state', async () => {
        const { session, sub } = await completeCheckout();
        listSubscriptions.mockImplementation(() => records([{ ...sub, id: 'sub_other' }]));

        expect(await reconcileCheckoutReturn(session.id)).toBe(false);
        expect(metadata.is_premium).toBeUndefined();
    });

    it('rejects another signed-in account replaying a real session ID', async () => {
        const { session } = await completeCheckout();
        // The fake Clerk account shares the Stripe ID to prove the session's owner tag is checked too.
        vi.mocked(auth).mockResolvedValueOnce({ userId: 'user_other' } as Awaited<ReturnType<typeof auth>>);
        const writes = updateMetadata.mock.calls.length;

        expect(await reconcileCheckoutReturn(session.id)).toBe(false);
        expect(updateMetadata).toHaveBeenCalledTimes(writes);
    });

    it('fails closed without Stripe credentials or a signed-in account', async () => {
        vi.mocked(getStripe).mockImplementationOnce(() => { throw new Error('Stripe is not configured'); });
        expect(await reconcileCheckoutReturn('cs_test_1')).toBe(false);
        vi.mocked(auth).mockResolvedValueOnce({ userId: null } as Awaited<ReturnType<typeof auth>>);
        expect(await reconcileCheckoutReturn('cs_test_1')).toBe(false);
        expect(await reconcileCheckoutReturn('invalid-session')).toBe(false);
        expect(retrieveSession).not.toHaveBeenCalled();
        expect(updateMetadata).not.toHaveBeenCalled();
    });
});

describe('authenticated billing portal', () => {
    it('rejects a known foreign scope before looking up the customer', async () => {
        Object.assign(metadata, {
            stripe_customer_id: 'cus_wrong_scope',
            stripe_customer_account_id: scope.account,
            stripe_customer_livemode: true,
        });
        expect((await billingPortal()).status).toBe(404);
        expect(retrieveCustomer).not.toHaveBeenCalled();
        expect(createPortalSession).not.toHaveBeenCalled();
    });

    it('allows a legacy customer only after verifying its provider account, mode and owner', async () => {
        metadata.stripe_customer_id = 'cus_legacy';
        retrieveCustomer.mockResolvedValueOnce({ id: 'cus_legacy', livemode: true, metadata: { userId: scope.userId } });
        expect((await billingPortal()).status).toBe(404);
        expect(createPortalSession).not.toHaveBeenCalled();

        expect((await billingPortal()).status).toBe(200);
        expect(createPortalSession).toHaveBeenCalledExactlyOnceWith({
            customer: 'cus_legacy', return_url: 'https://app.example.test/account',
        });
    });
});
