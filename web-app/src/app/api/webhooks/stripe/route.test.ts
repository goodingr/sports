import { afterEach, describe, it, expect, beforeEach, vi } from "vitest";
import Stripe from "stripe";
import { paidInvoiceFixture } from "@/lib/stripe-paid-invoice.fixture";

vi.mock("server-only", () => ({}));

process.env.STRIPE_SECRET_KEY = "sk_test_dummy";
process.env.STRIPE_WEBHOOK_SECRET = "whsec_test_secret";
process.env.STRIPE_PRICE_ID = "price_test";
process.env.STRIPE_ACCOUNT_ID = "acct_fixture";
process.env.NEXT_PUBLIC_APP_URL = "http://localhost:3000";

const headersStore = new Map<string, string>();
vi.mock("next/headers", () => ({
    headers: async () => ({
        get: (name: string) => headersStore.get(name.toLowerCase()) ?? null,
    }),
}));

import { POST } from "./route";
import {
    PREMIUM_KEY,
    setClerkForTests,
    setStripeForTests,
} from "@/lib/billing";

type StoredUser = {
    id: string;
    publicMetadata: Record<string, unknown>;
    privateMetadata: Record<string, unknown>;
};

const WEBHOOK_SECRET = "whsec_test_secret";
const PRICE_ID = "price_test";

function makeCustomer(overrides: Partial<Stripe.Customer> = {}): Stripe.Customer {
    return {
        id: "cus_test", livemode: false, metadata: { userId: "user_clerk_1" }, ...overrides,
    } as Stripe.Customer;
}

function makeStripeWithMocks(opts: {
    subscriptions?: Record<string, Stripe.Subscription>;
    customers?: Record<string, Stripe.Customer | Stripe.DeletedCustomer>;
    account?: string;
    invoices?: ReturnType<typeof paidInvoiceFixture>[];
}): Stripe {
    const real = new Stripe("sk_test_dummy", { typescript: true });
    const subs = opts.subscriptions ?? {};
    const customers = opts.customers ?? { cus_test: makeCustomer() };
    Object.assign(real, {
        accounts: { retrieve: async () => ({ id: opts.account ?? 'acct_fixture' }) },
        invoices: {
            list: async ({ subscription, status }: Stripe.InvoiceListParams) => {
                expect(status).toBe("paid");
                if (opts.invoices) return { data: opts.invoices, has_more: false };
                const sub = subs[subscription ?? ""];
                const item = sub?.items.data.find(({ price }) => price.id === PRICE_ID);
                return { data: sub && item ? [paidInvoiceFixture(sub, item)] : [], has_more: false };
            },
        },
        subscriptions: {
            retrieve: async (id: string) => {
                if (!subs[id]) throw new Error(`subscription not found: ${id}`);
                return subs[id];
            },
            list: (params: { customer: string; price: string; status: string; limit: number }) => ({
                async *[Symbol.asyncIterator]() {
                    expect(params.status).toBe('all');
                    expect(params.price).toBe(PRICE_ID);
                    // Return all test subscriptions to also exercise the app's item validation.
                    for (const sub of Object.values(subs)) {
                        if (sub.customer === params.customer) yield sub;
                    }
                },
            }),
        },
        customers: {
            retrieve: async (id: string) => {
                if (!customers[id]) throw new Error(`customer not found: ${id}`);
                return customers[id];
            },
        },
    });
    return real;
}

type TestEvent = { id: string; type: string; data: { object: unknown }; livemode?: boolean; account?: string };

function signedRequest(stripe: Stripe, event: TestEvent): Request {
    const body = JSON.stringify({ livemode: false, ...event });
    const signature = stripe.webhooks.generateTestHeaderString({
        payload: body,
        secret: WEBHOOK_SECRET,
    });
    headersStore.clear();
    headersStore.set("stripe-signature", signature);
    return new Request("http://localhost/api/webhooks/stripe", {
        method: "POST",
        body,
        headers: { "stripe-signature": signature },
    });
}

function makeSubscription(overrides: Partial<Stripe.Subscription> = {}): Stripe.Subscription {
    return {
        id: "sub_test",
        created: 1_790_000_000,
        status: "active",
        customer: "cus_test",
        livemode: false,
        metadata: { userId: "user_clerk_1" },
        items: {
            data: [{ id: "si_1", current_period_end: 1_800_000_000, price: { id: PRICE_ID } }],
        },
        ...overrides,
    } as unknown as Stripe.Subscription;
}

function makeUsers(): Record<string, StoredUser> {
    return {
        user_clerk_1: {
            id: "user_clerk_1", publicMetadata: { stripe_customer_id: "cus_test" }, privateMetadata: {},
        },
    };
}

function makeFakeClerk(users: Record<string, StoredUser>) {
    return {
        users: {
            async getUser(id: string) {
                if (!users[id]) throw new Error(`no user ${id}`);
                return users[id];
            },
            async updateUserMetadata(
                id: string,
                patch: {
                    publicMetadata?: Record<string, unknown>;
                    privateMetadata?: Record<string, unknown>;
                },
            ) {
                const user = users[id];
                for (const key of ['publicMetadata', 'privateMetadata'] as const) {
                    for (const [field, value] of Object.entries(patch[key] ?? {})) {
                        if (value === null) delete user[key][field];
                        else user[key][field] = value;
                    }
                }
                return user;
            },
        },
    };
}

beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(1_780_000_000_000);
    setStripeForTests(null);
    setClerkForTests(null);
    headersStore.clear();
});

afterEach(() => vi.useRealTimers());

describe("POST /api/webhooks/stripe", () => {
    it("does not extend an active subscription on a signed failed live renewal", async () => {
        vi.stubEnv("STRIPE_SECRET_KEY", "sk_live_webhook_fixture");
        try {
            const users = makeUsers();
            setClerkForTests(makeFakeClerk(users) as never);
            const now = Date.now() / 1000;
            Object.assign(users.user_clerk_1.publicMetadata, {
                is_premium: true, subscription_current_period_end: now,
                stripe_customer_account_id: "acct_fixture", stripe_customer_livemode: true,
                stripe_entitlement: { account_id: "acct_fixture", customer_id: "cus_test", livemode: true, price_id: PRICE_ID },
            });
            const sub = makeSubscription({ livemode: true, latest_invoice: "in_failed_renewal" });
            const item = sub.items.data[0];
            const previous = paidInvoiceFixture(sub, item, now - 3600, now);
            const open = { ...paidInvoiceFixture(sub, item, now), id: sub.latest_invoice as string, status: "open", amount_paid: 0 };
            const stripe = makeStripeWithMocks({
                subscriptions: { sub_test: sub },
                customers: { cus_test: makeCustomer({ livemode: true }) },
                invoices: [open, previous],
            });
            setStripeForTests(stripe);

            const response = await POST(signedRequest(stripe, {
                id: "evt_failed_renewal", livemode: true, type: "invoice.payment_failed", data: { object: open },
            }));
            expect(response.status).toBe(200);
            expect(users.user_clerk_1.publicMetadata.is_premium).toBe(false);
            expect(users.user_clerk_1.publicMetadata.subscription_current_period_end).toBeUndefined();
        } finally {
            vi.unstubAllEnvs();
        }
    });

    it("rejects requests with an invalid signature", async () => {
        setStripeForTests(makeStripeWithMocks({}));
        const body = JSON.stringify({ id: "evt_x", type: "checkout.session.completed" });
        headersStore.set("stripe-signature", "t=1,v1=deadbeef");
        const req = new Request("http://localhost/api/webhooks/stripe", {
            method: "POST",
            body,
        });
        const res = await POST(req);
        expect(res.status).toBe(400);
    });

    it("rejects requests with no signature header", async () => {
        setStripeForTests(makeStripeWithMocks({}));
        const req = new Request("http://localhost/api/webhooks/stripe", {
            method: "POST",
            body: "{}",
        });
        const res = await POST(req);
        expect(res.status).toBe(400);
    });

    it("grants premium on checkout.session.completed", async () => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({
            subscriptions: { sub_test: makeSubscription() },
        });
        setStripeForTests(stripe);

        const event: TestEvent = {
            id: "evt_checkout_1",
            type: "checkout.session.completed",
            data: {
                object: {
                    id: "cs_1",
                    mode: "subscription",
                    subscription: "sub_test",
                    customer: "cus_test",
                    metadata: { userId: "user_clerk_1" },
                } as unknown as Stripe.Checkout.Session,
            },
        };
        const res = await POST(signedRequest(stripe, event));
        expect(res.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(true);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe("active");
        expect(users.user_clerk_1.publicMetadata.stripe_customer_id).toBe("cus_test");
        expect(users.user_clerk_1.publicMetadata).toMatchObject({
            stripe_customer_account_id: 'acct_fixture', stripe_customer_livemode: false,
            stripe_entitlement: {
                customer_id: 'cus_test', account_id: 'acct_fixture', livemode: false, price_id: PRICE_ID,
            },
        });
        expect(users.user_clerk_1.privateMetadata.stripe_last_event_id).toBe(
            "evt_checkout_1",
        );
    });

    it('ignores signed events with a different mode or connected account', async () => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const sub = makeSubscription();
        const stripe = makeStripeWithMocks({ subscriptions: { sub_test: sub } });
        setStripeForTests(stripe);
        const provider = vi.spyOn(stripe.accounts, 'retrieve');
        const event = {
            id: 'evt_wrong_scope', type: 'customer.subscription.created', data: { object: sub },
        };

        expect((await POST(signedRequest(stripe, { ...event, livemode: true }))).status).toBe(200);
        expect((await POST(signedRequest(stripe, { ...event, account: 'acct_other' }))).status).toBe(200);
        expect(provider).not.toHaveBeenCalled();
        expect(users.user_clerk_1.publicMetadata.is_premium).toBeUndefined();
    });

    it('requires the actual Stripe account to match before writing entitlement metadata', async () => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const sub = makeSubscription();
        const stripe = makeStripeWithMocks({ account: 'acct_other', subscriptions: { sub_test: sub } });
        setStripeForTests(stripe);
        const log = vi.spyOn(console, 'error').mockImplementation(() => {});
        try {
            expect((await POST(signedRequest(stripe, {
                id: 'evt_wrong_provider', type: 'customer.subscription.created', data: { object: sub },
            }))).status).toBe(500);
            expect(users.user_clerk_1.publicMetadata.is_premium).toBeUndefined();
        } finally {
            log.mockRestore();
        }
    });

    it("revokes premium on customer.subscription.deleted", async () => {
        const users = makeUsers();
        users.user_clerk_1.publicMetadata = {
            [PREMIUM_KEY]: true,
            subscription_status: "active",
            stripe_customer_id: "cus_test",
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({
            subscriptions: { sub_test: makeSubscription({ status: 'canceled' }) },
        });
        setStripeForTests(stripe);

        const event: TestEvent = {
            id: "evt_del_1",
            type: "customer.subscription.deleted",
            data: {
                object: makeSubscription({ status: "canceled" }),
            },
        };
        const res = await POST(signedRequest(stripe, event));
        expect(res.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(false);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe("canceled");
    });

    it("revokes premium when subscription becomes past_due", async () => {
        const users = makeUsers();
        users.user_clerk_1.publicMetadata = {
            ...users.user_clerk_1.publicMetadata,
            [PREMIUM_KEY]: true,
            subscription_status: "active",
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({
            subscriptions: { sub_test: makeSubscription({ status: 'past_due' }) },
        });
        setStripeForTests(stripe);

        const event: TestEvent = {
            id: "evt_upd_1",
            type: "customer.subscription.updated",
            data: {
                object: makeSubscription({ status: "past_due" }),
            },
        };
        const res = await POST(signedRequest(stripe, event));
        expect(res.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(false);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe("past_due");
    });

    it("revokes premium on invoice.payment_failed (re-fetch shows past_due)", async () => {
        const users = makeUsers();
        users.user_clerk_1.publicMetadata = {
            ...users.user_clerk_1.publicMetadata,
            [PREMIUM_KEY]: true,
            subscription_status: "active",
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({
            subscriptions: { sub_test: makeSubscription({ status: "past_due" }) },
        });
        setStripeForTests(stripe);

        const event: TestEvent = {
            id: "evt_inv_1",
            type: "invoice.payment_failed",
            data: {
                object: {
                    id: "in_1",
                    subscription: "sub_test",
                    customer: "cus_test",
                } as unknown as Stripe.Invoice,
            },
        };
        const res = await POST(signedRequest(stripe, event));
        expect(res.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(false);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe("past_due");
    });

    it("is idempotent on replay of the same event id", async () => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({
            subscriptions: { sub_test: makeSubscription() },
        });
        setStripeForTests(stripe);

        const event: TestEvent = {
            id: "evt_replay",
            type: "customer.subscription.updated",
            data: {
                object: makeSubscription(),
            },
        };

        const res1 = await POST(signedRequest(stripe, event));
        expect(res1.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(true);

        // Mutate stored state to detect a second write.
        users.user_clerk_1.publicMetadata = {
            ...users.user_clerk_1.publicMetadata,
            sentinel: "should-not-be-overwritten",
        };

        const res2 = await POST(signedRequest(stripe, event));
        expect(res2.status).toBe(200);
        // A replay must not overwrite the sentinel.
        expect(users.user_clerk_1.publicMetadata.sentinel).toBe(
            "should-not-be-overwritten",
        );
    });

    it('keeps cancellation applied when an older active event arrives later', async () => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({
            subscriptions: { sub_test: makeSubscription({ status: 'canceled' }) },
        });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_old_active', type: 'customer.subscription.updated',
            data: { object: makeSubscription({ status: 'active' }) },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata.is_premium).toBe(false);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe('canceled');
    });

    it('retains a replacement subscription when an old subscription cancellation arrives', async () => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({
            subscriptions: {
                sub_test: makeSubscription({ status: 'canceled' }),
                sub_new: makeSubscription({ id: 'sub_new', created: 1_790_000_001 }),
            },
        });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_old_canceled', type: 'customer.subscription.deleted',
            data: { object: makeSubscription({ status: 'canceled' }) },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata.is_premium).toBe(true);
    });

    it('does not let an unrelated active price replace a canceled paid plan', async () => {
        const users = makeUsers();
        users.user_clerk_1.publicMetadata.is_premium = true;
        setClerkForTests(makeFakeClerk(users) as never);
        const canceled = makeSubscription({ status: 'canceled' });
        const unrelated = makeSubscription({ id: 'sub_other', created: 1_790_000_001, metadata: {} });
        unrelated.items.data[0].price.id = 'price_other';
        const stripe = makeStripeWithMocks({ subscriptions: { sub_test: canceled, sub_other: unrelated } });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_other_price', type: 'customer.subscription.created', data: { object: unrelated },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata.is_premium).toBe(false);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe('canceled');
    });

    it.each(['active', 'canceled'] as const)(
        'ignores an old %s subscription event for a different Stripe customer', async (status) => {
            const users = makeUsers();
            const priorMetadata = { stripe_customer_id: 'cus_test', is_premium: status === 'canceled' };
            users.user_clerk_1.publicMetadata = { ...priorMetadata };
            setClerkForTests(makeFakeClerk(users) as never);
            const old = makeSubscription({ customer: 'cus_old', status });
            const stripe = makeStripeWithMocks({
                customers: { cus_old: makeCustomer({ id: 'cus_old' }) },
                subscriptions: { sub_test: old },
            });
            setStripeForTests(stripe);

            const response = await POST(signedRequest(stripe, {
                id: 'evt_old_customer', type: 'customer.subscription.updated', data: { object: old },
            }));

            expect(response.status).toBe(200);
            expect(users.user_clerk_1.publicMetadata).toEqual(priorMetadata);
            expect(users.user_clerk_1.privateMetadata).toEqual({});
        },
    );

    it('uses the canonical customer’s legacy owner tag and the exact price item’s period', async () => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const sub = makeSubscription({ metadata: {} });
        const ownItem = sub.items.data[0];
        sub.items.data.unshift({
            ...ownItem, id: 'si_unrelated', price: { ...ownItem.price, id: 'price_other' },
            current_period_end: ownItem.current_period_end + 10_000,
        });
        const stripe = makeStripeWithMocks({
            customers: { cus_test: makeCustomer({ metadata: { clerkUserId: 'user_clerk_1' } }) },
            subscriptions: { sub_test: sub },
        });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_legacy_owner', type: 'customer.subscription.updated', data: { object: sub },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata.is_premium).toBe(true);
        expect(users.user_clerk_1.publicMetadata.subscription_current_period_end).toBe(ownItem.current_period_end);
    });

    it('does not adopt a customer from a webhook when checkout never established a canonical binding', async () => {
        const users = makeUsers();
        users.user_clerk_1.publicMetadata = {};
        setClerkForTests(makeFakeClerk(users) as never);
        const sub = makeSubscription();
        const stripe = makeStripeWithMocks({ subscriptions: { sub_test: sub } });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_unbound', type: 'customer.subscription.created', data: { object: sub },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata).toEqual({});
    });

    it.each<[string, Partial<Stripe.Customer>]>([
        ['owner', { metadata: { userId: 'user_other' } }],
        ['mode', { livemode: true }],
    ])('ignores a subscription whose %s disagrees with the provider customer', async (_, overrides) => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const sub = makeSubscription();
        const stripe = makeStripeWithMocks({
            customers: { cus_test: makeCustomer(overrides) },
            subscriptions: { sub_test: sub },
        });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_wrong_owner', type: 'customer.subscription.created', data: { object: sub },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata).toEqual({ stripe_customer_id: 'cus_test' });
    });

    it('revokes the known canonical customer after Stripe deletes it', async () => {
        const users = makeUsers();
        users.user_clerk_1.publicMetadata.is_premium = true;
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({ customers: {
            cus_test: { id: 'cus_test', object: 'customer', deleted: true },
        } });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_deleted_customer', type: 'customer.subscription.deleted',
            data: { object: makeSubscription({ status: 'canceled' }) },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata.is_premium).toBe(false);
        expect(users.user_clerk_1.publicMetadata.stripe_customer_id).toBe('cus_test');
    });

    it.each(['sub_test', makeSubscription()])('handles current invoice parent subscription %s', async (subscription) => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({
            subscriptions: { sub_test: makeSubscription({ status: 'unpaid' }) },
        });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_current_invoice', type: 'invoice.payment_failed',
            data: { object: { parent: { type: 'subscription_details', subscription_details: { subscription } } } },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe('unpaid');
    });

    it('restores access when a successful invoice follows payment recovery', async () => {
        const users = makeUsers();
        Object.assign(users.user_clerk_1.publicMetadata, { is_premium: false, subscription_status: 'unpaid' });
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({ subscriptions: { sub_test: makeSubscription() } });
        setStripeForTests(stripe);

        const response = await POST(signedRequest(stripe, {
            id: 'evt_recovered_invoice', type: 'invoice.paid',
            data: { object: { parent: { type: 'subscription_details', subscription_details: { subscription: 'sub_test' } } } },
        }));

        expect(response.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata.is_premium).toBe(true);
    });

    it('reconciles a cancellation that races with an older access grant', async () => {
        const users = makeUsers();
        const subscriptions = { sub_test: makeSubscription() };
        const clerk = makeFakeClerk(users);
        const writeMetadata = clerk.users.updateUserMetadata;
        let allowFirstWrite: () => void;
        const firstWriteGate = new Promise<void>((resolve) => { allowFirstWrite = resolve; });
        let notifyFirstWrite: () => void;
        const firstWriteStarted = new Promise<void>((resolve) => { notifyFirstWrite = resolve; });
        let writes = 0;
        vi.spyOn(clerk.users, 'updateUserMetadata').mockImplementation(async (userId, patch) => {
            writes += 1;
            if (writes === 1) {
                notifyFirstWrite();
                await firstWriteGate;
            }
            return writeMetadata(userId, patch);
        });
        setClerkForTests(clerk as never);
        const stripe = makeStripeWithMocks({ subscriptions });
        setStripeForTests(stripe);

        const oldGrant = POST(signedRequest(stripe, {
            id: 'evt_before_cancellation', type: 'customer.subscription.updated',
            data: { object: makeSubscription() },
        }));
        await firstWriteStarted;
        subscriptions.sub_test = makeSubscription({ status: 'canceled' });
        const cancellation = await POST(signedRequest(stripe, {
            id: 'evt_cancellation', type: 'customer.subscription.deleted',
            data: { object: subscriptions.sub_test },
        }));
        allowFirstWrite!();

        expect(cancellation.status).toBe(200);
        expect((await oldGrant).status).toBe(200);
        expect(users.user_clerk_1.publicMetadata.is_premium).toBe(false);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe('canceled');
    });

    it('asks Stripe to retry when current subscription state cannot be loaded', async () => {
        const users = makeUsers();
        users.user_clerk_1.publicMetadata.is_premium = false;
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({});
        Object.assign(stripe.subscriptions, {
            list: () => ({
                async *[Symbol.asyncIterator]() {
                    throw new Error('Stripe unavailable');
                },
            }),
        });
        setStripeForTests(stripe);
        const log = vi.spyOn(console, 'error').mockImplementation(() => {});
        try {
            const response = await POST(signedRequest(stripe, {
                id: 'evt_retry_later', type: 'customer.subscription.updated',
                data: { object: makeSubscription() },
            }));
            expect(response.status).toBe(500);
            expect(users.user_clerk_1.publicMetadata.is_premium).toBe(false);
        } finally {
            log.mockRestore();
        }
    });

    it("ignores unrelated event types", async () => {
        const users = makeUsers();
        setClerkForTests(makeFakeClerk(users) as never);
        const stripe = makeStripeWithMocks({});
        setStripeForTests(stripe);

        const event: TestEvent = {
            id: "evt_other",
            type: "charge.succeeded",
            data: { object: {} as unknown as Stripe.Charge } as Stripe.Event.Data,
        };
        const res = await POST(signedRequest(stripe, event));
        expect(res.status).toBe(200);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBeUndefined();
    });
});
