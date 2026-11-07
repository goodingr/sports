import { describe, it, expect, beforeEach, vi } from "vitest";
import Stripe from "stripe";

process.env.STRIPE_SECRET_KEY = "sk_test_dummy";
process.env.STRIPE_WEBHOOK_SECRET = "whsec_test_secret";
process.env.STRIPE_PRICE_ID = "price_test";
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

function makeStripeWithMocks(opts: {
    subscriptions?: Record<string, Stripe.Subscription>;
    customers?: Record<string, Stripe.Customer>;
}): Stripe {
    const real = new Stripe("sk_test_dummy", { typescript: true });
    const subs = opts.subscriptions ?? {};
    const customers = opts.customers ?? {};
    Object.assign(real, {
        subscriptions: {
            retrieve: async (id: string) => {
                if (!subs[id]) throw new Error(`subscription not found: ${id}`);
                return subs[id];
            },
            list: (params: { customer: string; status: string; limit: number }) => ({
                async *[Symbol.asyncIterator]() {
                    expect(params.status).toBe('all');
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

type TestEvent = { id: string; type: string; data: { object: unknown } };

function signedRequest(stripe: Stripe, event: TestEvent): Request {
    const body = JSON.stringify(event);
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
        metadata: { userId: "user_clerk_1" },
        items: {
            data: [{ id: "si_1", current_period_end: 1_800_000_000 } as never],
        },
        ...overrides,
    } as unknown as Stripe.Subscription;
}

function makeUsers(): Record<string, StoredUser> {
    return {
        user_clerk_1: { id: "user_clerk_1", publicMetadata: {}, privateMetadata: {} },
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
    setStripeForTests(null);
    setClerkForTests(null);
    headersStore.clear();
});

describe("POST /api/webhooks/stripe", () => {
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
        expect(users.user_clerk_1.privateMetadata.stripe_last_event_id).toBe(
            "evt_checkout_1",
        );
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
        users.user_clerk_1.publicMetadata = { is_premium: false, subscription_status: 'unpaid' };
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
        users.user_clerk_1.publicMetadata = { is_premium: false };
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
