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
                if (patch.publicMetadata) user.publicMetadata = patch.publicMetadata;
                if (patch.privateMetadata) user.privateMetadata = patch.privateMetadata;
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
        const stripe = makeStripeWithMocks({});
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
        const stripe = makeStripeWithMocks({});
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
