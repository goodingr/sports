import { describe, it, expect, beforeEach } from "vitest";
import type Stripe from "stripe";
import {
    PREMIUM_KEY,
    applySnapshot,
    createBillingPortalSession,
    getOrCreateCustomer,
    getStoredCustomerId,
    isPremiumStatus,
    revokedSnapshot,
    setClerkForTests,
    setStripeForTests,
    snapshotFromSubscription,
} from "./billing";

type StoredUser = {
    id: string;
    publicMetadata: Record<string, unknown>;
    privateMetadata: Record<string, unknown>;
};

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

function makeSubscription(overrides: Partial<Stripe.Subscription> = {}): Stripe.Subscription {
    return {
        id: "sub_123",
        status: "active",
        customer: "cus_abc",
        metadata: { userId: "user_clerk_1" },
        items: {
            data: [
                {
                    id: "si_1",
                    current_period_end: 1_800_000_000,
                } as unknown as Stripe.SubscriptionItem,
            ],
        },
        ...overrides,
    } as unknown as Stripe.Subscription;
}

beforeEach(() => {
    setStripeForTests(null);
    setClerkForTests(null);
});

describe("isPremiumStatus", () => {
    it("treats active and trialing as premium", () => {
        expect(isPremiumStatus("active")).toBe(true);
        expect(isPremiumStatus("trialing")).toBe(true);
    });
    it("treats failure-mode statuses as not premium", () => {
        for (const s of ["past_due", "unpaid", "canceled", "incomplete", "paused"]) {
            expect(isPremiumStatus(s)).toBe(false);
        }
    });
    it("handles null/undefined", () => {
        expect(isPremiumStatus(null)).toBe(false);
        expect(isPremiumStatus(undefined)).toBe(false);
    });
});

describe("snapshotFromSubscription", () => {
    it("captures status, customer, period end", () => {
        const snap = snapshotFromSubscription(makeSubscription());
        expect(snap.is_premium).toBe(true);
        expect(snap.subscription_status).toBe("active");
        expect(snap.stripe_customer_id).toBe("cus_abc");
        expect(snap.subscription_current_period_end).toBe(1_800_000_000);
    });
    it("returns is_premium=false for past_due", () => {
        const snap = snapshotFromSubscription(makeSubscription({ status: "past_due" }));
        expect(snap.is_premium).toBe(false);
    });
});

describe("applySnapshot", () => {
    it("writes is_premium under public_metadata.is_premium", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: { id: "user_clerk_1", publicMetadata: {}, privateMetadata: {} },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const changed = await applySnapshot(
            "user_clerk_1",
            snapshotFromSubscription(makeSubscription()),
            "evt_1",
        );
        expect(changed).toBe(true);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(true);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe("active");
        expect(users.user_clerk_1.publicMetadata.stripe_customer_id).toBe("cus_abc");
        expect(users.user_clerk_1.publicMetadata.subscription_current_period_end).toBe(
            1_800_000_000,
        );
        expect(users.user_clerk_1.privateMetadata.stripe_last_event_id).toBe("evt_1");
    });

    it("is idempotent on replay of the same event id", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: {
                id: "user_clerk_1",
                publicMetadata: {},
                privateMetadata: { stripe_last_event_id: "evt_dup" },
            },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const changed = await applySnapshot(
            "user_clerk_1",
            snapshotFromSubscription(makeSubscription()),
            "evt_dup",
        );
        expect(changed).toBe(false);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBeUndefined();
    });

    it("revokes premium when applying a revoked snapshot", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: {
                id: "user_clerk_1",
                publicMetadata: {
                    [PREMIUM_KEY]: true,
                    subscription_status: "active",
                    stripe_customer_id: "cus_abc",
                    subscription_current_period_end: 1_800_000_000,
                },
                privateMetadata: {},
            },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        await applySnapshot(
            "user_clerk_1",
            revokedSnapshot("cus_abc", "canceled"),
            "evt_cancel",
        );
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(false);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe("canceled");
        expect(
            users.user_clerk_1.publicMetadata.subscription_current_period_end,
        ).toBeUndefined();
    });
});

describe("getOrCreateCustomer", () => {
    it("reuses an existing stripe_customer_id from Clerk metadata", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: {
                id: "user_clerk_1",
                publicMetadata: { stripe_customer_id: "cus_existing" },
                privateMetadata: {},
            },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        let createCalled = false;
        setStripeForTests({
            customers: {
                create: async () => {
                    createCalled = true;
                    return { id: "cus_new" };
                },
            },
        } as unknown as Stripe);
        const id = await getOrCreateCustomer("user_clerk_1", "user@example.com");
        expect(id).toBe("cus_existing");
        expect(createCalled).toBe(false);
    });

    it("creates a new customer and stores the id", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: { id: "user_clerk_1", publicMetadata: {}, privateMetadata: {} },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        setStripeForTests({
            customers: {
                create: async (params: { email: string; metadata: Record<string, string> }) => {
                    expect(params.email).toBe("user@example.com");
                    expect(params.metadata.userId).toBe("user_clerk_1");
                    return { id: "cus_new" };
                },
            },
        } as unknown as Stripe);
        const id = await getOrCreateCustomer("user_clerk_1", "user@example.com");
        expect(id).toBe("cus_new");
        expect(users.user_clerk_1.publicMetadata.stripe_customer_id).toBe("cus_new");
    });
});

describe("getStoredCustomerId", () => {
    it("returns the stored stripe_customer_id when present", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: {
                id: "user_clerk_1",
                publicMetadata: { stripe_customer_id: "cus_existing" },
                privateMetadata: {},
            },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const id = await getStoredCustomerId("user_clerk_1");
        expect(id).toBe("cus_existing");
    });

    it("returns null when the user has no stored customer", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: { id: "user_clerk_1", publicMetadata: {}, privateMetadata: {} },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const id = await getStoredCustomerId("user_clerk_1");
        expect(id).toBeNull();
    });
});

describe("createBillingPortalSession", () => {
    it("forwards customer + return_url to Stripe and returns the redirect URL", async () => {
        let received: { customer?: string; return_url?: string } = {};
        setStripeForTests({
            billingPortal: {
                sessions: {
                    create: async (params: { customer: string; return_url: string }) => {
                        received = params;
                        return { url: "https://billing.stripe.com/session/test" };
                    },
                },
            },
        } as unknown as Stripe);
        const url = await createBillingPortalSession(
            "cus_abc",
            "https://example.test/account",
        );
        expect(url).toBe("https://billing.stripe.com/session/test");
        expect(received.customer).toBe("cus_abc");
        expect(received.return_url).toBe("https://example.test/account");
    });
});
