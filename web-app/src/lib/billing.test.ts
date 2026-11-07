import { afterEach, describe, it, expect, beforeEach, vi } from "vitest";
import type Stripe from "stripe";
import {
    PREMIUM_KEY,
    applySnapshot,
    createBillingPortalSession,
    getStoredCustomerId,
    hasScopedPremium,
    getVerifiedCheckoutPrice,
    isPremiumStatus,
    revokedSnapshot,
    setClerkForTests,
    setStripeForTests,
    type StripeScope,
    snapshotFromSubscription,
} from "./billing";

const scope: StripeScope = { account: 'acct_test', livemode: false, priceId: 'price_test' };
const NOW = 1_780_000_000;

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
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW * 1000);
    setStripeForTests(null);
    setClerkForTests(null);
});

afterEach(() => vi.useRealTimers());

describe("getVerifiedCheckoutPrice", () => {
    const product = { id: "prod_test", object: "product", active: true } as Stripe.Product;
    const recurring: Stripe.Price.Recurring = {
        interval: "month", interval_count: 1, usage_type: "licensed", meter: null, trial_period_days: null,
    };
    const price = {
        id: "price_test", active: true, product, currency: "usd", unit_amount: 2900, livemode: false,
        unit_amount_decimal: "2900.00", type: "recurring", billing_scheme: "per_unit",
        custom_unit_amount: null, transform_quantity: null, recurring,
    };
    const retrieve = vi.fn();
    const stripe = { prices: { retrieve } } as unknown as Stripe;

    beforeEach(() => {
        retrieve.mockReset();
        vi.stubEnv("STRIPE_PRICE_ID", price.id);
    });

    afterEach(() => vi.unstubAllEnvs());

    it.each([false, true])("accepts the standard price in either Stripe mode (%s)", async (livemode) => {
        vi.stubEnv("NODE_ENV", "production");
        retrieve.mockResolvedValue({ ...price, livemode });

        await expect(getVerifiedCheckoutPrice(stripe, { ...scope, livemode })).resolves.toEqual({ id: price.id, livemode });
        expect(retrieve).toHaveBeenCalledExactlyOnceWith(price.id, { expand: ["product"] });
    });

    it.each<[string, Partial<Stripe.Price>]>([
        ["a different ID", { id: "price_other" }],
        ["an archived price", { active: false }],
        ["an archived product", { product: { ...product, active: false } }],
        ["an unverified product", { product: product.id }],
        ["a different currency", { currency: "cad" }],
        ["a different amount", { unit_amount: 3000, unit_amount_decimal: "3000" }],
        ["a fractional amount", { unit_amount: null, unit_amount_decimal: "2900.001" }],
        ["an inconsistent decimal", { unit_amount_decimal: "2900.01" }],
        ["a one-time purchase", { type: "one_time", recurring: null }],
        ["an annual period", { recurring: { ...recurring, interval: "year" } }],
        ["a multi-month period", { recurring: { ...recurring, interval_count: 2 } }],
        ["tiered billing", { billing_scheme: "tiered" }],
        ["metered usage", { recurring: { ...recurring, usage_type: "metered", meter: "mtr_test" } }],
        ["a trial", { recurring: { ...recurring, trial_period_days: 7 } }],
        ["a customer-defined amount", { custom_unit_amount: { minimum: null, maximum: null, preset: 2900 } }],
        ["a quantity transformation", { transform_quantity: { divide_by: 2, round: "down" } }],
    ])("rejects %s", async (_, overrides) => {
        retrieve.mockResolvedValue({ ...price, ...overrides });

        await expect(getVerifiedCheckoutPrice(stripe, scope)).resolves.toBeNull();
    });
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
    it('retains access through a scheduled cancellation and records the end date', () => {
        const snap = snapshotFromSubscription(makeSubscription({
            cancel_at_period_end: true,
            cancel_at: 1_790_000_000,
        }));
        expect(snap.is_premium).toBe(true);
        expect(snap.subscription_cancel_at_period_end).toBe(true);
        expect(snap.subscription_current_period_end).toBe(1_790_000_000);
    });

    it("limits an authorized trial to its explicit end", () => {
        const trial = makeSubscription({ status: "trialing", trial_end: NOW + 100 });
        expect(snapshotFromSubscription(trial)).toMatchObject({
            is_premium: true, subscription_current_period_end: NOW + 100,
        });
        vi.setSystemTime((NOW + 100) * 1000);
        expect(snapshotFromSubscription(trial).is_premium).toBe(false);
        expect(snapshotFromSubscription(makeSubscription({ status: "trialing", trial_end: null })).is_premium).toBe(false);
    });

    it("needs the exact product item with a valid end", () => {
        const sub = makeSubscription();
        sub.items.data[0].price = { id: "price_other" } as Stripe.Price;
        Object.assign(sub, { current_period_end: NOW + 1000 });
        expect(snapshotFromSubscription(sub, scope.priceId)).toMatchObject({
            is_premium: false, subscription_current_period_end: null,
        });
        sub.items.data[0].price.id = scope.priceId;
        sub.items.data[0].current_period_end = Infinity;
        expect(snapshotFromSubscription(sub, scope.priceId)).toMatchObject({
            is_premium: false, subscription_current_period_end: null,
        });
    });
});

describe("hasScopedPremium", () => {
    it.each([undefined, null, NOW, NOW - 1, `${NOW + 1}`, NOW + 0.5, Infinity, Number.MAX_SAFE_INTEGER + 1])(
        "rejects an expired or malformed period end (%s) despite verified scope",
        (end) => {
            const metadata = {
                is_premium: true, stripe_customer_id: "cus_abc",
                stripe_customer_account_id: scope.account, stripe_customer_livemode: scope.livemode,
                stripe_entitlement: { customer_id: "cus_abc", account_id: scope.account, livemode: scope.livemode, price_id: scope.priceId },
                subscription_current_period_end: end,
            };
            expect(hasScopedPremium(metadata, scope)).toBe(false);
            expect(hasScopedPremium({ ...metadata, subscription_current_period_end: NOW + 1 }, scope)).toBe(true);
        },
    );
});

describe("applySnapshot", () => {
    it("writes is_premium under public_metadata.is_premium", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: {
                id: "user_clerk_1", publicMetadata: { stripe_customer_id: "cus_abc" }, privateMetadata: {},
            },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const changed = await applySnapshot(
            "user_clerk_1",
            snapshotFromSubscription(makeSubscription()),
            "evt_1", scope,
        );
        expect(changed).toBe(true);
        expect(hasScopedPremium(users.user_clerk_1.publicMetadata, scope)).toBe(true);
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
                publicMetadata: {
                    ...snapshotFromSubscription(makeSubscription()),
                    stripe_customer_account_id: scope.account, stripe_customer_livemode: scope.livemode,
                    stripe_entitlement: { customer_id: 'cus_abc', account_id: scope.account, livemode: scope.livemode, price_id: scope.priceId },
                },
                privateMetadata: { stripe_last_event_id: "evt_dup" },
            },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const changed = await applySnapshot(
            "user_clerk_1",
            snapshotFromSubscription(makeSubscription()),
            "evt_dup", scope,
        );
        expect(changed).toBe(false);
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(true);
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
                    isPremium: true,
                    preference: 'kept',
                },
                privateMetadata: {},
            },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        await applySnapshot(
            "user_clerk_1",
            revokedSnapshot("cus_abc", "canceled"),
            "evt_cancel", scope,
        );
        expect(users.user_clerk_1.publicMetadata[PREMIUM_KEY]).toBe(false);
        expect(users.user_clerk_1.publicMetadata.subscription_status).toBe("canceled");
        expect(
            users.user_clerk_1.publicMetadata.subscription_current_period_end,
        ).toBeUndefined();
        expect(users.user_clerk_1.publicMetadata.isPremium).toBeUndefined();
        expect(users.user_clerk_1.publicMetadata.preference).toBe('kept');
    });

    it('repairs changed Stripe state even when an event was already delivered', async () => {
        const users = {
            user_clerk_1: {
                id: 'user_clerk_1',
                publicMetadata: {
                    ...snapshotFromSubscription(makeSubscription()),
                    stripe_customer_account_id: scope.account, stripe_customer_livemode: scope.livemode,
                    stripe_entitlement: { customer_id: 'cus_abc', account_id: scope.account, livemode: scope.livemode, price_id: scope.priceId },
                },
                privateMetadata: { stripe_last_event_id: 'evt_retry' },
            },
        };
        setClerkForTests(makeFakeClerk(users) as never);

        expect(await applySnapshot('user_clerk_1', revokedSnapshot('cus_abc', 'canceled'), 'evt_retry', scope)).toBe(true);
        expect(users.user_clerk_1.publicMetadata.is_premium).toBe(false);
    });

    it('skips a write when the stored canonical customer differs from the verified snapshot', async () => {
        const prior = { stripe_customer_id: 'cus_replacement', is_premium: true };
        const users = {
            user_clerk_1: { id: 'user_clerk_1', publicMetadata: { ...prior }, privateMetadata: {} },
        };
        setClerkForTests(makeFakeClerk(users) as never);

        expect(await applySnapshot('user_clerk_1', revokedSnapshot('cus_abc', 'canceled'), 'evt_old', scope)).toBe(false);
        expect(users.user_clerk_1.publicMetadata).toEqual(prior);
        expect(users.user_clerk_1.privateMetadata).toEqual({});
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
        const id = await getStoredCustomerId("user_clerk_1", scope);
        expect(id).toBe("cus_existing");
    });

    it("returns null when the user has no stored customer", async () => {
        const users: Record<string, StoredUser> = {
            user_clerk_1: { id: "user_clerk_1", publicMetadata: {}, privateMetadata: {} },
        };
        setClerkForTests(makeFakeClerk(users) as never);
        const id = await getStoredCustomerId("user_clerk_1", scope);
        expect(id).toBeNull();
    });
});

describe("createBillingPortalSession", () => {
    it("forwards customer + return_url to Stripe and returns the redirect URL", async () => {
        let received: { customer?: string; return_url?: string } = {};
        const stripe = {
            billingPortal: {
                sessions: {
                    create: async (params: { customer: string; return_url: string }) => {
                        received = params;
                        return { url: "https://billing.stripe.com/session/test" };
                    },
                },
            },
        } as unknown as Stripe;
        const url = await createBillingPortalSession(
            stripe,
            "cus_abc",
            "https://example.test/account",
        );
        expect(url).toBe("https://billing.stripe.com/session/test");
        expect(received.customer).toBe("cus_abc");
        expect(received.return_url).toBe("https://example.test/account");
    });
});
