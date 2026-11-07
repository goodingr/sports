import { auth } from "@clerk/nextjs/server";
import type Stripe from "stripe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
    configuredStripeScope, hasScopedPremium, setClerkForTests, setStripeForTests,
} from "@/lib/billing";
import { refreshAccountSubscription } from "./refresh-subscription";

vi.mock("server-only", () => ({}));
vi.mock("@clerk/nextjs/server", () => ({ auth: vi.fn() }));

const now = 1_780_000_000;
const userId = "user_renewal";
const customerId = "cus_renewal";
const accountId = "acct_billing";
const priceId = "price_monthly";
let metadata: Record<string, unknown>;
let privateMetadata: Record<string, unknown>;
const retrieveAccount = vi.fn();
const retrieveCustomer = vi.fn();
const listSubscriptions = vi.fn();
const updateUserMetadata = vi.fn();
const createCheckout = vi.fn();
const createPayment = vi.fn();
const createPortal = vi.fn();

function providerSubscription(status = "active", end = now + 30 * 24 * 3600, price = priceId) {
    return {
        id: "sub_renewal", customer: customerId, livemode: true, created: now - 1000,
        status, metadata: { userId }, cancel_at_period_end: false,
        items: { data: [{ price: { id: price }, current_period_end: end }] },
    };
}

function subscriptions(...rows: ReturnType<typeof providerSubscription>[]) {
    listSubscriptions.mockImplementation(async function* () { yield* rows; });
}

beforeEach(() => {
    vi.resetAllMocks();
    vi.useFakeTimers();
    vi.setSystemTime(now * 1000);
    vi.stubEnv("STRIPE_SECRET_KEY", "sk_live_fixture");
    vi.stubEnv("STRIPE_ACCOUNT_ID", accountId);
    vi.stubEnv("STRIPE_PRICE_ID", priceId);
    vi.stubEnv("COMMERCIAL_PILOT_ONLY", "false");
    vi.stubEnv("PAID_ENROLLMENT_ENABLED", "false");
    vi.mocked(auth).mockResolvedValue({ userId } as Awaited<ReturnType<typeof auth>>);
    metadata = {
        is_premium: true, subscription_status: "active", subscription_current_period_end: now,
        stripe_customer_id: customerId, stripe_customer_account_id: accountId, stripe_customer_livemode: true,
        stripe_entitlement: { customer_id: customerId, account_id: accountId, price_id: priceId, livemode: true },
    };
    privateMetadata = {};
    updateUserMetadata.mockImplementation(async (_, patch) => {
        for (const [key, value] of Object.entries(patch.publicMetadata ?? {})) {
            if (value === null) delete metadata[key];
            else metadata[key] = value;
        }
        Object.assign(privateMetadata, patch.privateMetadata);
    });
    setClerkForTests({
        users: {
            getUser: async () => ({ id: userId, publicMetadata: { ...metadata }, privateMetadata: { ...privateMetadata } }),
            updateUserMetadata,
        },
    } as never);
    retrieveAccount.mockResolvedValue({ id: accountId });
    retrieveCustomer.mockResolvedValue({ id: customerId, livemode: true, metadata: { userId } });
    subscriptions(providerSubscription());
    setStripeForTests({
        accounts: { retrieve: retrieveAccount },
        customers: { retrieve: retrieveCustomer },
        subscriptions: { list: listSubscriptions },
        checkout: { sessions: { create: createCheckout } },
        paymentIntents: { create: createPayment },
        billingPortal: { sessions: { create: createPortal } },
    } as unknown as Stripe);
});

afterEach(async () => {
    await vi.runOnlyPendingTimersAsync();
    vi.useRealTimers();
    vi.unstubAllEnvs();
    setClerkForTests(null);
    setStripeForTests(null);
});

describe("signed-in account subscription refresh", () => {
    it("restores a verified renewal while enrollment is closed and coalesces repeated checks", async () => {
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);
        expect(await Promise.all([refreshAccountSubscription(), refreshAccountSubscription()])).toEqual(["active", "active"]);
        expect(await refreshAccountSubscription()).toBe("cooldown");
        expect(retrieveAccount).toHaveBeenCalledTimes(1);
        expect(listSubscriptions).toHaveBeenCalledTimes(2);
        expect(listSubscriptions).toHaveBeenCalledWith({ customer: customerId, price: priceId, status: "all", limit: 100 });
        expect(updateUserMetadata).toHaveBeenCalledTimes(1);
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(true);
        expect(createCheckout).not.toHaveBeenCalled();
        expect(createPayment).not.toHaveBeenCalled();
        expect(createPortal).not.toHaveBeenCalled();
    });

    it.each(["canceled", "active"])("clears stored access for an ended %s subscription", async (status) => {
        subscriptions(providerSubscription(status, now));
        expect(await refreshAccountSubscription()).toBe("inactive");
        expect(metadata.is_premium).toBe(false);
        expect(metadata.stripe_entitlement).toBeUndefined();
    });

    it("does not restore access using an unrelated price", async () => {
        subscriptions(providerSubscription("active", now + 3600, "price_other"));
        expect(await refreshAccountSubscription()).toBe("inactive");
        expect(metadata.stripe_entitlement).toBeUndefined();
    });

    it("rejects a wrong account or customer owner and bounds retry after failure", async () => {
        retrieveAccount.mockResolvedValueOnce({ id: "acct_other" });
        expect(await refreshAccountSubscription()).toBe("unavailable");
        expect(retrieveCustomer).not.toHaveBeenCalled();
        expect(await refreshAccountSubscription()).toBe("cooldown");
        await vi.advanceTimersByTimeAsync(30_000);
        retrieveCustomer.mockResolvedValueOnce({ id: customerId, livemode: true, metadata: { userId: "user_other" } });
        expect(await refreshAccountSubscription()).toBe("unavailable");
        expect(listSubscriptions).not.toHaveBeenCalled();
        expect(updateUserMetadata).not.toHaveBeenCalled();
    });

    it("does not reach Stripe for a signed-out or pilot-only account", async () => {
        vi.mocked(auth).mockResolvedValueOnce({ userId: null } as Awaited<ReturnType<typeof auth>>);
        expect(await refreshAccountSubscription()).toBe("unavailable");
        vi.stubEnv("COMMERCIAL_PILOT_ONLY", "true");
        expect(await refreshAccountSubscription()).toBe("unavailable");
        expect(auth).toHaveBeenCalledTimes(1);
        expect(retrieveAccount).not.toHaveBeenCalled();
    });
});
