import { auth } from "@clerk/nextjs/server";
import type Stripe from "stripe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
    configuredStripeScope, hasScopedPremium, setClerkForTests, setStripeForTests,
} from "@/lib/billing";
import { paidInvoiceFixture } from "@/lib/stripe-paid-invoice.fixture";
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
const listInvoices = vi.fn();
const listLineItems = vi.fn();
const updateUserMetadata = vi.fn();
const createCheckout = vi.fn();
const createPayment = vi.fn();
const createPortal = vi.fn();

function providerSubscription(status = "active", end = now + 30 * 24 * 3600, price = priceId) {
    return {
        id: "sub_renewal", customer: customerId, livemode: true, created: now - 1000,
        status, metadata: { userId }, cancel_at_period_end: false,
        items: { data: [{ id: "si_renewal", price: { id: price }, current_period_end: end }] },
    };
}

function subscriptions(...rows: ReturnType<typeof providerSubscription>[]) {
    listSubscriptions.mockImplementation(async function* () { yield* rows; });
}

type InvoiceFixture = ReturnType<typeof paidInvoiceFixture>;

function invoices(...data: InvoiceFixture[]) {
    listInvoices.mockResolvedValue({ data, has_more: false });
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
    const sub = providerSubscription();
    subscriptions(sub);
    invoices(paidInvoiceFixture(sub, sub.items.data[0]));
    setStripeForTests({
        accounts: { retrieve: retrieveAccount },
        customers: { retrieve: retrieveCustomer },
        subscriptions: { list: listSubscriptions },
        invoices: { list: listInvoices, listLineItems },
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
        expect(listInvoices).toHaveBeenCalledTimes(2);
        expect(listInvoices).toHaveBeenCalledWith({ customer: customerId, subscription: "sub_renewal", status: "paid", limit: 12 });
        expect(updateUserMetadata).toHaveBeenCalledTimes(1);
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(true);
        expect(createCheckout).not.toHaveBeenCalled();
        expect(createPayment).not.toHaveBeenCalled();
        expect(createPortal).not.toHaveBeenCalled();
    });

    it("does not advance expired access for an active subscription with an unfinalized invoice", async () => {
        const sub = { ...providerSubscription(), latest_invoice: "in_draft_renewal" };
        const item = sub.items.data[0];
        const draft = { ...paidInvoiceFixture(sub, item, now), status: "draft", amount_paid: 0 };
        invoices(draft, paidInvoiceFixture(sub, item, now - 3600, now));
        subscriptions(sub);

        expect(await refreshAccountSubscription()).toBe("inactive");
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);
        expect(metadata.subscription_current_period_end).toBeUndefined();
    });

    it("keeps the existing paid service period during an unpaid renewal and extends after payment", async () => {
        const sub = providerSubscription();
        const item = sub.items.data[0];
        const oldEnd = now + 60;
        const old = paidInvoiceFixture(sub, item, now - 3600, oldEnd);
        const renewal = paidInvoiceFixture(sub, item, oldEnd);
        invoices({ ...renewal, status: "open", amount_paid: 0 }, old);
        metadata.subscription_current_period_end = oldEnd;

        expect(await refreshAccountSubscription()).toBe("active");
        expect(metadata.subscription_current_period_end).toBe(oldEnd);
        await vi.advanceTimersByTimeAsync(60_000);
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);

        invoices(renewal, old);
        expect(await refreshAccountSubscription()).toBe("active");
        expect(metadata.subscription_current_period_end).toBe(item.current_period_end);
    });

    it("authorizes a finite explicit trial without looking up paid invoices", async () => {
        subscriptions({ ...providerSubscription("trialing"), trial_end: now + 120 } as ReturnType<typeof providerSubscription>);
        expect(await refreshAccountSubscription()).toBe("active");
        expect(metadata.subscription_current_period_end).toBe(now + 120);
        expect(listInvoices).not.toHaveBeenCalled();
    });

    it.each<[string, (invoice: InvoiceFixture) => void]>([
        ["unpaid invoice", (invoice) => { invoice.status = "open"; }],
        ["zero paid amount", (invoice) => { invoice.amount_paid = 0; }],
        ["wrong currency", (invoice) => { invoice.currency = "cad"; }],
        ["wrong customer", (invoice) => { invoice.customer = "cus_other"; }],
        ["wrong mode", (invoice) => { invoice.livemode = false; }],
        ["wrong subscription", (invoice) => { invoice.parent.subscription_details.subscription = "sub_other"; }],
        ["wrong line invoice", (invoice) => { invoice.lines.data[0].invoice = "in_other"; }],
        ["wrong line price", (invoice) => { invoice.lines.data[0].pricing.price_details.price = "price_other"; }],
        ["wrong subscription item", (invoice) => { invoice.lines.data[0].parent.subscription_item_details.subscription_item = "si_other"; }],
        ["proration", (invoice) => { invoice.lines.data[0].parent.subscription_item_details.proration = true; }],
        ["nonpositive line", (invoice) => { invoice.lines.data[0].amount = 0; }],
        ["fully discounted target line on a positive invoice", (invoice) => {
            const line = invoice.lines.data[0];
            line.discount_amounts = [{ amount: line.amount, discount: "di_waived" }];
        }],
        ["malformed coverage", (invoice) => { invoice.lines.data[0].period.end = Infinity; }],
        ["future service", (invoice) => { invoice.lines.data[0].period.start = now + 1; }],
        ["future payment", (invoice) => { invoice.status_transitions.paid_at = now + 1; }],
    ])("does not extend access using %s", async (_, mutate) => {
        const sub = providerSubscription();
        const invoice = paidInvoiceFixture(sub, sub.items.data[0]);
        mutate(invoice);
        invoices(invoice);

        expect(await refreshAccountSubscription()).toBe("inactive");
        expect(metadata.stripe_entitlement).toBeUndefined();
    });

    it("uses complete paginated lines and caps the end at the item or cancellation", async () => {
        const sub = { ...providerSubscription(), cancel_at: now + 3600 };
        const item = sub.items.data[0];
        const invoice = paidInvoiceFixture(sub, item, now - 60, item.current_period_end + 60);
        const lines = invoice.lines.data.map((line) => ({
            ...line, invoice: undefined, subscription: undefined, parent: {
                ...line.parent,
                subscription_item_details: { ...line.parent.subscription_item_details, subscription: null },
            },
        }));
        invoice.lines = { has_more: true, data: [] };
        invoices(invoice);
        subscriptions(sub);
        listLineItems.mockReturnValue({ autoPagingToArray: async () => lines });

        expect(await refreshAccountSubscription()).toBe("active");
        expect(listLineItems).toHaveBeenCalledWith(invoice.id, { limit: 100 });
        expect(metadata.subscription_current_period_end).toBe(sub.cancel_at);
    });

    it("keeps the previous expiring grant when invoice verification is unavailable", async () => {
        metadata.subscription_current_period_end = now + 60;
        listInvoices.mockRejectedValueOnce(new Error("Temporary Stripe error"));

        expect(await refreshAccountSubscription()).toBe("unavailable");
        expect(updateUserMetadata).not.toHaveBeenCalled();
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(true);
        await vi.advanceTimersByTimeAsync(60_000);
        expect(hasScopedPremium(metadata, configuredStripeScope())).toBe(false);
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
