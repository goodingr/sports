import "server-only";

import type Stripe from "stripe";
import {
    applySnapshot,
    CHECKOUT_CURRENCY,
    configuredStripeScope,
    getStoredCustomerId,
    getVerifiedStripeAccount,
    ownsStripeCustomer,
    revokedSnapshot,
    snapshotFromSubscription,
    type PremiumSnapshot,
    type StripeScope,
} from "./billing";

export function resourceId(resource: string | { id: string } | null): string | null {
    return typeof resource === "string" ? resource : resource?.id ?? null;
}

export function isCheckoutSessionId(value: unknown): value is string {
    return typeof value === "string" && /^cs_(?:test|live)_[A-Za-z0-9_]{1,200}$/.test(value);
}

function hasPrice(sub: Stripe.Subscription, priceId: string): boolean {
    return sub.items.data.some((item) => item.price.id === priceId);
}

export async function reconcilePaidCheckoutReturn(
    stripe: Stripe, sessionId: string, userId: string,
): Promise<boolean> {
    const scope = configuredStripeScope();
    if (!scope || !sessionId.startsWith(scope.livemode ? "cs_live_" : "cs_test_")) return false;
    const { priceId, livemode } = scope;
    const customerId = await getStoredCustomerId(userId, scope);
    if (!customerId) return false;

    const session = await stripe.checkout.sessions.retrieve(sessionId, { expand: ["line_items"] });
    const owner = session.metadata?.userId ?? session.metadata?.clerkUserId;
    const items = session.line_items;
    if (session.id !== sessionId || resourceId(session.customer) !== customerId || owner !== userId
        || session.livemode !== livemode || session.mode !== "subscription"
        || session.status !== "complete" || session.payment_status !== "paid"
        || session.currency !== CHECKOUT_CURRENCY || !(session.amount_total && session.amount_total > 0)
        || !items || items.has_more || items.data.length !== 1
        || items.data[0].price?.id !== priceId || items.data[0].quantity !== 1) return false;

    const subscriptionId = resourceId(session.subscription);
    if (!subscriptionId) return false;
    const sub = await stripe.subscriptions.retrieve(subscriptionId);
    if (sub.id !== subscriptionId || resourceId(sub.customer) !== customerId || sub.livemode !== session.livemode
        || sub.status !== "active" || !hasPrice(sub, priceId)) return false;

    return reconcileStripeSubscription(stripe, sub, `checkout_return:${session.id}`, session.metadata, userId);
}

export async function reconcileStripeSubscription(
    stripe: Stripe,
    sub: Stripe.Subscription,
    eventId: string,
    checkoutMetadata: Stripe.Metadata | null,
    expectedUserId?: string,
): Promise<boolean> {
    const scope = configuredStripeScope();
    if (!scope) throw new Error("Stripe scope is not configured");
    if (sub.livemode !== scope.livemode) return false;
    const customerId = resourceId(sub.customer);
    if (!customerId) return false;
    await getVerifiedStripeAccount(stripe, scope);
    const customer = await stripe.customers.retrieve(customerId);
    if (customer.id !== customerId) return false;
    const deleted = "deleted" in customer && customer.deleted;
    if (!deleted && customer.livemode !== sub.livemode) return false;
    const metadata = deleted ? undefined : customer.metadata;
    const owners = new Set([
        checkoutMetadata?.userId, checkoutMetadata?.clerkUserId,
        sub.metadata?.userId, sub.metadata?.clerkUserId,
        metadata?.userId, metadata?.clerkUserId,
    ].filter((id): id is string => Boolean(id)));
    if (owners.size !== 1) return false;
    const [userId] = owners;
    if ((expectedUserId && expectedUserId !== userId) || await getStoredCustomerId(userId, scope) !== customerId) {
        return false;
    }
    if (deleted) {
        await applySnapshot(userId, revokedSnapshot(customerId, "canceled"), eventId, scope);
        return false;
    }

    const snapshot = await synchronizeCustomer(stripe, scope, customerId, userId, eventId, expectedUserId ? sub.id : undefined);
    return snapshot !== null;
}

/** Recover an existing current-price entitlement when verified checkout detects an existing subscription. */
export async function reconcileExistingCheckoutCustomer(
    stripe: Stripe, scope: StripeScope, customerId: string, userId: string,
): Promise<void> {
    // Checkout has already verified the provider account and price for this scope.
    const customer = await stripe.customers.retrieve(customerId);
    if (customer.id !== customerId || !ownsStripeCustomer(customer, userId, scope.livemode)) return;
    await synchronizeCustomer(stripe, scope, customerId, userId, `checkout_existing:${customerId}:${scope.priceId}`, undefined, true);
}

export async function reconcileAccountSubscription(
    stripe: Stripe, scope: StripeScope, userId: string,
): Promise<boolean> {
    const customerId = await getStoredCustomerId(userId, scope);
    if (!customerId) return false;
    await getVerifiedStripeAccount(stripe, scope);
    const customer = await stripe.customers.retrieve(customerId);
    if (customer.id !== customerId || !ownsStripeCustomer(customer, userId, scope.livemode)) {
        throw new Error("Stripe customer ownership could not be verified");
    }
    const snapshot = await synchronizeCustomer(stripe, scope, customerId, userId, "account_refresh");
    if (!snapshot) throw new Error("Stripe customer binding changed during refresh");
    return snapshot.is_premium;
}

async function synchronizeCustomer(
    stripe: Stripe, scope: StripeScope, customerId: string, userId: string, eventId: string,
    requiredActiveId?: string, requireInitialPremium = false,
): Promise<PremiumSnapshot | null> {
    // Re-read current provider state after writing to handle delayed and racing updates.
    for (let attempt = 0; attempt < 3; attempt += 1) {
        const state = await currentCustomerState(stripe, customerId, userId, scope, requiredActiveId);
        if (attempt === 0 && (!state.verified || (requireInitialPremium && !state.snapshot.is_premium))) return null;
        await applySnapshot(userId, state.snapshot, eventId, scope);
        const confirmed = await currentCustomerState(stripe, customerId, userId, scope, requiredActiveId);
        if (JSON.stringify(state) === JSON.stringify(confirmed)) {
            return confirmed.verified && await getStoredCustomerId(userId, scope) === customerId
                ? confirmed.snapshot : null;
        }
    }
    throw new Error("Subscription state changed during synchronization");
}

async function currentCustomerState(
    stripe: Stripe,
    customerId: string,
    userId: string,
    { priceId, livemode }: StripeScope,
    requiredActiveId?: string,
) {
    const subscriptions: Stripe.Subscription[] = [];
    for await (const sub of stripe.subscriptions.list({
        customer: customerId, price: priceId, status: "all", limit: 100,
    })) {
        if (resourceId(sub.customer) === customerId
            && sub.livemode === livemode
            && (!sub.metadata.userId || sub.metadata.userId === userId)
            && (!sub.metadata.clerkUserId || sub.metadata.clerkUserId === userId)
            && hasPrice(sub, priceId)) {
            subscriptions.push(sub);
        }
    }
    subscriptions.sort((left, right) => right.created - left.created);
    const snapshots = subscriptions.map((sub) => ({ sub, snapshot: snapshotFromSubscription(sub, priceId) }));
    const current = snapshots.find(({ snapshot }) => snapshot.is_premium) ?? snapshots[0];
    return {
        snapshot: current?.snapshot ?? revokedSnapshot(customerId, "canceled"),
        verified: !requiredActiveId || snapshots.some(({ sub, snapshot }) =>
            sub.id === requiredActiveId && sub.status === "active" && snapshot.is_premium),
    };
}
