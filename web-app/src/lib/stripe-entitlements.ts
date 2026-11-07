import "server-only";

import type Stripe from "stripe";
import {
    applySnapshot,
    CHECKOUT_CURRENCY,
    getStoredCustomerId,
    getStripePriceId,
    isPremiumStatus,
    revokedSnapshot,
    snapshotFromSubscription,
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
    const priceId = getStripePriceId();
    const customerId = await getStoredCustomerId(userId);
    if (!customerId) return false;

    const session = await stripe.checkout.sessions.retrieve(sessionId, { expand: ["line_items"] });
    const owner = session.metadata?.userId ?? session.metadata?.clerkUserId;
    const items = session.line_items;
    if (session.id !== sessionId || resourceId(session.customer) !== customerId || owner !== userId
        || session.mode !== "subscription" || session.status !== "complete" || session.payment_status !== "paid"
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
    const customerId = resourceId(sub.customer);
    if (!customerId) return false;
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
    if ((expectedUserId && expectedUserId !== userId) || await getStoredCustomerId(userId) !== customerId) {
        return false;
    }
    if (deleted) {
        await applySnapshot(userId, revokedSnapshot(customerId, "canceled"), eventId);
        return false;
    }

    const priceId = getStripePriceId();
    const requiredActiveId = expectedUserId ? sub.id : undefined;
    // Re-read current provider state after writing to handle delayed and racing updates.
    for (let attempt = 0; attempt < 3; attempt += 1) {
        const state = await currentCustomerState(stripe, customerId, userId, priceId, sub.livemode, requiredActiveId);
        if (attempt === 0 && !state.verified) return false;
        await applySnapshot(userId, state.snapshot, eventId);
        const confirmed = await currentCustomerState(stripe, customerId, userId, priceId, sub.livemode, requiredActiveId);
        if (JSON.stringify(state) === JSON.stringify(confirmed)) {
            return confirmed.verified && (!expectedUserId || await getStoredCustomerId(userId) === customerId);
        }
    }
    throw new Error("Subscription state changed during synchronization");
}

async function currentCustomerState(
    stripe: Stripe,
    customerId: string,
    userId: string,
    priceId: string,
    livemode: boolean,
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
    const current = subscriptions.find((sub) => isPremiumStatus(sub.status)) ?? subscriptions[0];
    return {
        snapshot: current ? snapshotFromSubscription(current, priceId) : revokedSnapshot(customerId, "canceled"),
        verified: !requiredActiveId || subscriptions.some((sub) => sub.id === requiredActiveId && sub.status === "active"),
    };
}
