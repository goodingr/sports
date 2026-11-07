import type Stripe from "stripe";
import { CHECKOUT_CURRENCY, getClerk } from "./billing";
import type { CheckoutScope, CheckoutSessionParams, CheckoutStore } from "./checkout-store";

const TERMINAL_SUBSCRIPTIONS: ReadonlySet<Stripe.Subscription.Status> = new Set([
    "canceled",
    "incomplete_expired",
]);

type CheckoutResult = { url: string } | { blocked: true };

function ownsCustomer(customer: Stripe.Customer | Stripe.DeletedCustomer, scope: CheckoutScope): boolean {
    return !("deleted" in customer && customer.deleted)
        && customer.livemode === scope.livemode
        && (customer.metadata.userId ?? customer.metadata.clerkUserId) === scope.userId;
}

async function canonicalCustomer(
    stripe: Stripe,
    store: CheckoutStore,
    scope: CheckoutScope,
    legacyCustomerId: string | null,
): Promise<string> {
    let attempt = store.getCustomer(scope);
    if (!attempt) {
        if (legacyCustomerId) {
            const legacy = await stripe.customers.retrieve(legacyCustomerId);
            if (legacy.id !== legacyCustomerId || !ownsCustomer(legacy, scope)) {
                throw new Error("Checkout customer ownership could not be verified");
            }
        }
        attempt = store.reserveCustomer(scope, { metadata: { userId: scope.userId } }, legacyCustomerId ?? undefined);
    }

    let customerId = attempt.stripeId;
    if (legacyCustomerId && customerId !== legacyCustomerId) {
        throw new Error("Checkout customer references disagree");
    }
    if (!customerId) {
        if (!attempt.retryable) throw new Error("Checkout customer needs reconciliation");
        const customer = await stripe.customers.create(attempt.params, { idempotencyKey: attempt.key });
        if (!ownsCustomer(customer, scope) || !store.saveCustomer(scope, attempt.key, customer.id)) {
            throw new Error("Checkout customer response could not be recorded");
        }
        customerId = customer.id;
    }

    if (!legacyCustomerId) {
        const clerk = await getClerk();
        await clerk.users.updateUserMetadata(scope.userId, { publicMetadata: { stripe_customer_id: customerId } });
    }
    return customerId;
}

function resourceId(resource: string | { id: string } | null): string | null {
    return typeof resource === "string" ? resource : resource?.id ?? null;
}

async function hasNonterminalSubscription(stripe: Stripe, customer: string): Promise<boolean> {
    for await (const subscription of stripe.subscriptions.list({ customer, status: "all", limit: 100 })) {
        if (!TERMINAL_SUBSCRIPTIONS.has(subscription.status)) return true;
    }
    return false;
}

async function subscriptionEnded(
    stripe: Stripe, session: Stripe.Checkout.Session, customer: string, livemode: boolean,
): Promise<boolean> {
    const subscriptionId = resourceId(session.subscription);
    if (!subscriptionId) return false;
    const subscription = await stripe.subscriptions.retrieve(subscriptionId);
    return subscription.id === subscriptionId && resourceId(subscription.customer) === customer
        && subscription.livemode === livemode && TERMINAL_SUBSCRIPTIONS.has(subscription.status);
}

async function hasUnresolvedCheckout(
    stripe: Stripe, customer: string, livemode: boolean, knownSessionId?: string,
): Promise<boolean> {
    for await (const session of stripe.checkout.sessions.list({ customer, limit: 100 })) {
        if ((knownSessionId && session.id === knownSessionId)
            || session.mode !== "subscription" || session.status === "expired") {
            continue;
        }
        if (session.status !== "complete" || !(await subscriptionEnded(stripe, session, customer, livemode))) {
            return true;
        }
    }
    return false;
}

export async function createOrResumeCheckout(
    stripe: Stripe,
    store: CheckoutStore,
    scope: CheckoutScope,
    legacyCustomerId: string | null,
    priceId: string,
    appUrl: string,
): Promise<CheckoutResult> {
    const customer = await canonicalCustomer(stripe, store, scope, legacyCustomerId);
    const params: CheckoutSessionParams = {
        customer,
        currency: CHECKOUT_CURRENCY,
        adaptive_pricing: { enabled: false },
        success_url: new URL("/account?success=true&session_id={CHECKOUT_SESSION_ID}", appUrl).href,
        cancel_url: new URL("/pricing?canceled=true", appUrl).href,
        payment_method_types: ["card"],
        mode: "subscription",
        billing_address_collection: "auto",
        line_items: [{ price: priceId, quantity: 1 }],
        subscription_data: { metadata: { userId: scope.userId } },
        metadata: { userId: scope.userId },
    };

    for (let check = 0; check < 4; check += 1) {
        let attempt = store.getSession(scope);
        let replace: { generation: number; sessionId: string } | undefined;
        if (attempt?.stripeId) {
            const session = await stripe.checkout.sessions.retrieve(attempt.stripeId);
            if (session.id !== attempt.stripeId || session.mode !== "subscription"
                || resourceId(session.customer) !== customer || session.livemode !== scope.livemode
                || session.metadata?.userId !== scope.userId) {
                throw new Error("Checkout session ownership could not be verified");
            }
            if (session.status === "open") {
                if (attempt.params.line_items[0].price !== priceId || !session.url
                    || new URL(session.url).protocol !== "https:" || session.expires_at * 1000 <= Date.now()) {
                    throw new Error("Existing checkout needs reconciliation");
                }
                if (await hasUnresolvedCheckout(stripe, customer, scope.livemode, session.id)
                    || await hasNonterminalSubscription(stripe, customer)) return { blocked: true };
                return { url: session.url };
            }
            if (session.status === "complete") {
                if (!(await subscriptionEnded(stripe, session, customer, scope.livemode))) return { blocked: true };
            } else if (session.status !== "expired") {
                throw new Error("Checkout session is in an unknown state");
            }
            replace = { generation: attempt.generation, sessionId: attempt.stripeId };
        }

        if (!attempt || replace) {
            if (await hasUnresolvedCheckout(stripe, customer, scope.livemode, replace?.sessionId)) {
                const current = store.getSession(scope);
                if (current && current.key !== attempt?.key) continue;
                return { blocked: true };
            }
            if (await hasNonterminalSubscription(stripe, customer)) return { blocked: true };
            attempt = store.reserveSession(scope, params, replace);
            if (attempt.stripeId) continue;
        }
        if (!attempt.retryable || attempt.params.line_items[0].price !== priceId) {
            throw new Error("Checkout session needs reconciliation");
        }
        const session = await stripe.checkout.sessions.create(attempt.params, { idempotencyKey: attempt.key });
        if (!store.saveSession(scope, attempt.key, session.id)) {
            throw new Error("Checkout session response could not be recorded");
        }
        // A replayed create can return the original open response after payment.
        // Only a fresh retrieve in the next iteration can return a checkout URL.
    }
    throw new Error("Checkout session changed during verification");
}
