import Stripe from "stripe";
import { clerkClient } from "@clerk/nextjs/server";

export const PREMIUM_KEY = "is_premium" as const;
export const CHECKOUT_CURRENCY = "usd" as const;

const MONTHLY_PRICE_CENTS = 2900;

const ACTIVE_STATUSES: ReadonlySet<Stripe.Subscription.Status> = new Set([
    "active",
    "trialing",
]);

function requireEnv(name: string): string {
    const value = process.env[name];
    if (!value) {
        throw new Error(`${name} is not configured`);
    }
    return value;
}

export function getStripeSecret(): string {
    return requireEnv("STRIPE_SECRET_KEY");
}

export function getStripePriceId(): string {
    return requireEnv("STRIPE_PRICE_ID");
}

export async function getVerifiedCheckoutPrice(stripe: Stripe): Promise<Pick<Stripe.Price, "id" | "livemode"> | null> {
    const priceId = getStripePriceId();
    const price = await stripe.prices.retrieve(priceId, { expand: ["product"] });
    const { product, recurring, unit_amount_decimal: decimal } = price;
    if (
        price.id !== priceId
        || typeof price.livemode !== "boolean"
        || price.active !== true
        || !product || typeof product === "string" || !("active" in product) || product.active !== true
        || price.currency !== CHECKOUT_CURRENCY
        || price.unit_amount !== MONTHLY_PRICE_CENTS
        || (decimal != null && !/^2900(?:\.0+)?$/.test(decimal))
        || price.type !== "recurring"
        || price.billing_scheme !== "per_unit"
        || price.custom_unit_amount != null
        || price.transform_quantity != null
        || recurring?.interval !== "month"
        || recurring.interval_count !== 1
        || recurring.usage_type !== "licensed"
        || recurring.meter != null
        || (recurring.trial_period_days != null && recurring.trial_period_days !== 0)
    ) {
        return null;
    }
    return { id: priceId, livemode: price.livemode };
}

export function getStripeWebhookSecret(): string {
    return requireEnv("STRIPE_WEBHOOK_SECRET");
}

export function getAppUrl(): string {
    return requireEnv("NEXT_PUBLIC_APP_URL");
}

let _stripe: Stripe | null = null;

export function getStripe(): Stripe {
    if (!_stripe) {
        _stripe = new Stripe(getStripeSecret(), { typescript: true });
    }
    return _stripe;
}

// Test seam — lets tests inject a mocked Stripe client.
export function setStripeForTests(client: Stripe | null): void {
    _stripe = client;
}

type ClerkClient = Awaited<ReturnType<typeof clerkClient>>;
let _clerkOverride: ClerkClient | null = null;

export async function getClerk(): Promise<ClerkClient> {
    if (_clerkOverride) return _clerkOverride;
    return clerkClient();
}

// Test seam — lets tests inject a mocked Clerk client.
export function setClerkForTests(client: ClerkClient | null): void {
    _clerkOverride = client;
}

export function isPremiumStatus(status: string | null | undefined): boolean {
    if (!status) return false;
    return ACTIVE_STATUSES.has(status as Stripe.Subscription.Status);
}

export interface PremiumSnapshot {
    is_premium: boolean;
    subscription_status: string | null;
    stripe_customer_id: string | null;
    subscription_current_period_end: number | null;
    subscription_cancel_at_period_end: boolean;
}

function customerIdOf(sub: Stripe.Subscription): string | null {
    if (!sub.customer) return null;
    return typeof sub.customer === "string" ? sub.customer : sub.customer.id;
}

function periodEndOf(sub: Stripe.Subscription, priceId?: string): number | null {
    // In Stripe API 2025+, current_period_end lives on subscription items.
    const items = sub.items?.data;
    const item = (priceId ? items?.find(({ price }) => price.id === priceId) : items?.[0]) as
        | (Stripe.SubscriptionItem & { current_period_end?: number })
        | undefined;
    // Fallback for older API versions where it sat on the subscription itself.
    const legacy = (sub as unknown as { current_period_end?: number }).current_period_end;
    const periodEnd = item?.current_period_end ?? legacy;
    if (typeof periodEnd !== "number") return null;
    return sub.cancel_at ? Math.min(periodEnd, sub.cancel_at) : periodEnd;
}

export function snapshotFromSubscription(sub: Stripe.Subscription, priceId?: string): PremiumSnapshot {
    return {
        is_premium: isPremiumStatus(sub.status),
        subscription_status: sub.status,
        stripe_customer_id: customerIdOf(sub),
        subscription_current_period_end: periodEndOf(sub, priceId),
        subscription_cancel_at_period_end: sub.cancel_at_period_end || sub.cancel_at != null,
    };
}

export function revokedSnapshot(
    customerId: string | null,
    status: string,
): PremiumSnapshot {
    return {
        is_premium: false,
        subscription_status: status,
        stripe_customer_id: customerId,
        subscription_current_period_end: null,
        subscription_cancel_at_period_end: false,
    };
}

/**
 * Apply a current premium snapshot for the customer already bound to the user.
 * Repeated events skip writes when the stored billing state already matches.
 *
 * Returns true when state changed (or the event was applied for the first time).
 */
export async function applySnapshot(
    userId: string,
    snapshot: PremiumSnapshot,
    eventId: string,
): Promise<boolean> {
    const clerk = await getClerk();
    const user = await clerk.users.getUser(userId);
    const priv = (user.privateMetadata ?? {}) as Record<string, unknown>;
    const pub = (user.publicMetadata ?? {}) as Record<string, unknown>;
    const { stripe_customer_id, ...entitlement } = snapshot;
    if (!stripe_customer_id || pub.stripe_customer_id !== stripe_customer_id) return false;
    const nextPublic: Record<string, unknown> = { ...entitlement, isPremium: null };
    const matchesStored = Object.entries(nextPublic).every(([key, value]) =>
        value === null ? pub[key] == null : pub[key] === value,
    );
    if (priv.stripe_last_event_id === eventId && matchesStored) {
        return false;
    }
    const nextPrivate: Record<string, unknown> = {
        stripe_last_event_id: eventId,
        stripe_last_event_at: Math.floor(Date.now() / 1000),
    };
    await clerk.users.updateUserMetadata(userId, {
        publicMetadata: nextPublic,
        privateMetadata: nextPrivate,
    });
    return true;
}

/**
 * Look up the Stripe customer id stored on a Clerk user. Returns null when
 * the user has never run checkout (no customer record exists yet).
 */
export async function getStoredCustomerId(userId: string): Promise<string | null> {
    const clerk = await getClerk();
    const user = await clerk.users.getUser(userId);
    const id = (user.publicMetadata as Record<string, unknown> | undefined)?.[
        "stripe_customer_id"
    ];
    return typeof id === "string" && id.length > 0 ? id : null;
}

/**
 * Open a Stripe Billing Portal session for a customer so they can manage or
 * cancel their subscription. Returns the redirect URL.
 */
export async function createBillingPortalSession(
    customerId: string,
    returnUrl: string,
): Promise<string> {
    const stripe = getStripe();
    const session = await stripe.billingPortal.sessions.create({
        customer: customerId,
        return_url: returnUrl,
    });
    return session.url;
}
