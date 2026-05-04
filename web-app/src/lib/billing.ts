import Stripe from "stripe";
import { clerkClient } from "@clerk/nextjs/server";

export const PREMIUM_KEY = "is_premium" as const;

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
}

function customerIdOf(sub: Stripe.Subscription): string | null {
    if (!sub.customer) return null;
    return typeof sub.customer === "string" ? sub.customer : sub.customer.id;
}

function periodEndOf(sub: Stripe.Subscription): number | null {
    // In Stripe API 2025+, current_period_end lives on subscription items.
    const item = sub.items?.data?.[0] as
        | (Stripe.SubscriptionItem & { current_period_end?: number })
        | undefined;
    if (item?.current_period_end) return item.current_period_end;
    // Fallback for older API versions where it sat on the subscription itself.
    const legacy = (sub as unknown as { current_period_end?: number }).current_period_end;
    return typeof legacy === "number" ? legacy : null;
}

export function snapshotFromSubscription(sub: Stripe.Subscription): PremiumSnapshot {
    return {
        is_premium: isPremiumStatus(sub.status),
        subscription_status: sub.status,
        stripe_customer_id: customerIdOf(sub),
        subscription_current_period_end: periodEndOf(sub),
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
    };
}

/**
 * Find the Clerk userId that owns a Stripe customer. Looks first at the
 * customer's metadata (which we set during checkout), and falls back to
 * scanning Clerk users by stored stripe_customer_id.
 */
export async function findUserIdForCustomer(
    customerId: string,
): Promise<string | null> {
    const stripe = getStripe();
    const customer = await stripe.customers.retrieve(customerId);
    if (customer && !("deleted" in customer && customer.deleted)) {
        const meta = (customer as Stripe.Customer).metadata ?? {};
        if (meta.userId) return meta.userId;
        if (meta.clerkUserId) return meta.clerkUserId;
    }
    return null;
}

/**
 * Look up the userId associated with a webhook event. We prefer subscription
 * metadata (set during checkout) and fall back to the Stripe customer record.
 */
export async function resolveUserId(
    sub: Stripe.Subscription | null,
    customerId: string | null,
): Promise<string | null> {
    const fromSub = sub?.metadata?.userId;
    if (fromSub) return fromSub;
    if (customerId) return findUserIdForCustomer(customerId);
    return null;
}

/**
 * Apply a premium snapshot to a Clerk user. Idempotent: a no-op if the stored
 * state already matches and the same Stripe event id was previously processed.
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
    if (priv.stripe_last_event_id === eventId) {
        return false; // replay
    }
    const pub = (user.publicMetadata ?? {}) as Record<string, unknown>;
    const nextPublic: Record<string, unknown> = {
        ...pub,
        [PREMIUM_KEY]: snapshot.is_premium,
        subscription_status: snapshot.subscription_status,
    };
    if (snapshot.stripe_customer_id) {
        nextPublic.stripe_customer_id = snapshot.stripe_customer_id;
    }
    if (snapshot.subscription_current_period_end !== null) {
        nextPublic.subscription_current_period_end =
            snapshot.subscription_current_period_end;
    } else {
        delete nextPublic.subscription_current_period_end;
    }
    const nextPrivate: Record<string, unknown> = {
        ...priv,
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

/**
 * Get or create a Stripe customer for a Clerk user. Persists the resulting
 * customer id on Clerk so we always reuse it on subsequent checkouts.
 */
export async function getOrCreateCustomer(
    userId: string,
    email: string,
): Promise<string> {
    const stripe = getStripe();
    const clerk = await getClerk();
    const user = await clerk.users.getUser(userId);
    const existing = (user.publicMetadata as Record<string, unknown> | undefined)?.[
        "stripe_customer_id"
    ];
    if (typeof existing === "string" && existing.length > 0) {
        return existing;
    }
    const customer = await stripe.customers.create({
        email,
        metadata: { userId },
    });
    const pub = (user.publicMetadata ?? {}) as Record<string, unknown>;
    await clerk.users.updateUserMetadata(userId, {
        publicMetadata: { ...pub, stripe_customer_id: customer.id },
    });
    return customer.id;
}
