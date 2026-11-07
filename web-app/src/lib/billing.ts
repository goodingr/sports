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

export type StripeScope = { account: string; livemode: boolean; priceId: string };

function isStripeAccountId(value: unknown): value is string {
    return typeof value === "string" && /^acct_[A-Za-z0-9_]+$/.test(value);
}

export function configuredStripeScope(): StripeScope | null {
    const account = process.env.STRIPE_ACCOUNT_ID;
    const priceId = process.env.STRIPE_PRICE_ID;
    const key = /^(?:sk|rk)_(test|live)_[A-Za-z0-9_]+$/.exec(process.env.STRIPE_SECRET_KEY ?? "");
    if (!isStripeAccountId(account) || !priceId || !/^price_[A-Za-z0-9_]+$/.test(priceId) || !key) return null;
    return { account, priceId, livemode: key[1] === "live" };
}

export async function getVerifiedStripeAccount(stripe: Stripe, scope: StripeScope): Promise<Stripe.Account> {
    const account = await stripe.accounts.retrieve();
    if (account.id !== scope.account) throw new Error("Stripe account differs from billing configuration");
    return account;
}

export async function getVerifiedCheckoutPrice(
    stripe: Stripe, scope: StripeScope,
): Promise<Pick<Stripe.Price, "id" | "livemode"> | null> {
    const { priceId } = scope;
    const price = await stripe.prices.retrieve(priceId, { expand: ["product"] });
    const { product, recurring, unit_amount_decimal: decimal } = price;
    if (
        price.id !== priceId
        || price.livemode !== scope.livemode
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

function isStripeCustomerId(value: unknown): value is string {
    return typeof value === "string" && /^cus_[A-Za-z0-9_]+$/.test(value);
}

export function hasScopedPremium(metadata: Record<string, unknown>, scope: StripeScope | null): boolean {
    if (!scope || metadata.is_premium !== true) return false;
    const customerId = scopedCustomerId(metadata, scope);
    const entitlement = metadata.stripe_entitlement;
    if (!customerId || !entitlement || typeof entitlement !== "object" || Array.isArray(entitlement)) return false;
    const proof = entitlement as Record<string, unknown>;
    return proof.customer_id === customerId && proof.account_id === scope.account
        && proof.livemode === scope.livemode && proof.price_id === scope.priceId;
}

export function scopedCustomerId(metadata: Record<string, unknown>, scope: StripeScope | null): string | null {
    const customerId = metadata.stripe_customer_id;
    return scope && metadata.stripe_customer_account_id === scope.account
        && metadata.stripe_customer_livemode === scope.livemode
        && isStripeCustomerId(customerId) ? customerId : null;
}

export function customerBinding(metadata: Record<string, unknown>, scope: StripeScope) {
    const id = metadata.stripe_customer_id;
    if (id == null) return null;
    if (!isStripeCustomerId(id)) throw new Error("Stored Stripe customer reference is invalid");
    const { stripe_customer_account_id: account, stripe_customer_livemode: livemode } = metadata;
    if (account == null && livemode == null) return { id, scoped: false };
    if (!isStripeAccountId(account) || typeof livemode !== "boolean") {
        throw new Error("Stored Stripe customer has incomplete scope");
    }
    return account === scope.account && livemode === scope.livemode ? { id, scoped: true } : null;
}

export function customerBindingMetadata(customerId: string, scope: StripeScope) {
    return {
        stripe_customer_id: customerId,
        stripe_customer_account_id: scope.account,
        stripe_customer_livemode: scope.livemode,
    };
}

export function ownsStripeCustomer(
    customer: Stripe.Customer | Stripe.DeletedCustomer, userId: string, livemode: boolean,
): customer is Stripe.Customer {
    if (customer.deleted || customer.livemode !== livemode || !isStripeCustomerId(customer.id)) return false;
    const owners = [customer.metadata.userId, customer.metadata.clerkUserId].filter(Boolean);
    return owners.length > 0 && owners.every((owner) => owner === userId);
}

export function isMissingStripeCustomer(error: unknown): boolean {
    return error instanceof Stripe.errors.StripeInvalidRequestError
        && error.code === "resource_missing" && error.statusCode === 404;
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
    scope: StripeScope,
): Promise<boolean> {
    const clerk = await getClerk();
    const user = await clerk.users.getUser(userId);
    const priv = (user.privateMetadata ?? {}) as Record<string, unknown>;
    const pub = (user.publicMetadata ?? {}) as Record<string, unknown>;
    const { stripe_customer_id, ...entitlement } = snapshot;
    const binding = customerBinding(pub, scope);
    if (!stripe_customer_id || binding?.id !== stripe_customer_id) return false;
    const proof = snapshot.is_premium ? {
        customer_id: stripe_customer_id, account_id: scope.account, livemode: scope.livemode, price_id: scope.priceId,
    } : null;
    const nextPublic = {
        ...entitlement, isPremium: null,
        ...(!binding.scoped && {
            stripe_customer_account_id: scope.account,
            stripe_customer_livemode: scope.livemode,
        }),
    };
    const matchesStored = (proof ? hasScopedPremium(pub, scope) : pub.stripe_entitlement == null)
        && Object.entries(nextPublic).every(([key, value]) =>
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
        publicMetadata: { ...nextPublic, stripe_entitlement: proof },
        privateMetadata: nextPrivate,
    });
    return true;
}

/**
 * Look up the Stripe customer id stored on a Clerk user. Returns null when
 * the user has never run checkout (no customer record exists yet).
 */
export async function getStoredCustomerId(userId: string, scope: StripeScope): Promise<string | null> {
    const clerk = await getClerk();
    const user = await clerk.users.getUser(userId);
    return customerBinding(user.publicMetadata ?? {}, scope)?.id ?? null;
}

/**
 * Open a Stripe Billing Portal session for a customer so they can manage or
 * cancel their subscription. Returns the redirect URL.
 */
export async function createBillingPortalSession(
    stripe: Stripe,
    customerId: string,
    returnUrl: string,
): Promise<string> {
    const session = await stripe.billingPortal.sessions.create({
        customer: customerId,
        return_url: returnUrl,
    });
    return session.url;
}
