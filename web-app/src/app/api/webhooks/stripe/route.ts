import { headers } from "next/headers";
import { NextResponse } from "next/server";
import type Stripe from "stripe";
import { isPilotOnly, pilotBillingUnavailable } from "@/lib/pilot";
import {
    applySnapshot,
    getStoredCustomerId,
    getStripe,
    getStripePriceId,
    getStripeWebhookSecret,
    isPremiumStatus,
    revokedSnapshot,
    snapshotFromSubscription,
    type PremiumSnapshot,
} from "@/lib/billing";

const HANDLED: ReadonlySet<Stripe.Event["type"]> = new Set([
    "checkout.session.completed",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "customer.subscription.paused",
    "customer.subscription.resumed",
    "invoice.payment_failed",
    "invoice.paid",
]);

export async function POST(req: Request) {
    if (isPilotOnly()) return pilotBillingUnavailable();

    let stripe;
    let webhookSecret: string;
    try {
        stripe = getStripe();
        webhookSecret = getStripeWebhookSecret();
    } catch (error) {
        console.error("[WEBHOOK_CONFIG_ERROR]", error);
        return new NextResponse("Billing not configured", { status: 500 });
    }

    const body = await req.text();
    const signature = (await headers()).get("Stripe-Signature");
    if (!signature) {
        return new NextResponse("Missing signature", { status: 400 });
    }

    let event: Stripe.Event;
    try {
        event = stripe.webhooks.constructEvent(body, signature, webhookSecret);
    } catch (error) {
        const message = error instanceof Error ? error.message : "invalid signature";
        return new NextResponse(`Webhook Error: ${message}`, { status: 400 });
    }

    if (!HANDLED.has(event.type)) {
        return new NextResponse(null, { status: 200 });
    }

    try {
        await handleEvent(stripe, event);
    } catch (error) {
        console.error("[WEBHOOK_HANDLER_ERROR]", event.type, event.id, error);
        // 500 lets Stripe retry; the handler is idempotent so retries are safe.
        return new NextResponse("Webhook handler error", { status: 500 });
    }

    return new NextResponse(null, { status: 200 });
}

async function handleEvent(stripe: Stripe, event: Stripe.Event): Promise<void> {
    switch (event.type) {
        case "checkout.session.completed": {
            const session = event.data.object as Stripe.Checkout.Session;
            if (session.mode !== "subscription" || !session.subscription) return;
            const subId =
                typeof session.subscription === "string"
                    ? session.subscription
                    : session.subscription.id;
            const sub = await stripe.subscriptions.retrieve(subId);
            if (resourceId(session.customer) !== resourceId(sub.customer)) return;
            await syncFromSubscription(stripe, sub, event.id, session.metadata);
            return;
        }
        case "customer.subscription.created":
        case "customer.subscription.updated":
        case "customer.subscription.deleted":
        case "customer.subscription.paused":
        case "customer.subscription.resumed": {
            const sub = event.data.object as Stripe.Subscription;
            await syncFromSubscription(stripe, sub, event.id, null);
            return;
        }
        case "invoice.payment_failed":
        case "invoice.paid": {
            const invoice = event.data.object as Stripe.Invoice;
            const legacy = invoice as Stripe.Invoice & { subscription?: string | Stripe.Subscription };
            const subRef = invoice.parent?.type === "subscription_details"
                ? invoice.parent.subscription_details?.subscription
                : legacy.subscription;
            const subId = typeof subRef === "string" ? subRef : subRef?.id;
            if (!subId) return;
            const sub = await stripe.subscriptions.retrieve(subId);
            if (invoice.customer && resourceId(invoice.customer) !== resourceId(sub.customer)) return;
            await syncFromSubscription(stripe, sub, event.id, null);
            return;
        }
    }
}

function resourceId(resource: string | { id: string } | null): string | null {
    return typeof resource === "string" ? resource : resource?.id ?? null;
}

async function syncFromSubscription(
    stripe: Stripe,
    sub: Stripe.Subscription,
    eventId: string,
    checkoutMetadata: Stripe.Metadata | null,
): Promise<void> {
    const customerId = resourceId(sub.customer);
    if (!customerId) return;
    const customer = await stripe.customers.retrieve(customerId);
    if (customer.id !== customerId) return;
    const deleted = "deleted" in customer && customer.deleted;
    if (!deleted && customer.livemode !== sub.livemode) return;
    const metadata = deleted ? undefined : customer.metadata;
    const owners = new Set([
        checkoutMetadata?.userId, checkoutMetadata?.clerkUserId,
        sub.metadata?.userId, sub.metadata?.clerkUserId,
        metadata?.userId, metadata?.clerkUserId,
    ].filter((id): id is string => Boolean(id)));
    if (owners.size !== 1) return;
    const [userId] = owners;
    if (await getStoredCustomerId(userId) !== customerId) return;
    if (deleted) {
        await applySnapshot(userId, revokedSnapshot(customerId, "canceled"), eventId);
        return;
    }

    const priceId = getStripePriceId();
    // Events can arrive out of order. Reconcile all current subscriptions so
    // an old cancellation cannot revoke a replacement subscription.
    for (let attempt = 0; attempt < 3; attempt += 1) {
        const snapshot = await currentCustomerSnapshot(stripe, customerId, userId, priceId, sub.livemode);
        await applySnapshot(userId, snapshot, eventId);
        const confirmed = await currentCustomerSnapshot(stripe, customerId, userId, priceId, sub.livemode);
        if (JSON.stringify(snapshot) === JSON.stringify(confirmed)) return;
    }
    throw new Error("Subscription state changed during synchronization");
}

async function currentCustomerSnapshot(
    stripe: Stripe,
    customerId: string,
    userId: string,
    priceId: string,
    livemode: boolean,
): Promise<PremiumSnapshot> {
    const subscriptions: Stripe.Subscription[] = [];
    for await (const sub of stripe.subscriptions.list({
        customer: customerId, price: priceId, status: "all", limit: 100,
    })) {
        if (resourceId(sub.customer) === customerId
            && sub.livemode === livemode
            && (!sub.metadata.userId || sub.metadata.userId === userId)
            && (!sub.metadata.clerkUserId || sub.metadata.clerkUserId === userId)
            && sub.items.data.some((item) => item.price.id === priceId)) {
            subscriptions.push(sub);
        }
    }
    subscriptions.sort((left, right) => right.created - left.created);
    const current = subscriptions.find((sub) => isPremiumStatus(sub.status)) ?? subscriptions[0];
    return current ? snapshotFromSubscription(current, priceId) : revokedSnapshot(customerId, "canceled");
}
