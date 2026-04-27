import { headers } from "next/headers";
import { NextResponse } from "next/server";
import type Stripe from "stripe";
import {
    applySnapshot,
    getStripe,
    getStripeWebhookSecret,
    resolveUserId,
    revokedSnapshot,
    snapshotFromSubscription,
} from "@/lib/billing";

const HANDLED: ReadonlySet<Stripe.Event["type"]> = new Set([
    "checkout.session.completed",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "invoice.payment_failed",
]);

export async function POST(req: Request) {
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
            await syncFromSubscription(sub, event.id, session.metadata?.userId ?? null);
            return;
        }
        case "customer.subscription.created":
        case "customer.subscription.updated": {
            const sub = event.data.object as Stripe.Subscription;
            await syncFromSubscription(sub, event.id, null);
            return;
        }
        case "customer.subscription.deleted": {
            const sub = event.data.object as Stripe.Subscription;
            await syncDeleted(sub, event.id);
            return;
        }
        case "invoice.payment_failed": {
            const invoice = event.data.object as Stripe.Invoice;
            await syncFromInvoiceFailure(stripe, invoice, event.id);
            return;
        }
    }
}

async function syncFromSubscription(
    sub: Stripe.Subscription,
    eventId: string,
    userIdHint: string | null,
): Promise<void> {
    const customerId =
        typeof sub.customer === "string" ? sub.customer : sub.customer?.id ?? null;
    const userId = userIdHint ?? (await resolveUserId(sub, customerId));
    if (!userId) {
        console.warn("[WEBHOOK] no userId for subscription", sub.id);
        return;
    }
    await applySnapshot(userId, snapshotFromSubscription(sub), eventId);
}

async function syncDeleted(sub: Stripe.Subscription, eventId: string): Promise<void> {
    const customerId =
        typeof sub.customer === "string" ? sub.customer : sub.customer?.id ?? null;
    const userId = await resolveUserId(sub, customerId);
    if (!userId) {
        console.warn("[WEBHOOK] no userId for deleted subscription", sub.id);
        return;
    }
    await applySnapshot(userId, revokedSnapshot(customerId, "canceled"), eventId);
}

async function syncFromInvoiceFailure(
    stripe: Stripe,
    invoice: Stripe.Invoice,
    eventId: string,
): Promise<void> {
    const subRef = (invoice as unknown as { subscription?: string | Stripe.Subscription })
        .subscription;
    const subId = typeof subRef === "string" ? subRef : subRef?.id;
    if (!subId) return;
    const sub = await stripe.subscriptions.retrieve(subId);
    const customerId =
        typeof sub.customer === "string" ? sub.customer : sub.customer?.id ?? null;
    const userId = await resolveUserId(sub, customerId);
    if (!userId) {
        console.warn("[WEBHOOK] no userId for failed invoice", invoice.id);
        return;
    }
    // Re-fetch state from Stripe — could be past_due, unpaid, or already active
    // again if a retry succeeded by the time we processed this event.
    await applySnapshot(userId, snapshotFromSubscription(sub), eventId);
}
