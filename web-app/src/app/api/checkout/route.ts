import { NextResponse } from "next/server";
import { auth, currentUser } from "@clerk/nextjs/server";
import { isCheckoutAvailable } from "@/lib/api";
import {
    configuredStripeScope, getAppUrl, getStripe, getVerifiedCheckoutPrice,
    getVerifiedStripeAccount, hasScopedPremium,
} from "@/lib/billing";
import { createOrResumeCheckout } from "@/lib/checkout";
import { CheckoutStore } from "@/lib/checkout-store";
import { isPilotOnly, pilotBillingUnavailable } from "@/lib/pilot";
import { reconcileExistingCheckoutCustomer } from "@/lib/stripe-entitlements";

export const runtime = "nodejs";

export async function POST() {
    if (isPilotOnly()) return pilotBillingUnavailable();

    try {
        const { userId } = await auth();
        if (!userId) return new NextResponse("Unauthorized", { status: 401 });
        if (!(await isCheckoutAvailable())) {
            return new NextResponse("Subscriptions are currently unavailable", { status: 503 });
        }

        const user = await currentUser();
        if (!user || user.id !== userId) return new NextResponse("Unauthorized", { status: 401 });
        const scope = configuredStripeScope();
        if (!scope) throw new Error("Stripe scope is not configured");
        const path = process.env.SPORTS_CHECKOUT_DB_PATH;
        if (!path) throw new Error("Checkout storage is not configured");
        const stripe = getStripe();
        const price = await getVerifiedCheckoutPrice(stripe, scope);
        if (!price) throw new Error("Checkout price differs from the offer");
        const account = await getVerifiedStripeAccount(stripe, scope);
        if (scope.livemode && account.charges_enabled !== true) {
            throw new Error("Stripe account cannot accept charges");
        }
        if (hasScopedPremium(user.publicMetadata, scope)) {
            return new NextResponse("Already subscribed", { status: 409 });
        }
        const store = new CheckoutStore(path, { requireExisting: true });
        try {
            const result = await createOrResumeCheckout(
                stripe, store, { ...scope, userId }, user.publicMetadata, getAppUrl(),
            );
            if ("blocked" in result) {
                await reconcileExistingCheckoutCustomer(stripe, scope, result.customer, userId);
                return new NextResponse("Subscription or payment already in progress", { status: 409 });
            }
            return NextResponse.json({ url: result.url }, { headers: { "Cache-Control": "no-store" } });
        } finally {
            store.close();
        }
    } catch {
        console.error("[STRIPE_CHECKOUT_UNAVAILABLE]");
        return new NextResponse("Subscriptions are currently unavailable", { status: 503 });
    }
}
