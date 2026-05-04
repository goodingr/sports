import { NextResponse } from "next/server";
import { auth } from "@clerk/nextjs/server";
import {
    createBillingPortalSession,
    getAppUrl,
    getStoredCustomerId,
    getStripe,
} from "@/lib/billing";

export async function POST() {
    try {
        // Force-load the Stripe client so misconfigured envs fail fast and
        // we return a clear 500 instead of a confusing portal error.
        getStripe();
    } catch (error) {
        console.error("[PORTAL_CONFIG_ERROR]", error);
        return new NextResponse("Billing not configured", { status: 500 });
    }

    let appUrl: string;
    try {
        appUrl = getAppUrl();
    } catch (error) {
        console.error("[PORTAL_CONFIG_ERROR]", error);
        return new NextResponse("Billing not configured", { status: 500 });
    }

    try {
        const { userId } = await auth();
        if (!userId) {
            return new NextResponse("Unauthorized", { status: 401 });
        }

        const customerId = await getStoredCustomerId(userId);
        if (!customerId) {
            return new NextResponse("No subscription found", { status: 404 });
        }

        const url = await createBillingPortalSession(
            customerId,
            `${appUrl}/account`,
        );
        return NextResponse.json({ url });
    } catch (error) {
        console.error("[PORTAL_ERROR]", error);
        return new NextResponse("Internal Error", { status: 500 });
    }
}
