import { NextResponse } from "next/server";
import { auth } from "@clerk/nextjs/server";
import { isPilotOnly, pilotBillingUnavailable } from "@/lib/pilot";
import {
    configuredStripeScope,
    createBillingPortalSession,
    getAppUrl,
    getStoredCustomerId,
    getStripe,
    getVerifiedStripeAccount,
    ownsStripeCustomer,
} from "@/lib/billing";

export async function POST() {
    if (isPilotOnly()) return pilotBillingUnavailable();

    try {
        const { userId } = await auth();
        if (!userId) {
            return new NextResponse("Unauthorized", { status: 401 });
        }

        const scope = configuredStripeScope();
        if (!scope) return new NextResponse("Billing not configured", { status: 500 });
        const customerId = await getStoredCustomerId(userId, scope);
        if (!customerId) {
            return new NextResponse("No subscription found", { status: 404 });
        }
        const stripe = getStripe();
        await getVerifiedStripeAccount(stripe, scope);
        const customer = await stripe.customers.retrieve(customerId);
        if (customer.id !== customerId || !ownsStripeCustomer(customer, userId, scope.livemode)) {
            return new NextResponse("No subscription found", { status: 404 });
        }
        const url = await createBillingPortalSession(
            stripe,
            customerId,
            `${getAppUrl()}/account`,
        );
        return NextResponse.json({ url });
    } catch (error) {
        console.error("[PORTAL_ERROR]", error);
        return new NextResponse("Internal Error", { status: 500 });
    }
}
