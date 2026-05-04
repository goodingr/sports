import { NextResponse } from "next/server";
import { auth, currentUser } from "@clerk/nextjs/server";
import {
    getAppUrl,
    getOrCreateCustomer,
    getStripe,
    getStripePriceId,
} from "@/lib/billing";

export async function POST() {
    let stripe;
    let priceId: string;
    let appUrl: string;
    try {
        stripe = getStripe();
        priceId = getStripePriceId();
        appUrl = getAppUrl();
    } catch (error) {
        console.error("[CHECKOUT_CONFIG_ERROR]", error);
        return new NextResponse("Billing not configured", { status: 500 });
    }

    try {
        const { userId } = await auth();
        const user = await currentUser();

        if (!userId || !user) {
            return new NextResponse("Unauthorized", { status: 401 });
        }

        const email = user.emailAddresses[0]?.emailAddress;
        if (!email) {
            return new NextResponse("Email required", { status: 400 });
        }

        const customerId = await getOrCreateCustomer(userId, email);

        const session = await stripe.checkout.sessions.create({
            customer: customerId,
            success_url: `${appUrl}/account?success=true`,
            cancel_url: `${appUrl}/pricing?canceled=true`,
            payment_method_types: ["card"],
            mode: "subscription",
            billing_address_collection: "auto",
            line_items: [{ price: priceId, quantity: 1 }],
            subscription_data: {
                metadata: { userId },
            },
            metadata: { userId },
        });

        return NextResponse.json({ url: session.url });
    } catch (error) {
        console.error("[STRIPE_ERROR]", error);
        return new NextResponse("Internal Error", { status: 500 });
    }
}
