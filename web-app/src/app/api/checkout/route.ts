import { NextResponse } from "next/server";
import { auth, currentUser } from "@clerk/nextjs/server";
import { isCheckoutAvailable } from "@/lib/api";
import {
    getAppUrl,
    getOrCreateCustomer,
    getStripe,
    getStripePriceId,
} from "@/lib/billing";

export async function POST() {
    try {
        const { userId } = await auth();
        if (!userId) {
            return new NextResponse("Unauthorized", { status: 401 });
        }
        if (!(await isCheckoutAvailable())) {
            return new NextResponse("Subscriptions are currently unavailable", { status: 503 });
        }

        const user = await currentUser();
        if (!user) {
            return new NextResponse("Unauthorized", { status: 401 });
        }
        if (user.publicMetadata.is_premium === true || user.publicMetadata.isPremium === true) {
            return new NextResponse("Already subscribed", { status: 409 });
        }

        const email = user.emailAddresses[0]?.emailAddress;
        if (!email) {
            return new NextResponse("Email required", { status: 400 });
        }

        const stripe = getStripe();
        const priceId = getStripePriceId();
        const appUrl = getAppUrl();
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
