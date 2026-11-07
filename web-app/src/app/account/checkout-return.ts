"use server";

import { auth } from "@clerk/nextjs/server";
import { getStripe } from "@/lib/billing";
import { isPilotOnly } from "@/lib/pilot";
import { isCheckoutSessionId, reconcilePaidCheckoutReturn } from "@/lib/stripe-entitlements";

export async function reconcileCheckoutReturn(sessionId: unknown): Promise<boolean> {
    if (isPilotOnly() || !isCheckoutSessionId(sessionId)) return false;

    try {
        const { userId } = await auth();
        return userId ? await reconcilePaidCheckoutReturn(getStripe(), sessionId, userId) : false;
    } catch {
        return false;
    }
}
