"use server";

import { auth } from "@clerk/nextjs/server";
import { configuredStripeScope, getStripe } from "@/lib/billing";
import { isPilotOnly } from "@/lib/pilot";
import { reconcileAccountSubscription } from "@/lib/stripe-entitlements";

type RefreshStatus = "active" | "inactive" | "unavailable" | "cooldown";
type RefreshEntry = { pending: Promise<RefreshStatus> | null; retryAt: number };
const REFRESH_COOLDOWN_MS = 30_000;
const refreshes = new Map<string, RefreshEntry>();

export async function refreshAccountSubscription(): Promise<RefreshStatus> {
    if (isPilotOnly()) return "unavailable";
    try {
        const { userId } = await auth();
        const scope = configuredStripeScope();
        if (!userId || !scope) return "unavailable";

        const existing = refreshes.get(userId);
        if (existing?.pending) return existing.pending;
        if (existing && existing.retryAt > Date.now()) return "cooldown";

        const pending = reconcileAccountSubscription(getStripe(), scope, userId).then<RefreshStatus, RefreshStatus>(
            (active) => active ? "active" : "inactive",
            () => "unavailable",
        );
        const entry: RefreshEntry = { pending, retryAt: 0 };
        refreshes.set(userId, entry);
        try {
            return await pending;
        } finally {
            entry.pending = null;
            entry.retryAt = Date.now() + REFRESH_COOLDOWN_MS;
            setTimeout(() => {
                if (refreshes.get(userId) === entry) refreshes.delete(userId);
            }, REFRESH_COOLDOWN_MS).unref();
        }
    } catch {
        return "unavailable";
    }
}
