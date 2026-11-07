"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { refreshAccountSubscription } from "./refresh-subscription";

const MESSAGES = {
    active: "Stripe confirmed your subscription. Pick access may take a moment to update.",
    inactive: "Stripe reported no current subscription access. Check your payment details in Manage Subscription.",
    unavailable: "We couldn’t verify your subscription with Stripe. Try again shortly.",
    cooldown: "Wait a moment before checking again.",
};

export function RefreshSubscriptionButton() {
    const router = useRouter();
    const [loading, setLoading] = useState(false);
    const [status, setStatus] = useState<keyof typeof MESSAGES | null>(null);

    async function refresh() {
        setLoading(true);
        setStatus(null);
        try {
            const result = await refreshAccountSubscription();
            setStatus(result);
            if (result === "active" || result === "inactive") router.refresh();
        } catch {
            setStatus("unavailable");
        } finally {
            setLoading(false);
        }
    }

    return (
        <div>
            <button
                type="button"
                onClick={refresh}
                disabled={loading}
                aria-busy={loading}
                className="rounded-lg border border-white/10 px-4 py-2 text-sm text-foreground hover:bg-white/5 disabled:opacity-50"
            >
                {loading ? "Checking subscription…" : "Refresh subscription status"}
            </button>
            {status && <p role="status" className="mt-2 text-sm text-muted-foreground">{MESSAGES[status]}</p>}
        </div>
    );
}
