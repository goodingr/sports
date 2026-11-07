"use client";

import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { reconcileCheckoutReturn } from "./checkout-return";

export function CheckoutReturnStatus({ sessionId }: { sessionId: string }) {
    const router = useRouter();
    const [status, setStatus] = useState<"checking" | "confirmed" | "unverified">("checking");

    useEffect(() => {
        let active = true;
        void reconcileCheckoutReturn(sessionId).then((confirmed) => {
            if (!active) return;
            setStatus(confirmed ? "confirmed" : "unverified");
            if (confirmed) router.refresh();
        }).catch(() => {
            if (active) setStatus("unverified");
        });
        return () => { active = false; };
    }, [router, sessionId]);

    return (
        <div role="status" className="mb-6 rounded-xl border border-white/10 bg-card p-4 text-sm text-muted-foreground">
            {status === "checking" && "Checking your checkout with Stripe…"}
            {status === "confirmed" && "Payment confirmed. Your subscription is active."}
            {status === "unverified" && "We couldn’t confirm this checkout yet. If you paid, refresh this page to check your subscription."}
        </div>
    );
}
