"use client";

import { Container } from "@/components/ui/Container";
import { Check } from "lucide-react";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";

export default function Page() {
    return (
        <Container className="py-20">
            <div className="text-center mb-16">
                <h1 className="text-4xl font-bold text-foreground mb-4">Simple, Transparent Pricing</h1>
                <p className="text-xl text-muted-foreground">Unlock premium insights and start winning today.</p>
            </div>

            <div className="max-w-md mx-auto bg-card border border-primary/20 rounded-2xl overflow-hidden shadow-2xl shadow-primary/10">
                <div className="p-8">
                    <h3 className="text-2xl font-bold text-foreground mb-2">Pro Access</h3>
                    <div className="flex items-baseline gap-1 mb-6">
                        <span className="text-4xl font-bold text-foreground">$29</span>
                        <span className="text-muted-foreground">/month</span>
                    </div>

                    <ul className="space-y-4 mb-8">
                        <Feature text="Unlimited Access to All Picks" />
                        <Feature text="Real-time Odds & Edge Analysis" />
                        <Feature text="Detailed Performance Tracking" />
                        <Feature text="Email Notifications" />
                        <Feature text="Cancel Anytime" />
                    </ul>

                    <CheckoutButton />
                </div>
            </div>
        </Container>
    );
}

function CheckoutButton() {
    const [loading, setLoading] = useState(false);
    const router = useRouter();
    const { isSignedIn } = useAuth();

    const handleCheckout = async () => {
        if (!isSignedIn) {
            router.push("/signup");
            return;
        }

        try {
            setLoading(true);
            const response = await fetch("/api/checkout", {
                method: "POST",
            });

            const data = await response.json();
            if (data.url) {
                window.location.href = data.url;
            } else {
                console.error("No checkout URL returned");
            }
        } catch (error) {
            console.error("Checkout error:", error);
        } finally {
            setLoading(false);
        }
    };

    return (
        <button
            onClick={handleCheckout}
            disabled={loading}
            className="block w-full bg-primary text-primary-foreground text-center py-3 rounded-lg font-bold hover:bg-amber-500 transition-colors disabled:opacity-50"
        >
            {loading ? "Loading..." : "Get Started"}
        </button>
    );
}

function Feature({ text }: { text: string }) {
    return (
        <li className="flex items-center gap-3">
            <div className="p-1 bg-success/10 rounded-full">
                <Check className="h-4 w-4 text-success" />
            </div>
            <span className="text-foreground">{text}</span>
        </li>
    );
}
