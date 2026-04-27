'use client';

import { Container } from '@/components/ui/Container';
import { Check, AlertCircle } from 'lucide-react';
import Link from 'next/link';
import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth, useUser } from '@clerk/nextjs';

function hasPremiumAccess(metadata: Record<string, unknown> | null | undefined): boolean {
    return metadata?.is_premium === true || metadata?.isPremium === true;
}

export default function Page() {
    return (
        <Container className="py-12 sm:py-20 px-4">
            <div className="text-center mb-10 sm:mb-16">
                <h1 className="text-3xl sm:text-4xl font-bold text-foreground mb-4">
                    Simple, Transparent Pricing
                </h1>
                <p className="text-base sm:text-xl text-muted-foreground">
                    Unlock our full library of model-driven analytics and research tools.
                </p>
            </div>

            <div className="max-w-md mx-auto bg-card border border-primary/20 rounded-2xl overflow-hidden shadow-2xl shadow-primary/10">
                <div className="p-6 sm:p-8">
                    <h2 className="text-xl sm:text-2xl font-bold text-foreground mb-2">Pro Access</h2>
                    <div className="flex items-baseline gap-1 mb-6">
                        <span className="text-3xl sm:text-4xl font-bold text-foreground">$29</span>
                        <span className="text-muted-foreground">/month</span>
                    </div>

                    <ul className="space-y-3 sm:space-y-4 mb-8">
                        <Feature text="Unlimited Access to All Picks" />
                        <Feature text="Real-time Odds & Edge Analysis" />
                        <Feature text="Detailed Performance Tracking" />
                        <Feature text="Email Notifications" />
                        <Feature text="Cancel Anytime" />
                    </ul>

                    <CheckoutButton />
                </div>
            </div>

            <div className="max-w-md mx-auto mt-8 text-xs text-muted-foreground text-center leading-relaxed px-2">
                <p>
                    Subscription provides access to analytics and educational content only. We do
                    not accept wagers. No outcome is guaranteed; betting involves real financial
                    risk. You must be of legal age and follow the laws of your jurisdiction. See{' '}
                    <Link href="/responsible-gaming" className="text-primary hover:underline">
                        Responsible Gaming
                    </Link>{' '}
                    and{' '}
                    <Link href="/terms" className="text-primary hover:underline">
                        Terms
                    </Link>
                    .
                </p>
            </div>
        </Container>
    );
}

function CheckoutButton() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const router = useRouter();
    const { isSignedIn, isLoaded } = useAuth();
    const { user } = useUser();

    const isPremium = hasPremiumAccess(user?.publicMetadata);

    const handleCheckout = async () => {
        setError(null);
        if (!isLoaded) return;
        if (!isSignedIn) {
            router.push('/signup?redirect_url=/pricing');
            return;
        }

        try {
            setLoading(true);
            const response = await fetch('/api/checkout', { method: 'POST' });

            if (!response.ok) {
                throw new Error(`Checkout failed (${response.status})`);
            }

            const data = (await response.json()) as { url?: string };
            if (data.url) {
                window.location.href = data.url;
                return;
            }
            throw new Error('No checkout URL returned');
        } catch (err) {
            console.error('Checkout error:', err);
            setError("We couldn't start checkout. Please try again in a moment.");
        } finally {
            setLoading(false);
        }
    };

    if (isLoaded && isPremium) {
        return (
            <div className="block w-full text-center bg-success/10 text-success border border-success/20 py-3 rounded-lg font-bold">
                You&apos;re subscribed
            </div>
        );
    }

    return (
        <>
            <button
                onClick={handleCheckout}
                disabled={loading || !isLoaded}
                aria-busy={loading}
                className="block w-full bg-primary text-primary-foreground text-center py-3 rounded-lg font-bold hover:bg-amber-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
                {loading ? 'Loading...' : isSignedIn ? 'Subscribe Now' : 'Sign Up & Subscribe'}
            </button>
            {error && (
                <p
                    role="alert"
                    className="mt-3 text-sm text-danger flex items-center gap-2 justify-center"
                >
                    <AlertCircle className="h-4 w-4" aria-hidden="true" /> {error}
                </p>
            )}
        </>
    );
}

function Feature({ text }: { text: string }) {
    return (
        <li className="flex items-center gap-3">
            <div className="p-1 bg-success/10 rounded-full">
                <Check className="h-4 w-4 text-success" aria-hidden="true" />
            </div>
            <span className="text-foreground">{text}</span>
        </li>
    );
}
