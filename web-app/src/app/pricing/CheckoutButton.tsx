'use client';

import { AlertCircle } from 'lucide-react';
import Link from 'next/link';
import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@clerk/nextjs';
import { isCheckoutAvailable } from '@/lib/api';

export function CheckoutButton({ isPremium }: { isPremium: boolean }) {
    const [available, setAvailable] = useState<boolean | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const router = useRouter();
    const { isSignedIn, isLoaded } = useAuth();

    useEffect(() => {
        let active = true;
        isCheckoutAvailable().then((open) => {
            if (active) setAvailable(open);
        });
        return () => { active = false; };
    }, []);

    const handleCheckout = async () => {
        setError(null);
        if (!isLoaded || available !== true) return;
        if (!isSignedIn) {
            router.push('/signup?redirect_url=/pricing');
            return;
        }

        try {
            setLoading(true);
            const response = await fetch('/api/checkout', { method: 'POST' });

            if (response.status === 503) {
                setAvailable(false);
                return;
            }
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

    if (available !== true) {
        return (
            <div role="status" className="rounded-lg border border-white/10 bg-white/5 p-3 text-center text-sm text-muted-foreground">
                {available === null ? (
                    'Checking subscription availability…'
                ) : (
                    <>
                        <p className="font-semibold text-foreground">Subscriptions are currently paused</p>
                        <p className="mt-1">Enrollment opens after the picks pass validation and the service is ready.</p>
                        <Link href="/#model-history" className="mt-2 inline-block text-primary hover:underline">Explore model tracking</Link>
                    </>
                )}
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
