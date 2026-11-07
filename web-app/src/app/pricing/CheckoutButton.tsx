'use client';

import { AlertCircle } from 'lucide-react';
import Link from 'next/link';
import { useCallback, useEffect, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@clerk/nextjs';
import { readCheckoutAvailability } from '@/lib/api';

type Availability = 'checking' | 'open' | 'closed' | 'unavailable';
const AUTOMATIC_RECHECK_INTERVAL_MS = 1000;

export function CheckoutButton({ isPremium, interestAvailable }: { isPremium: boolean; interestAvailable: boolean }) {
    const [availability, setAvailability] = useState<Availability>('checking');
    const [checkingAvailability, setCheckingAvailability] = useState(false);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const availabilityRequest = useRef<AbortController | null>(null);
    const lastAvailabilityCheck = useRef(-Infinity);
    const checkoutInFlight = useRef(false);
    const router = useRouter();
    const { isSignedIn, isLoaded } = useAuth();

    const checkAvailability = useCallback(async (automatic = false) => {
        const now = performance.now();
        if (isPremium || availabilityRequest.current || checkoutInFlight.current) return;
        if (automatic && now - lastAvailabilityCheck.current < AUTOMATIC_RECHECK_INTERVAL_MS) return;

        const request = new AbortController();
        availabilityRequest.current = request;
        lastAvailabilityCheck.current = now;
        setCheckingAvailability(true);
        try {
            const open = await readCheckoutAvailability(request.signal);
            if (!request.signal.aborted) setAvailability(open ? 'open' : 'closed');
        } catch {
            if (!request.signal.aborted) setAvailability('unavailable');
        } finally {
            if (availabilityRequest.current === request) {
                availabilityRequest.current = null;
                setCheckingAvailability(false);
            }
        }
    }, [isPremium]);

    useEffect(() => {
        if (isPremium) return;
        const initialCheck = window.setTimeout(() => void checkAvailability(), 0);
        const recheckVisible = () => {
            if (document.visibilityState === 'visible') void checkAvailability(true);
        };
        window.addEventListener('focus', recheckVisible);
        document.addEventListener('visibilitychange', recheckVisible);
        return () => {
            window.clearTimeout(initialCheck);
            availabilityRequest.current?.abort();
            availabilityRequest.current = null;
            window.removeEventListener('focus', recheckVisible);
            document.removeEventListener('visibilitychange', recheckVisible);
        };
    }, [checkAvailability, isPremium]);

    const handleCheckout = async () => {
        if (!isLoaded || availability !== 'open' || availabilityRequest.current || checkoutInFlight.current) return;
        setError(null);
        if (!isSignedIn) {
            router.push('/signup?redirect_url=/pricing');
            return;
        }

        try {
            checkoutInFlight.current = true;
            setLoading(true);
            const response = await fetch('/api/checkout', { method: 'POST' });

            if (response.status === 503) {
                setAvailability('unavailable');
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
            checkoutInFlight.current = false;
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

    if (availability !== 'open') {
        return (
            <div className="rounded-lg border border-white/10 bg-white/5 p-3 text-center text-sm text-muted-foreground">
                <div role="status">
                    {availability === 'checking' && 'Checking subscription availability…'}
                    {availability === 'closed' && (
                        <>
                            <p className="font-semibold text-foreground">Subscriptions are currently paused</p>
                            <p className="mt-1">Enrollment opens after the picks pass validation and the service is ready.</p>
                            {interestAvailable && (
                                <div className="mt-3">
                                    <Link href="/interest" className="font-semibold text-primary hover:underline">Record price interest</Link>
                                    <p className="mt-1 text-xs">Free, with no payment obligation.</p>
                                </div>
                            )}
                            <Link href="/#model-history" className="mt-2 inline-block text-primary hover:underline">Explore model tracking</Link>
                        </>
                    )}
                    {availability === 'unavailable' && (
                        <p className="font-semibold text-foreground">We couldn’t check subscription availability.</p>
                    )}
                </div>
                {availability !== 'checking' && (
                    <button
                        type="button"
                        onClick={() => void checkAvailability()}
                        disabled={checkingAvailability}
                        className="mt-3 rounded-lg border border-white/10 px-3 py-2 font-medium text-foreground disabled:opacity-50"
                    >
                        {checkingAvailability ? 'Checking availability…' : 'Check availability again'}
                    </button>
                )}
            </div>
        );
    }

    return (
        <>
            <button
                onClick={handleCheckout}
                disabled={loading || checkingAvailability || !isLoaded}
                aria-busy={loading || checkingAvailability}
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
