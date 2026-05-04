'use client';

import { useState } from 'react';
import { AlertCircle } from 'lucide-react';

export function ManageSubscriptionButton() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const handleClick = async () => {
        setError(null);
        try {
            setLoading(true);
            const response = await fetch('/api/billing/portal', { method: 'POST' });
            if (!response.ok) {
                throw new Error(`Portal request failed (${response.status})`);
            }
            const data = (await response.json()) as { url?: string };
            if (data.url) {
                window.location.href = data.url;
                return;
            }
            throw new Error('No portal URL returned');
        } catch (err) {
            console.error('Portal error:', err);
            setError("We couldn't open the billing portal. Please try again in a moment.");
            setLoading(false);
        }
    };

    return (
        <div className="w-full">
            <button
                type="button"
                onClick={handleClick}
                disabled={loading}
                aria-busy={loading}
                className="w-full sm:w-auto inline-flex items-center justify-center bg-primary text-primary-foreground font-bold px-6 py-3 rounded-lg hover:bg-amber-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
                {loading ? 'Opening…' : 'Manage Subscription'}
            </button>
            {error && (
                <p
                    role="alert"
                    className="mt-3 text-sm text-danger flex items-center gap-2"
                >
                    <AlertCircle className="h-4 w-4" aria-hidden="true" /> {error}
                </p>
            )}
        </div>
    );
}
