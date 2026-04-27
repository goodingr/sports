import { useState, useEffect } from 'react';
import { fetchAPI, type StatsResponse } from '@/lib/api';

export type Stats = StatsResponse;

export function useStats() {
    const [stats, setStats] = useState<Stats | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;
        async function loadStats() {
            try {
                const data = await fetchAPI<Stats>('/api/bets/stats');
                if (!cancelled) setStats(data);
            } catch (err) {
                if (!cancelled) setError(err instanceof Error ? err.message : 'Failed to load stats');
            } finally {
                if (!cancelled) setLoading(false);
            }
        }

        loadStats();
        return () => {
            cancelled = true;
        };
    }, []);

    return { stats, loading, error };
}
