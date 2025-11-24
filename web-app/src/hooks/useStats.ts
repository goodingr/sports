import { useState, useEffect } from 'react';
import { fetchAPI } from '@/lib/api';

interface Stats {
    roi: number;
    win_rate: number;
    total_profit: number;
    total_bets: number;
}

export function useStats() {
    const [stats, setStats] = useState<Stats | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        async function loadStats() {
            try {
                const data = await fetchAPI<Stats>('/api/bets/stats');
                setStats(data);
            } catch (err) {
                setError(err instanceof Error ? err.message : 'Failed to load stats');
            } finally {
                setLoading(false);
            }
        }

        loadStats();
    }, []);

    return { stats, loading, error };
}
