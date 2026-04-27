'use client';

import { useState, useEffect } from 'react';
import { fetchAPI, normalizeBet, type Bet, type PaginatedBetsResponse, type UpcomingBetsResponse } from '@/lib/api';
import { BetCard } from './BetCard';
import { Lock, AlertCircle } from 'lucide-react';
import Link from 'next/link';
import { useUser, useAuth } from '@clerk/nextjs';

function buildAuthHeaders(token: string | null): Record<string, string> {
    return token ? { Authorization: `Bearer ${token}` } : {};
}

function hasPremiumAccess(metadata: Record<string, unknown> | null | undefined): boolean {
    return metadata?.is_premium === true || metadata?.isPremium === true;
}

function groupByDate(bets: Bet[]): Array<[string, Bet[]]> {
    const groups: Record<string, Bet[]> = {};
    for (const bet of bets) {
        const date = new Date(bet.commence_time).toLocaleDateString('en-US', {
            weekday: 'long',
            month: 'long',
            day: 'numeric',
        });
        if (!groups[date]) groups[date] = [];
        groups[date].push(bet);
    }
    return Object.entries(groups);
}

export function BetFeed() {
    const [upcoming, setUpcoming] = useState<Bet[]>([]);
    const [history, setHistory] = useState<Bet[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [loadingMore, setLoadingMore] = useState(false);
    const [activeTab, setActiveTab] = useState<'upcoming' | 'history'>('upcoming');
    const [page, setPage] = useState(1);
    const [totalHistory, setTotalHistory] = useState(0);
    const { user, isLoaded } = useUser();
    const { getToken } = useAuth();

    const isPremium = hasPremiumAccess(user?.publicMetadata);

    useEffect(() => {
        let cancelled = false;
        async function loadBets() {
            try {
                setError(null);
                const token = await getToken();
                const headers = buildAuthHeaders(token);

                const [upcomingData, historyData] = await Promise.all([
                    fetchAPI<UpcomingBetsResponse>('/api/bets/upcoming', { headers }),
                    fetchAPI<PaginatedBetsResponse>('/api/bets/history?limit=30&page=1', { headers }),
                ]);

                if (cancelled) return;
                setUpcoming((upcomingData.data ?? []).map(normalizeBet));
                setHistory((historyData.data ?? []).map(normalizeBet));
                setTotalHistory(historyData.total ?? 0);
            } catch (err) {
                if (!cancelled) {
                    console.error('Failed to load bets:', err);
                    setError("We couldn't load picks right now. Please try again in a moment.");
                }
            } finally {
                if (!cancelled) setLoading(false);
            }
        }

        if (isLoaded) {
            loadBets();
        }
        return () => {
            cancelled = true;
        };
    }, [isLoaded, getToken]);

    const handleLoadMore = async () => {
        if (loadingMore) return;
        setLoadingMore(true);
        try {
            const token = await getToken();
            const headers = buildAuthHeaders(token);

            const nextPage = page + 1;
            const data = await fetchAPI<PaginatedBetsResponse>(
                `/api/bets/history?limit=30&page=${nextPage}`,
                { headers }
            );

            setHistory((prev) => [...prev, ...(data.data ?? []).map(normalizeBet)]);
            setPage(nextPage);
            if (data.total) setTotalHistory(data.total);
        } catch (err) {
            console.error('Failed to load more history:', err);
        } finally {
            setLoadingMore(false);
        }
    };

    if (loading || !isLoaded) {
        return (
            <div className="space-y-4" role="status" aria-live="polite" aria-busy="true">
                <div className="h-10 bg-white/5 rounded animate-pulse" />
                {[0, 1, 2, 3].map((i) => (
                    <div key={i} className="h-20 bg-white/5 rounded animate-pulse" />
                ))}
                <span className="sr-only">Loading bets...</span>
            </div>
        );
    }

    if (error) {
        return (
            <div
                role="alert"
                className="text-center py-12 px-6 bg-danger/10 border border-danger/20 rounded-xl"
            >
                <AlertCircle className="h-8 w-8 text-danger mx-auto mb-3" />
                <p className="text-foreground font-medium mb-2">Unable to load picks</p>
                <p className="text-sm text-muted-foreground mb-4">{error}</p>
                <button
                    onClick={() => window.location.reload()}
                    className="inline-flex items-center justify-center px-4 py-2 border border-white/10 rounded-full text-sm font-medium hover:bg-white/5 transition-colors"
                >
                    Retry
                </button>
            </div>
        );
    }

    const upcomingGroups = groupByDate(upcoming);
    const historyGroups = groupByDate(history);

    return (
        <div className="space-y-6">
            {/* Tabs */}
            <div className="flex border-b border-white/10" role="tablist">
                <button
                    role="tab"
                    aria-selected={activeTab === 'upcoming'}
                    onClick={() => setActiveTab('upcoming')}
                    className={`flex-1 pb-3 text-sm font-medium text-center border-b-2 transition-colors ${
                        activeTab === 'upcoming'
                            ? 'border-primary text-primary'
                            : 'border-transparent text-muted-foreground hover:text-foreground'
                    }`}
                >
                    Odds
                </button>
                <button
                    role="tab"
                    aria-selected={activeTab === 'history'}
                    onClick={() => setActiveTab('history')}
                    className={`flex-1 pb-3 text-sm font-medium text-center border-b-2 transition-colors ${
                        activeTab === 'history'
                            ? 'border-primary text-primary'
                            : 'border-transparent text-muted-foreground hover:text-foreground'
                    }`}
                >
                    Past
                </button>
            </div>

            {/* Content */}
            {activeTab === 'upcoming' ? (
                <section className="space-y-6">
                    <div className="flex items-center justify-between gap-2">
                        <h3 className="text-base sm:text-lg font-bold text-foreground flex items-center gap-2">
                            Upcoming Picks
                            {!isPremium && <Lock className="h-4 w-4 text-primary" aria-hidden="true" />}
                        </h3>
                        {!isPremium && (
                            <Link href="/pricing" className="text-sm text-primary hover:underline shrink-0">
                                Unlock All &rarr;
                            </Link>
                        )}
                    </div>

                    <div className="space-y-8">
                        {upcomingGroups.length > 0 ? (
                            upcomingGroups.map(([date, bets]) => (
                                <div key={date}>
                                    <h4 className="text-xs sm:text-sm font-bold text-muted-foreground uppercase tracking-wider mb-3 ml-1">
                                        {date}
                                    </h4>
                                    <div className="space-y-1">
                                        {bets.map((bet, i) => (
                                            <BetCard
                                                key={`${bet.game_id}-${bet.prediction}-${i}`}
                                                bet={bet}
                                                isPremium={isPremium}
                                            />
                                        ))}
                                    </div>
                                </div>
                            ))
                        ) : (
                            <div className="text-center py-12 px-4 text-muted-foreground bg-white/5 rounded-xl border border-white/10">
                                <p className="font-medium mb-1">No upcoming picks right now</p>
                                <p className="text-sm">Check back soon — new picks drop daily.</p>
                            </div>
                        )}
                    </div>
                </section>
            ) : (
                <section className="space-y-6">
                    <h3 className="text-base sm:text-lg font-bold text-foreground">Recent Results</h3>
                    {historyGroups.length > 0 ? (
                        <>
                            <div className="space-y-8">
                                {historyGroups.map(([date, bets]) => (
                                    <div key={date}>
                                        <h4 className="text-xs sm:text-sm font-bold text-muted-foreground uppercase tracking-wider mb-3 ml-1">
                                            {date}
                                        </h4>
                                        <div className="space-y-1">
                                            {bets.map((bet, i) => (
                                                <BetCard
                                                    key={`${bet.game_id}-${bet.prediction}-${i}`}
                                                    bet={bet}
                                                    isPremium
                                                />
                                            ))}
                                        </div>
                                    </div>
                                ))}
                            </div>

                            {history.length < totalHistory && (
                                <div className="mt-8 text-center">
                                    <button
                                        onClick={handleLoadMore}
                                        disabled={loadingMore}
                                        className="inline-flex items-center justify-center px-6 py-3 border border-white/10 rounded-full text-sm font-medium hover:bg-white/5 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                                    >
                                        {loadingMore ? 'Loading...' : 'View More'}
                                    </button>
                                </div>
                            )}
                        </>
                    ) : (
                        <div className="text-center py-12 px-4 text-muted-foreground bg-white/5 rounded-xl border border-white/10">
                            <p className="font-medium mb-1">No completed picks yet</p>
                            <p className="text-sm">Once games finish, results will appear here.</p>
                        </div>
                    )}
                </section>
            )}
        </div>
    );
}
