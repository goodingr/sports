'use client';

import { useState, useEffect, useRef } from 'react';
import { fetchAPI, normalizeBet, normalizeUpcomingBets, type Bet, type PaginatedBetsResponse, type UpcomingBetsResponse } from '@/lib/api';
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
    const [partialError, setPartialError] = useState<string | null>(null);
    const [loadingMore, setLoadingMore] = useState(false);
    const [loadMoreError, setLoadMoreError] = useState<string | null>(null);
    const [activeTab, setActiveTab] = useState<'upcoming' | 'history'>('upcoming');
    const [page, setPage] = useState(1);
    const [totalHistory, setTotalHistory] = useState(0);
    const [reloadKey, setReloadKey] = useState(0);
    const { user, isLoaded } = useUser();
    const { getToken } = useAuth();
    const [apiPremium, setApiPremium] = useState<boolean | null>(null);
    const loadMoreController = useRef<AbortController | null>(null);

    const hasPremium = hasPremiumAccess(user?.publicMetadata);
    const isPremium = apiPremium ?? hasPremium;

    useEffect(() => {
        let cancelled = false;
        let upcomingInFlight = false;
        const lifecycle = new AbortController();

        function acceptUpcoming(data: UpcomingBetsResponse) {
            setUpcoming(normalizeUpcomingBets(data.data ?? []));
            setApiPremium(data.is_premium ?? null);
        }

        async function refreshUpcoming() {
            if (cancelled || upcomingInFlight || document.visibilityState !== 'visible') return;
            upcomingInFlight = true;
            try {
                const token = await getToken();
                if (cancelled) return;
                const data = await fetchAPI<UpcomingBetsResponse>('/api/bets/upcoming', {
                    headers: buildAuthHeaders(token),
                    cache: 'no-store',
                    signal: AbortSignal.any([lifecycle.signal, AbortSignal.timeout(10_000)]),
                });
                if (!cancelled) {
                    acceptUpcoming(data);
                    setError(null);
                    setPartialError((previous) => previous?.startsWith('Upcoming picks') ? null : previous);
                }
            } catch {
                if (!cancelled) {
                    setUpcoming([]);
                    setApiPremium(null);
                    setPartialError('Upcoming picks are temporarily unavailable. Model results are available under Past.');
                }
            } finally {
                upcomingInFlight = false;
            }
        }

        function refreshVisible() {
            if (document.visibilityState !== 'visible') return;
            setUpcoming((current) => current.filter((bet) => Date.parse(bet.quote_expires_at ?? '') > Date.now()));
            void refreshUpcoming();
        }

        async function loadBets() {
            upcomingInFlight = true;
            setError(null);
            setPartialError(null);
            setPage(1);
            setLoadMoreError(null);
            setLoadingMore(false);
            setLoading(true);
            try {
                const token = await getToken();
                if (cancelled) return;
                const headers = buildAuthHeaders(token);
                const signal = AbortSignal.any([lifecycle.signal, AbortSignal.timeout(10_000)]);

                const [upcomingResult, historyResult] = await Promise.allSettled([
                    fetchAPI<UpcomingBetsResponse>('/api/bets/upcoming', { headers, cache: 'no-store', signal }),
                    fetchAPI<PaginatedBetsResponse>('/api/bets/history?limit=30&page=1', {
                        headers,
                        cache: 'no-store',
                        signal,
                    }),
                ]);

                if (cancelled) return;

                if (upcomingResult.status === 'fulfilled') {
                    acceptUpcoming(upcomingResult.value);
                } else {
                    setUpcoming([]);
                    setApiPremium(null);
                }
                if (historyResult.status === 'fulfilled') {
                    setHistory((historyResult.value.data ?? []).map(normalizeBet));
                    setTotalHistory(historyResult.value.total ?? 0);
                } else {
                    setHistory([]);
                    setTotalHistory(0);
                }

                if (
                    upcomingResult.status === 'rejected' &&
                    historyResult.status === 'rejected'
                ) {
                    console.error('Failed to load bets:', upcomingResult.reason);
                    setError("We couldn't load picks right now. Please try again in a moment.");
                } else if (upcomingResult.status === 'rejected') {
                    console.error('Failed to load upcoming bets:', upcomingResult.reason);
                    setPartialError(
                        "Upcoming picks are temporarily unavailable. Model results are available under Past.",
                    );
                } else if (historyResult.status === 'rejected') {
                    console.error('Failed to load history:', historyResult.reason);
                    setPartialError(
                        "Model results are temporarily unavailable. Upcoming picks are available under Odds.",
                    );
                }
            } catch (err) {
                if (!cancelled) {
                    console.error('Failed to load bets:', err);
                    setError("We couldn't load picks right now. Please try again in a moment.");
                }
            } finally {
                upcomingInFlight = false;
                if (!cancelled) setLoading(false);
            }
        }

        if (!isLoaded) return;
        void loadBets();
        const refreshInterval = window.setInterval(refreshUpcoming, 60_000);
        window.addEventListener('focus', refreshVisible);
        document.addEventListener('visibilitychange', refreshVisible);
        return () => {
            cancelled = true;
            lifecycle.abort();
            loadMoreController.current?.abort();
            window.clearInterval(refreshInterval);
            window.removeEventListener('focus', refreshVisible);
            document.removeEventListener('visibilitychange', refreshVisible);
        };
    }, [isLoaded, getToken, reloadKey, user?.id, hasPremium]);

    useEffect(() => {
        if (upcoming.length === 0) return;
        const nextExpiry = Math.min(...upcoming.map((bet) => Date.parse(bet.quote_expires_at ?? '')));
        const expiryTimer = window.setTimeout(() => {
            setUpcoming((current) => current.filter((bet) => Date.parse(bet.quote_expires_at ?? '') > Date.now()));
        }, Math.max(0, nextExpiry - Date.now()));
        return () => window.clearTimeout(expiryTimer);
    }, [upcoming]);

    const handleLoadMore = async () => {
        if (loadingMore) return;
        setLoadingMore(true);
        setLoadMoreError(null);
        const controller = new AbortController();
        loadMoreController.current = controller;
        try {
            const token = await getToken();
            if (controller.signal.aborted) return;
            const headers = buildAuthHeaders(token);

            const nextPage = page + 1;
            const data = await fetchAPI<PaginatedBetsResponse>(
                `/api/bets/history?limit=30&page=${nextPage}`,
                { headers, cache: 'no-store', signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10_000)]) }
            );

            if (controller.signal.aborted) return;
            setHistory((prev) => [...prev, ...(data.data ?? []).map(normalizeBet)]);
            setPage(nextPage);
            if (data.total) setTotalHistory(data.total);
        } catch (err) {
            if (controller.signal.aborted) return;
            console.error('Failed to load more history:', err);
            setLoadMoreError("We couldn't load more results. Please try again.");
        } finally {
            if (!controller.signal.aborted) setLoadingMore(false);
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
                <AlertCircle className="h-8 w-8 text-danger mx-auto mb-3" aria-hidden="true" />
                <p className="text-foreground font-medium mb-2">Unable to load picks</p>
                <p className="text-sm text-muted-foreground mb-4">{error}</p>
                <button
                    type="button"
                    onClick={() => setReloadKey((k) => k + 1)}
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
            {partialError && (
                <div
                    role="status"
                    className="flex items-start gap-3 rounded-xl border border-amber-500/30 bg-amber-500/10 p-4 text-sm text-amber-200"
                >
                    <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" aria-hidden="true" />
                    <span>{partialError}</span>
                </div>
            )}
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
                            {!isPremium && upcoming.length > 0 && <Lock className="h-4 w-4 text-primary" aria-hidden="true" />}
                        </h3>
                        {!isPremium && upcoming.length > 0 && (
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
                                <p className="text-sm">Picks appear here after they pass our validation checks.</p>
                            </div>
                        )}
                    </div>
                </section>
            ) : (
                <section className="space-y-6">
                    <h3 className="text-base sm:text-lg font-bold text-foreground">Model Results</h3>
                    <p className="text-sm text-muted-foreground">Tracked totals model results at simulated stakes.</p>
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
                                                    isPremium={isPremium}
                                                />
                                            ))}
                                        </div>
                                    </div>
                                ))}
                            </div>

                            {history.length < totalHistory && (
                                <div className="mt-8 text-center">
                                    <button
                                        type="button"
                                        onClick={handleLoadMore}
                                        disabled={loadingMore}
                                        aria-busy={loadingMore}
                                        className="inline-flex items-center justify-center px-6 py-3 border border-white/10 rounded-full text-sm font-medium hover:bg-white/5 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                                    >
                                        {loadingMore ? 'Loading...' : 'View More'}
                                    </button>
                                    {loadMoreError && (
                                        <p
                                            role="alert"
                                            className="mt-3 text-sm text-danger flex items-center justify-center gap-2"
                                        >
                                            <AlertCircle className="h-4 w-4" aria-hidden="true" />
                                            {loadMoreError}
                                        </p>
                                    )}
                                </div>
                            )}
                        </>
                    ) : (
                        <div className="text-center py-12 px-4 text-muted-foreground bg-white/5 rounded-xl border border-white/10">
                            <p className="font-medium mb-1">No model results yet</p>
                            <p className="text-sm">Settled model selections will appear here.</p>
                        </div>
                    )}
                </section>
            )}
        </div>
    );
}
