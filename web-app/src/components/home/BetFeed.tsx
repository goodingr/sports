'use client';

import { useState, useEffect } from 'react';
import { fetchAPI } from '@/lib/api';
import { BetCard } from './BetCard';
import { Lock } from 'lucide-react';
import Link from 'next/link';
import { useUser } from '@clerk/nextjs';

interface Bet {
    game_id: string;
    commence_time: string;
    home_team: string;
    away_team: string;
    bet_type: string;
    prediction: string;
    odds: number;
    edge: number;
    stake: number;
    status: string;
    result?: string;
    profit?: number;
    home_score?: number;
    away_score?: number;
    league?: string;
    book?: string;
    book_url?: string;
}

export function BetFeed() {
    const [upcoming, setUpcoming] = useState<Bet[]>([]);
    const [history, setHistory] = useState<Bet[]>([]);
    const [loading, setLoading] = useState(true);
    const [loadingMore, setLoadingMore] = useState(false);
    const [activeTab, setActiveTab] = useState<'upcoming' | 'history'>('upcoming');
    const [page, setPage] = useState(1);
    const [totalHistory, setTotalHistory] = useState(0);
    const { user, isLoaded } = useUser();

    const isPremium = !!user?.publicMetadata?.isPremium;

    // Map API response to Bet interface
    const mapBet = (bet: any): Bet => ({
        ...bet,
        prediction: bet.description || bet.prediction,
        odds: Number(bet.moneyline || bet.odds || 0),
        bet_type: bet.side || bet.bet_type,
        stake: Number(bet.stake || 0),
        profit: Number(bet.profit || 0),
        edge: Number(bet.edge || 0),
    });

    useEffect(() => {
        async function loadBets() {
            try {
                const [upcomingData, historyData] = await Promise.all([
                    fetchAPI<{ data: any[] }>('/api/bets/upcoming'),
                    fetchAPI<{ data: any[], total: number }>('/api/bets/history?limit=30&page=1')
                ]);

                setUpcoming(upcomingData.data.map(mapBet));
                setHistory(historyData.data.map(mapBet));
                setTotalHistory(historyData.total || 0);
            } catch (err) {
                console.error('Failed to load bets:', err);
            } finally {
                setLoading(false);
            }
        }

        if (isLoaded) {
            loadBets();
        }
    }, [isLoaded]);
    const handleLoadMore = async () => {
        if (loadingMore) return;
        setLoadingMore(true);
        try {
            const nextPage = page + 1;
            const data = await fetchAPI<{ data: any[], total: number }>(`/api/bets/history?limit=30&page=${nextPage}`);

            setHistory(prev => [...prev, ...data.data.map(mapBet)]);
            setPage(nextPage);
            // Update total just in case it changed
            if (data.total) setTotalHistory(data.total);
        } catch (err) {
            console.error('Failed to load more history:', err);
        } finally {
            setLoadingMore(false);
        }
    };

    if (loading || !isLoaded) {
        return <div className="text-center py-20 text-muted-foreground animate-pulse">Loading bets...</div>;
    }

    return (
        <div className="space-y-6">
            {/* Tabs */}
            <div className="flex border-b border-white/10">
                <button
                    onClick={() => setActiveTab('upcoming')}
                    className={`flex-1 pb-3 text-sm font-medium text-center border-b-2 transition-colors ${activeTab === 'upcoming'
                        ? 'border-primary text-primary'
                        : 'border-transparent text-muted-foreground hover:text-foreground'
                        }`}
                >
                    Odds
                </button>
                <button
                    onClick={() => setActiveTab('history')}
                    className={`flex-1 pb-3 text-sm font-medium text-center border-b-2 transition-colors ${activeTab === 'history'
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
                    <div className="flex items-center justify-between">
                        <h3 className="text-lg font-bold text-foreground flex items-center gap-2">
                            Upcoming Picks
                            {!isPremium && <Lock className="h-4 w-4 text-primary" />}
                        </h3>
                        {!isPremium && (
                            <Link href="/pricing" className="text-sm text-primary hover:underline">
                                Unlock All &rarr;
                            </Link>
                        )}
                    </div>

                    <div className="space-y-8">
                        {upcoming.length > 0 ? (
                            Object.entries(
                                upcoming.reduce((groups, bet) => {
                                    const date = new Date(bet.commence_time).toLocaleDateString('en-US', {
                                        weekday: 'long',
                                        month: 'long',
                                        day: 'numeric'
                                    });
                                    if (!groups[date]) groups[date] = [];
                                    groups[date].push(bet);
                                    return groups;
                                }, {} as Record<string, Bet[]>)
                            ).map(([date, bets]) => (
                                <div key={date}>
                                    <h4 className="text-sm font-bold text-muted-foreground uppercase tracking-wider mb-3 ml-1">
                                        {date}
                                    </h4>
                                    <div className="space-y-1">
                                        {bets.map((bet, i) => (
                                            <BetCard key={`${bet.game_id}-${bet.prediction}-${i}`} bet={bet} isPremium={isPremium} />
                                        ))}
                                    </div>
                                </div>
                            ))
                        ) : (
                            <div className="text-center py-12 text-muted-foreground bg-white/5 rounded-xl border border-white/10">
                                No upcoming picks at the moment.
                            </div>
                        )}
                    </div>
                </section>
            ) : (
                <section className="space-y-6">
                    <h3 className="text-lg font-bold text-foreground">Recent Results</h3>
                    <div className="space-y-8">
                        {Object.entries(
                            history.reduce((groups, bet) => {
                                const date = new Date(bet.commence_time).toLocaleDateString('en-US', {
                                    weekday: 'long',
                                    month: 'long',
                                    day: 'numeric'
                                });
                                if (!groups[date]) groups[date] = [];
                                groups[date].push(bet);
                                return groups;
                            }, {} as Record<string, Bet[]>)
                        ).map(([date, bets]) => (
                            <div key={date}>
                                <h4 className="text-sm font-bold text-muted-foreground uppercase tracking-wider mb-3 ml-1">
                                    {date}
                                </h4>
                                <div className="space-y-1">
                                    {bets.map((bet, i) => (
                                        <BetCard key={`${bet.game_id}-${bet.prediction}-${i}`} bet={bet} isPremium={true} />
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
                </section>
            )}
        </div>
    );
}
