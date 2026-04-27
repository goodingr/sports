'use client';

import { useState } from 'react';
import { Lock, ExternalLink } from 'lucide-react';
import { MatchDetailsModal } from './MatchDetailsModal';
import type { Bet } from '@/lib/api';

interface BetCardProps {
    bet: Bet;
    isPremium?: boolean;
}

function formatOdds(odds: number): string {
    if (!Number.isFinite(odds) || odds === 0) return '–';
    return odds > 0 ? `+${odds}` : String(odds);
}

function formatMoney(amount: number): string {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        signDisplay: 'always',
    }).format(amount);
}

export function BetCard({ bet, isPremium = false }: BetCardProps) {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const isCompleted = bet.status === 'Completed';

    const now = new Date();
    const commenceDate = new Date(bet.commence_time);
    const isStarted = commenceDate <= now;

    // Locked when game hasn't completed yet AND user isn't premium.
    const isLocked = !isCompleted && !isPremium;

    const time = commenceDate.toLocaleTimeString('en-US', {
        hour: 'numeric',
        minute: '2-digit',
    });

    const oddsLabel = formatOdds(bet.odds);
    const ariaLabel = isLocked
        ? `${bet.away_team} at ${bet.home_team}, locked. Subscribe to unlock pick.`
        : `${bet.away_team} at ${bet.home_team}, ${bet.prediction || 'pick'}, odds ${oddsLabel}`;

    return (
        <>
            <div
                role="button"
                tabIndex={0}
                aria-label={ariaLabel}
                className="bg-card border border-white/10 rounded-none overflow-hidden mb-1 hover:border-white/20 transition-colors group relative cursor-pointer focus:outline-none focus:ring-2 focus:ring-primary/50"
                onClick={() => setIsModalOpen(true)}
                onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        setIsModalOpen(true);
                    }
                }}
            >
                {/* League (Absolute Top Left) */}
                <div className="absolute top-2 left-3 text-[10px] font-bold text-muted-foreground/50 uppercase tracking-wider">
                    {bet.league}
                </div>

                {/* Time (Absolute Top Center) */}
                <div className="absolute top-2 left-1/2 -translate-x-1/2 text-[10px] font-mono text-muted-foreground/50 uppercase tracking-wider">
                    {isStarted && !isCompleted ? (
                        <span className="text-red-500 font-bold animate-pulse">LIVE</span>
                    ) : (
                        time
                    )}
                </div>

                <div className="p-3 sm:p-4 pt-6 flex items-center justify-between gap-2 md:gap-4">
                    {/* Left: Away Team */}
                    <div className="flex-1 min-w-0 flex items-center justify-end text-right gap-2 md:gap-3">
                        <div className="font-bold text-foreground text-xs sm:text-sm md:text-base leading-tight truncate">
                            {bet.away_team}
                        </div>
                        {(isCompleted || isStarted) && (
                            <div className="text-xl sm:text-2xl md:text-3xl font-black font-mono text-foreground tracking-tighter shrink-0">
                                {bet.away_score ?? 0}
                            </div>
                        )}
                    </div>

                    {/* Center: Bet Details */}
                    <div className="flex flex-col items-center justify-center min-w-[72px] sm:min-w-[80px] md:min-w-[100px] z-10 shrink-0 pt-1">
                        {isLocked ? (
                            <a
                                href="/pricing"
                                onClick={(e) => e.stopPropagation()}
                                aria-label="Subscribe to unlock this pick"
                                className="flex flex-col items-center gap-1 text-primary hover:text-primary/80 transition-colors z-20"
                            >
                                <div className="bg-primary/10 p-1.5 rounded-full ring-1 ring-primary/20">
                                    <Lock className="h-3 w-3" aria-hidden="true" />
                                </div>
                                <span className="text-[10px] uppercase tracking-wider font-black">Unlock</span>
                                <span className="text-[10px] text-muted-foreground">Premium pick</span>
                            </a>
                        ) : !isCompleted && bet.book_url ? (
                            <a
                                href={bet.book_url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex flex-col items-center gap-1 transition-colors cursor-pointer group/link"
                                onClick={(e) => e.stopPropagation()}
                            >
                                <div className="text-xs sm:text-sm font-black text-primary uppercase tracking-tight text-center leading-tight">
                                    {bet.prediction}
                                </div>
                                <div className="text-xs font-medium text-muted-foreground">{oddsLabel}</div>
                                {bet.book && (
                                    <div className="text-[10px] text-muted-foreground group-hover/link:text-primary flex items-center gap-0.5 transition-colors">
                                        {bet.book}
                                        <ExternalLink className="h-2.5 w-2.5" aria-hidden="true" />
                                    </div>
                                )}
                            </a>
                        ) : (
                            <div className="flex flex-col items-center gap-1">
                                <div className="text-xs sm:text-sm font-black text-primary uppercase tracking-tight text-center leading-tight">
                                    {bet.prediction}
                                </div>

                                {(isCompleted || isStarted) && (
                                    <div className="text-[10px] font-black text-foreground whitespace-nowrap bg-muted/30 px-1.5 py-0.5 rounded">
                                        {isCompleted ? 'FINAL: ' : 'SCORE: '}
                                        {(bet.home_score ?? 0) + (bet.away_score ?? 0)}
                                    </div>
                                )}

                                <div
                                    className={`text-xs font-medium ${
                                        isCompleted
                                            ? bet.profit !== undefined && bet.profit > 0
                                                ? 'text-success'
                                                : 'text-danger'
                                            : 'text-muted-foreground'
                                    }`}
                                >
                                    {isCompleted ? formatMoney(bet.profit ?? 0) : oddsLabel}
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Right: Home Team */}
                    <div className="flex-1 min-w-0 flex items-center justify-start text-left gap-2 md:gap-3">
                        {(isCompleted || isStarted) && (
                            <div className="text-xl sm:text-2xl md:text-3xl font-black font-mono text-foreground tracking-tighter shrink-0">
                                {bet.home_score ?? 0}
                            </div>
                        )}
                        <div className="font-bold text-foreground text-xs sm:text-sm md:text-base leading-tight truncate">
                            {bet.home_team}
                        </div>
                    </div>

                    {/* Background VS divider effect */}
                    <div className="absolute inset-0 flex items-center justify-center pointer-events-none opacity-5">
                        <div className="w-px h-full bg-white/20"></div>
                    </div>
                </div>
            </div>

            <MatchDetailsModal
                isOpen={isModalOpen}
                onClose={() => setIsModalOpen(false)}
                gameId={bet.game_id}
                homeTeam={bet.home_team}
                awayTeam={bet.away_team}
                commenceTime={bet.commence_time}
                isLocked={isLocked}
                predictionInfo={{
                    predicted_total_points: bet.predicted_total_points,
                    recommended_bet: bet.recommended_bet,
                    edge: bet.edge,
                    home_score: bet.home_score,
                    away_score: bet.away_score,
                    profit: bet.profit,
                    won: bet.result === 'Win',
                    status: bet.status,
                    book: bet.book,
                    book_url: bet.book_url,
                }}
                oddsData={bet.odds_data}
            />
        </>
    );
}
