import { useState } from 'react';
import { Lock, CheckCircle, XCircle, MinusCircle, ExternalLink } from 'lucide-react';
import { MatchDetailsModal } from './MatchDetailsModal';

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

interface BetCardProps {
    bet: Bet;
    isPremium?: boolean;
}

export function BetCard({ bet, isPremium = false }: BetCardProps) {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const isCompleted = bet.status === 'Completed';
    const isLocked = !isCompleted && !isPremium;

    const time = new Date(bet.commence_time).toLocaleTimeString('en-US', {
        hour: 'numeric',
        minute: '2-digit',
    });

    return (
        <>
            <div
                className="bg-card border border-white/10 rounded-none overflow-hidden mb-1 hover:border-white/20 transition-colors group relative cursor-pointer"
                onClick={() => setIsModalOpen(true)}
            >
                {/* League (Absolute Top Left) */}
                <div className="absolute top-2 left-3 text-[10px] font-bold text-muted-foreground/50 uppercase tracking-wider">
                    {bet.league}
                </div>

                {/* Time (Absolute Top Center) */}
                <div className="absolute top-2 left-1/2 -translate-x-1/2 text-[10px] font-mono text-muted-foreground/50 uppercase tracking-wider">
                    {time}
                </div>

                <div className="p-4 pt-6 flex items-center justify-between gap-2 md:gap-4">

                    {/* Left: Away Team */}
                    <div className="flex-1 flex items-center justify-end text-right gap-2 md:gap-3">
                        <div className="font-bold text-foreground text-sm md:text-base leading-tight">
                            {bet.away_team}
                        </div>
                        {isCompleted && (
                            <div className="text-xl md:text-2xl font-black font-mono text-foreground">
                                {bet.away_score}
                            </div>
                        )}
                    </div>

                    {/* Center: Bet Details */}
                    <div className="flex flex-col items-center justify-center min-w-[80px] md:min-w-[100px] z-10 shrink-0 pt-1">
                        {isLocked ? (
                            <div className="flex flex-col items-center gap-1 text-muted-foreground">
                                <Lock className="h-4 w-4" />
                                <span className="text-[10px] uppercase tracking-wider font-medium">Premium</span>
                            </div>
                        ) : !isCompleted && bet.book_url ? (
                            // Clickable button for upcoming bets with sportsbook link
                            <a
                                href={bet.book_url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex flex-col items-center gap-1 hover:bg-primary/5 px-3 py-2 rounded transition-colors cursor-pointer"
                                onClick={(e) => e.stopPropagation()}
                            >
                                <div className="text-sm font-black text-primary uppercase tracking-tight text-center leading-tight">
                                    {bet.prediction}
                                </div>
                                <div className="text-xs font-medium text-muted-foreground">
                                    {bet.odds > 0 ? `+${bet.odds}` : str(bet.odds)}
                                </div>
                                {bet.book && (
                                    <div className="text-[10px] text-primary/70 flex items-center gap-0.5 transition-colors">
                                        {bet.book}
                                        <ExternalLink className="h-2.5 w-2.5" />
                                    </div>
                                )}
                            </a>
                        ) : (
                            // Non-clickable display for completed bets or bets without sportsbook
                            <div className="flex flex-col items-center gap-1">
                                {/* 1. Prediction (Line) */}
                                <div className="text-sm font-black text-primary uppercase tracking-tight text-center leading-tight">
                                    {bet.prediction}
                                </div>

                                {/* 2. Final Score (Badge) */}
                                {isCompleted && (
                                    <div>
                                        <Badge
                                            status={bet.status}
                                            result={bet.result}
                                            compact
                                            actualTotal={(bet.home_score || 0) + (bet.away_score || 0)}
                                        />
                                    </div>
                                )}

                                {/* 3. Profit/Odds */}
                                <div className={`text-xs font-medium ${isCompleted
                                    ? (bet.profit && bet.profit > 0 ? 'text-success' : 'text-danger')
                                    : 'text-muted-foreground'
                                    }`}>
                                    {isCompleted ? formatMoney(bet.profit || 0) : (bet.odds > 0 ? `+${bet.odds}` : str(bet.odds))}
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Right: Home Team */}
                    <div className="flex-1 flex items-center justify-start text-left gap-2 md:gap-3">
                        {isCompleted && (
                            <div className="text-xl md:text-2xl font-black font-mono text-foreground">
                                {bet.home_score}
                            </div>
                        )}
                        <div className="font-bold text-foreground text-sm md:text-base leading-tight">
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
            />
        </>
    );
}

function Badge({ status, result, compact, actualTotal }: { status: string, result?: string, compact?: boolean, actualTotal?: number }) {
    const baseClasses = "inline-flex items-center gap-1 rounded-full font-medium";
    const sizeClasses = compact ? "px-1.5 py-0.5 text-[10px]" : "px-2 py-1 text-xs";

    if (status === 'Pending') {
        return (
            <span className={`${baseClasses} ${sizeClasses} bg-blue-500/10 text-blue-500`}>
                Upcoming
            </span>
        );
    }

    if (result === 'Win') {
        return (
            <span className={`${baseClasses} ${sizeClasses} bg-success/10 text-success`}>
                <CheckCircle className={compact ? "h-2.5 w-2.5" : "h-3 w-3"} /> {compact ? "Win" : "Win"}
            </span>
        );
    }

    if (result === 'Loss') {
        return (
            <span className={`${baseClasses} ${sizeClasses} bg-danger/10 text-danger`}>
                <XCircle className={compact ? "h-2.5 w-2.5" : "h-3 w-3"} /> {compact ? "Loss" : "Loss"}
            </span>
        );
    }

    // If completed, show actual total instead of "Completed" text (for over/under bets)
    if (status === 'Completed' && actualTotal !== undefined && actualTotal > 0) {
        return (
            <span className="text-xs font-medium text-muted-foreground">
                {actualTotal}
            </span>
        );
    }

    // For completed games without a result (scores are shown elsewhere), don't show badge
    if (status === 'Completed' && !result) {
        return null;
    }

    return (
        <span className={`${baseClasses} ${sizeClasses} bg-muted text-muted-foreground`}>
            <MinusCircle className={compact ? "h-2.5 w-2.5" : "h-3 w-3"} /> {result || status}
        </span>
    );
}

function formatMoney(amount: number) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        signDisplay: 'always',
    }).format(amount);
}

function str(val: number) {
    return val.toString();
}
