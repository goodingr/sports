'use client';

import { useEffect } from 'react';
import { X, ExternalLink, Lock } from 'lucide-react';
import type { OddsRecord } from '@/lib/api';

interface BookOdds {
    name: string;
    url?: string;
    over: OddsRecord | null;
    under: OddsRecord | null;
}

interface MatchDetailsModalProps {
    isOpen: boolean;
    onClose: () => void;
    /** kept for future detail fetches; reserved */
    gameId?: string;
    homeTeam: string;
    awayTeam: string;
    commenceTime?: string;
    /** When true, the body of the modal is locked behind the paywall. */
    isLocked?: boolean;
    predictionInfo?: {
        predicted_total_points?: number | null;
        recommended_bet?: string | null;
        edge?: number | null;
        home_score?: number | null;
        away_score?: number | null;
        profit?: number | null;
        won?: boolean | null;
        status?: string;
        book?: string;
        book_url?: string;
    };
    oddsData?: OddsRecord[];
}

function groupOddsByBook(odds: OddsRecord[]): BookOdds[] {
    const acc: BookOdds[] = [];
    for (const curr of odds) {
        const existingBook = acc.find((b) => b.name === curr.book);
        if (existingBook) {
            if (curr.outcome === 'Over') existingBook.over = curr;
            if (curr.outcome === 'Under') existingBook.under = curr;
        } else {
            acc.push({
                name: curr.book,
                url: curr.book_url,
                over: curr.outcome === 'Over' ? curr : null,
                under: curr.outcome === 'Under' ? curr : null,
            });
        }
    }
    return acc.filter((book) => book.over || book.under);
}

export function MatchDetailsModal({
    isOpen,
    onClose,
    homeTeam,
    awayTeam,
    commenceTime,
    isLocked = false,
    predictionInfo,
    oddsData,
}: MatchDetailsModalProps) {
    // Close on Escape key
    useEffect(() => {
        if (!isOpen) return;
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => {
            window.removeEventListener('keydown', handleKeyDown);
        };
    }, [isOpen, onClose]);

    // Lock body scroll while modal is open
    useEffect(() => {
        if (!isOpen) return;
        const original = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        return () => {
            document.body.style.overflow = original;
        };
    }, [isOpen]);

    if (!isOpen) return null;

    const formattedDate = commenceTime
        ? new Date(commenceTime).toLocaleDateString('en-US', {
              weekday: 'short',
              month: 'short',
              day: 'numeric',
              hour: 'numeric',
              minute: 'numeric',
          })
        : '';

    const totalsByBook = groupOddsByBook(oddsData ?? []);

    // Treat a server-masked recommendation as locked too.
    const lockedFromServer = predictionInfo?.recommended_bet === 'Premium Only';
    const showLock = isLocked || lockedFromServer;

    return (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center p-3 sm:p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200"
            onClick={onClose}
            role="dialog"
            aria-modal="true"
            aria-label={`${awayTeam} at ${homeTeam} details`}
        >
            <div
                className="bg-card w-full max-w-md max-h-[90vh] rounded-xl shadow-2xl border border-white/10 flex flex-col animate-in zoom-in-95 duration-200 overflow-hidden relative"
                onClick={(e) => e.stopPropagation()}
            >
                {/* Header */}
                <div className="p-4 border-b border-white/10 flex items-start justify-between gap-3 bg-white/5 z-20 relative">
                    <div className="min-w-0">
                        {formattedDate && (
                            <div className="text-[10px] uppercase font-bold text-muted-foreground tracking-wider mb-1">
                                {formattedDate}
                            </div>
                        )}
                        <h2 className="text-base sm:text-lg font-bold text-foreground leading-tight">
                            <span className="block sm:inline">{awayTeam}</span>{' '}
                            <span className="text-muted-foreground text-sm font-normal">@</span>{' '}
                            <span className="block sm:inline">{homeTeam}</span>
                        </h2>
                    </div>
                    <button
                        onClick={onClose}
                        aria-label="Close"
                        className="p-2 hover:bg-white/10 rounded-full transition-colors text-muted-foreground hover:text-foreground shrink-0"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>

                {/* Content */}
                <div className="flex-1 overflow-y-auto p-4 space-y-6 relative">
                    {/* Premium Lock Overlay */}
                    {showLock && (
                        <div className="absolute inset-0 z-10 flex flex-col items-center justify-center bg-background/80 backdrop-blur-md p-6 text-center">
                            <div className="bg-primary/10 p-4 rounded-full mb-4 ring-1 ring-primary/20">
                                <Lock className="h-8 w-8 text-primary" />
                            </div>
                            <h3 className="text-xl font-bold text-foreground mb-2">Premium Only</h3>
                            <p className="text-sm text-muted-foreground mb-6 max-w-[260px]">
                                Subscribe to view our AI predictions, edges, and best live odds for this game.
                            </p>
                            <a
                                href="/pricing"
                                className="bg-primary hover:bg-primary/90 text-primary-foreground font-bold px-8 py-3 rounded-lg transition-all transform hover:scale-105 shadow-lg shadow-primary/25"
                            >
                                Subscribe to Unlock
                            </a>
                        </div>
                    )}

                    {/* Prediction Section */}
                    {predictionInfo && (predictionInfo.recommended_bet || predictionInfo.predicted_total_points || showLock) && (
                        <div
                            className={`grid grid-cols-2 gap-3 sm:gap-4 mb-6 ${
                                showLock ? 'opacity-20 pointer-events-none filter blur-sm' : ''
                            }`}
                        >
                            {/* Result Section (if completed) */}
                            {predictionInfo.status === 'Completed' && (
                                <>
                                    <div className="bg-white/5 border border-white/10 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                        <div className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-1">
                                            Final Total
                                        </div>
                                        <div className="text-lg font-black text-foreground">
                                            {(predictionInfo.away_score ?? 0) + (predictionInfo.home_score ?? 0)}
                                        </div>
                                        <div className="text-xs text-muted-foreground mt-1">
                                            {predictionInfo.away_score} - {predictionInfo.home_score}
                                        </div>
                                    </div>
                                    <div
                                        className={`border rounded-lg p-4 flex flex-col items-center justify-center text-center ${
                                            predictionInfo.profit && predictionInfo.profit > 0
                                                ? 'bg-success/10 border-success/20'
                                                : 'bg-danger/10 border-danger/20'
                                        }`}
                                    >
                                        <div
                                            className={`text-xs font-medium uppercase tracking-wider mb-1 ${
                                                predictionInfo.profit && predictionInfo.profit > 0
                                                    ? 'text-success'
                                                    : 'text-danger'
                                            }`}
                                        >
                                            Profit
                                        </div>
                                        <div
                                            className={`text-lg font-black ${
                                                predictionInfo.profit && predictionInfo.profit > 0
                                                    ? 'text-success'
                                                    : 'text-danger'
                                            }`}
                                        >
                                            {predictionInfo.profit
                                                ? predictionInfo.profit > 0
                                                    ? `+$${predictionInfo.profit.toFixed(2)}`
                                                    : `$${predictionInfo.profit.toFixed(2)}`
                                                : '$0.00'}
                                        </div>
                                    </div>
                                </>
                            )}

                            {/* Prediction Details */}
                            {predictionInfo.book_url ? (
                                <a
                                    href={predictionInfo.book_url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="bg-primary/10 border border-primary/20 rounded-lg p-4 flex flex-col items-center justify-center text-center hover:bg-primary/20 transition-colors cursor-pointer group"
                                >
                                    <div className="text-xs font-medium text-primary uppercase tracking-wider mb-1 flex items-center gap-1">
                                        Recommended Bet
                                        <ExternalLink className="h-3 w-3 opacity-50 group-hover:opacity-100" aria-hidden="true" />
                                    </div>
                                    <div className="text-lg font-black text-foreground">
                                        {predictionInfo.recommended_bet || 'N/A'}
                                    </div>
                                    {predictionInfo.edge ? (
                                        <div className="text-[10px] text-success font-medium mt-1 flex items-center gap-1">
                                            {predictionInfo.book && (
                                                <span className="text-muted-foreground">{predictionInfo.book}</span>
                                            )}
                                            {predictionInfo.edge > 0
                                                ? `+${(predictionInfo.edge * 100).toFixed(1)}% Edge`
                                                : ''}
                                        </div>
                                    ) : null}
                                </a>
                            ) : (
                                <div className="bg-primary/10 border border-primary/20 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                    <div className="text-xs font-medium text-primary uppercase tracking-wider mb-1">
                                        Recommended Bet
                                    </div>
                                    <div className="text-lg font-black text-foreground">
                                        {showLock ? 'OVER 215.5' : predictionInfo.recommended_bet || 'N/A'}
                                    </div>
                                    {predictionInfo.edge ? (
                                        <div className="text-[10px] text-success font-medium mt-1 flex items-center gap-1">
                                            {predictionInfo.book && (
                                                <span className="text-muted-foreground">{predictionInfo.book}</span>
                                            )}
                                            {predictionInfo.edge > 0
                                                ? `+${(predictionInfo.edge * 100).toFixed(1)}% Edge`
                                                : ''}
                                        </div>
                                    ) : showLock ? (
                                        <div className="text-[10px] text-success font-medium mt-1 flex items-center gap-1">
                                            <span className="text-muted-foreground">DraftKings</span>
                                            +4.2% Edge
                                        </div>
                                    ) : null}
                                </div>
                            )}
                            <div className="bg-white/5 border border-white/10 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                <div className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-1">
                                    Predicted Total
                                </div>
                                <div className="text-lg font-black text-foreground">
                                    {showLock
                                        ? '224.2'
                                        : predictionInfo.predicted_total_points
                                          ? predictionInfo.predicted_total_points.toFixed(1)
                                          : 'N/A'}
                                </div>
                                <div className="text-xs text-muted-foreground mt-1">Points</div>
                            </div>
                        </div>
                    )}

                    {/* Odds Section */}
                    {showLock ? (
                        <div className="grid gap-2 opacity-20 pointer-events-none filter blur-sm select-none">
                            <div className="grid grid-cols-4 text-xs font-medium text-muted-foreground px-3 mb-1">
                                <div>Sportsbook</div>
                                <div className="text-center">Line</div>
                                <div className="text-center">Over</div>
                                <div className="text-center">Under</div>
                            </div>
                            {[1, 2, 3].map((i) => (
                                <div
                                    key={i}
                                    className="grid grid-cols-4 items-center bg-white/5 rounded-lg p-3 border border-white/5"
                                >
                                    <div className="font-medium text-foreground">DraftKings</div>
                                    <div className="text-center font-bold text-foreground">215.5</div>
                                    <div className="text-center text-muted-foreground">-110</div>
                                    <div className="text-center text-muted-foreground">-110</div>
                                </div>
                            ))}
                        </div>
                    ) : totalsByBook.length > 0 ? (
                        <div>
                            <div className="grid gap-2">
                                <div className="grid grid-cols-4 text-xs font-medium text-muted-foreground px-3 mb-1">
                                    <div>Sportsbook</div>
                                    <div className="text-center">Line</div>
                                    <div className="text-center">Over</div>
                                    <div className="text-center">Under</div>
                                </div>
                                {totalsByBook.map((book) => {
                                    const line = book.over?.line ?? book.under?.line ?? '-';
                                    return (
                                        <a
                                            key={book.name}
                                            href={book.url}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="grid grid-cols-4 items-center bg-white/5 rounded-lg p-3 border border-white/5 hover:border-white/10 hover:bg-white/10 transition-all cursor-pointer group"
                                        >
                                            <div className="font-medium text-foreground flex items-center gap-2 group-hover:text-primary transition-colors">
                                                {book.name}
                                                <ExternalLink className="h-3 w-3 opacity-50 group-hover:opacity-100" aria-hidden="true" />
                                            </div>
                                            <div className="text-center font-bold text-foreground">{line}</div>
                                            <div className="text-center">
                                                {book.over ? (
                                                    <span className="text-xs font-medium text-muted-foreground group-hover:text-foreground">
                                                        {book.over.moneyline > 0 ? '+' : ''}
                                                        {book.over.moneyline}
                                                    </span>
                                                ) : (
                                                    '-'
                                                )}
                                            </div>
                                            <div className="text-center">
                                                {book.under ? (
                                                    <span className="text-xs font-medium text-muted-foreground group-hover:text-foreground">
                                                        {book.under.moneyline > 0 ? '+' : ''}
                                                        {book.under.moneyline}
                                                    </span>
                                                ) : (
                                                    '-'
                                                )}
                                            </div>
                                        </a>
                                    );
                                })}
                            </div>
                        </div>
                    ) : (
                        <div className="text-center py-8 text-muted-foreground">
                            No Over/Under odds available for this game.
                        </div>
                    )}

                    <p className="mt-6 text-[11px] text-muted-foreground/80 leading-relaxed">
                        Sportsbook links may be affiliate links — we may earn a commission if you
                        sign up or wager. Analytics and educational content only; no outcome is
                        guaranteed. 18+ (or 21+ where required). Bet responsibly —{" "}
                        <a href="/responsible-gaming" className="text-primary hover:underline">
                            see resources
                        </a>
                        .
                    </p>
                </div>
            </div>
        </div>
    );
}
