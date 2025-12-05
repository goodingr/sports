import { useEffect, useState } from 'react';
import { X, ExternalLink, Loader2 } from 'lucide-react';

interface OddsRecord {
    market: string;
    outcome: string;
    line: number;
    moneyline: number;
    book: string;
    book_url?: string;
    fetched_at_utc: string;
}

interface MatchDetailsModalProps {
    isOpen: boolean;
    onClose: () => void;
    gameId: string;
    homeTeam: string;
    awayTeam: string;
    predictionInfo?: {
        predicted_total_points: number | null | undefined;
        recommended_bet: string | null | undefined;
        edge: number | null | undefined;
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

export function MatchDetailsModal({ isOpen, onClose, gameId, homeTeam, awayTeam, predictionInfo, oddsData }: MatchDetailsModalProps) {
    const [odds, setOdds] = useState<OddsRecord[]>(oddsData || []);

    // Update odds if prop changes
    useEffect(() => {
        if (oddsData) {
            setOdds(oddsData);
        }
    }, [oddsData]);

    // Close on Escape key
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                onClose();
            }
        };

        if (isOpen) {
            window.addEventListener('keydown', handleKeyDown);
        }

        return () => {
            window.removeEventListener('keydown', handleKeyDown);
        };
    }, [isOpen, onClose]);

    if (!isOpen) return null;

    // Group odds by book
    const totalsByBook = odds.reduce((acc: any[], curr) => {
        const existingBook = acc.find(b => b.name === curr.book);
        if (existingBook) {
            if (curr.outcome === 'Over') existingBook.over = curr;
            if (curr.outcome === 'Under') existingBook.under = curr;
        } else {
            acc.push({
                name: curr.book,
                url: curr.book_url,
                over: curr.outcome === 'Over' ? curr : null,
                under: curr.outcome === 'Under' ? curr : null
            });
        }
        return acc;
    }, []).filter((book: any) => book.over || book.under);

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200" onClick={onClose}>
            <div className="bg-card w-full max-w-md max-h-[90vh] rounded-xl shadow-2xl border border-white/10 flex flex-col animate-in zoom-in-95 duration-200" onClick={e => e.stopPropagation()}>
                {/* Header */}
                <div className="p-4 border-b border-white/10 flex items-center justify-between bg-white/5">
                    <div>
                        <h2 className="text-lg font-bold text-foreground leading-tight">
                            {awayTeam} <span className="text-muted-foreground text-sm font-normal">@</span> {homeTeam}
                        </h2>

                    </div>
                    <button
                        onClick={onClose}
                        className="p-2 hover:bg-white/10 rounded-full transition-colors text-muted-foreground hover:text-foreground"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>

                {/* Content */}
                <div className="flex-1 overflow-y-auto p-4 space-y-6">

                    {/* Prediction Section - Always show if available */}
                    {predictionInfo && (predictionInfo.recommended_bet || predictionInfo.predicted_total_points) && (
                        <div className="grid grid-cols-2 gap-4 mb-6">
                            {/* Result Section (if completed) */}
                            {predictionInfo.status === 'Completed' && (
                                <>
                                    <div className="bg-white/5 border border-white/10 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                        <div className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-1">Final Total</div>
                                        <div className="text-lg font-black text-foreground">
                                            {(predictionInfo.away_score || 0) + (predictionInfo.home_score || 0)}
                                        </div>
                                        <div className="text-xs text-muted-foreground mt-1">
                                            {predictionInfo.away_score} - {predictionInfo.home_score}
                                        </div>
                                    </div>
                                    <div className={`border rounded-lg p-4 flex flex-col items-center justify-center text-center ${predictionInfo.profit && predictionInfo.profit > 0 ? 'bg-success/10 border-success/20' : 'bg-danger/10 border-danger/20'}`}>
                                        <div className={`text-xs font-medium uppercase tracking-wider mb-1 ${predictionInfo.profit && predictionInfo.profit > 0 ? 'text-success' : 'text-danger'}`}>Profit</div>
                                        <div className={`text-lg font-black ${predictionInfo.profit && predictionInfo.profit > 0 ? 'text-success' : 'text-danger'}`}>
                                            {predictionInfo.profit ? (predictionInfo.profit > 0 ? `+$${predictionInfo.profit.toFixed(2)}` : `$${predictionInfo.profit.toFixed(2)}`) : '$0.00'}
                                        </div>
                                    </div>
                                </>
                            )}

                            {/* Prediction Details - Always Show */}
                            <>
                                {predictionInfo.book_url ? (
                                    <a
                                        href={predictionInfo.book_url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="bg-primary/10 border border-primary/20 rounded-lg p-4 flex flex-col items-center justify-center text-center hover:bg-primary/20 transition-colors cursor-pointer group"
                                    >
                                        <div className="text-xs font-medium text-primary uppercase tracking-wider mb-1 flex items-center gap-1">
                                            Recommended Bet
                                            <ExternalLink className="h-3 w-3 opacity-50 group-hover:opacity-100" />
                                        </div>
                                        <div className="text-lg font-black text-foreground">
                                            {predictionInfo.recommended_bet || "N/A"}
                                        </div>
                                        {predictionInfo.edge && (
                                            <div className="text-[10px] text-success font-medium mt-1 flex items-center gap-1">
                                                {predictionInfo.book && <span className="text-muted-foreground">{predictionInfo.book}</span>}
                                                {predictionInfo.edge > 0 ? `+${(predictionInfo.edge * 100).toFixed(1)}% Edge` : ''}
                                            </div>
                                        )}
                                    </a>
                                ) : (
                                    <div className="bg-primary/10 border border-primary/20 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                        <div className="text-xs font-medium text-primary uppercase tracking-wider mb-1">Recommended Bet</div>
                                        <div className="text-lg font-black text-foreground">
                                            {predictionInfo.recommended_bet || "N/A"}
                                        </div>
                                        {predictionInfo.edge && (
                                            <div className="text-[10px] text-success font-medium mt-1 flex items-center gap-1">
                                                {predictionInfo.book && <span className="text-muted-foreground">{predictionInfo.book}</span>}
                                                {predictionInfo.edge > 0 ? `+${(predictionInfo.edge * 100).toFixed(1)}% Edge` : ''}
                                            </div>
                                        )}
                                    </div>
                                )}
                                <div className="bg-white/5 border border-white/10 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                    <div className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-1">Predicted Total</div>
                                    <div className="text-lg font-black text-foreground">
                                        {predictionInfo.predicted_total_points ? predictionInfo.predicted_total_points.toFixed(1) : "N/A"}
                                    </div>
                                    <div className="text-[10px] text-muted-foreground mt-1">
                                        Points
                                    </div>
                                </div>
                            </>
                        </div>
                    )}

                    {/* Odds Section */}
                    {totalsByBook.length > 0 ? (
                        <div>
                            <div className="grid gap-2">
                                <div className="grid grid-cols-4 text-xs font-medium text-muted-foreground px-3 mb-1">
                                    <div>Sportsbook</div>
                                    <div className="text-center">Line</div>
                                    <div className="text-center">Over</div>
                                    <div className="text-center">Under</div>
                                </div>
                                {totalsByBook.map((book: any) => {
                                    // Determine line from either over or under
                                    const line = book.over?.line || book.under?.line || '-';

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
                                                <ExternalLink className="h-3 w-3 opacity-50 group-hover:opacity-100" />
                                            </div>
                                            <div className="text-center font-bold text-foreground">
                                                {line}
                                            </div>
                                            <div className="text-center">
                                                {book.over ? (
                                                    <span className="text-xs font-medium text-muted-foreground group-hover:text-foreground">
                                                        {book.over.moneyline > 0 ? '+' : ''}{book.over.moneyline}
                                                    </span>
                                                ) : '-'}
                                            </div>
                                            <div className="text-center">
                                                {book.under ? (
                                                    <span className="text-xs font-medium text-muted-foreground group-hover:text-foreground">
                                                        {book.under.moneyline > 0 ? '+' : ''}{book.under.moneyline}
                                                    </span>
                                                ) : '-'}
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
                </div>
            </div>
        </div>
    );
}
