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
}

export function MatchDetailsModal({ isOpen, onClose, gameId, homeTeam, awayTeam }: MatchDetailsModalProps) {
    const [odds, setOdds] = useState<OddsRecord[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        if (isOpen && gameId) {
            fetchOdds();
        }
    }, [isOpen, gameId]);

    const fetchOdds = async () => {
        setLoading(true);
        setError(null);
        try {
            const response = await fetch(`http://localhost:8000/api/bets/game/${gameId}/odds`);
            if (!response.ok) {
                throw new Error('Failed to fetch odds');
            }
            const data = await response.json();
            setOdds(data.data || []);
        } catch (err) {
            console.error(err);
            setError('Could not load odds details.');
        } finally {
            setLoading(false);
        }
    };

    if (!isOpen) return null;

    // Filter for Totals only
    const totalsOdds = odds.filter(o => o.market === 'totals');

    // Helper to group by book
    const groupByBook = (records: OddsRecord[]) => {
        const books: Record<string, any> = {};
        records.forEach(r => {
            if (!books[r.book]) {
                books[r.book] = { name: r.book, url: r.book_url };
            }
            // outcome is 'over', 'under'
            books[r.book][r.outcome.toLowerCase()] = r;
        });
        return Object.values(books);
    };

    const totalsByBook = groupByBook(totalsOdds);

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm" onClick={onClose}>
            <div className="bg-card w-full max-w-2xl rounded-xl border border-white/10 shadow-2xl overflow-hidden flex flex-col max-h-[90vh]" onClick={e => e.stopPropagation()}>

                {/* Header */}
                <div className="p-4 border-b border-white/10 flex items-center justify-between bg-white/5">
                    <div>
                        <h2 className="text-lg font-bold text-foreground">{awayTeam} @ {homeTeam}</h2>
                        <p className="text-xs text-muted-foreground">Over / Under Comparison</p>
                    </div>
                    <button onClick={onClose} className="p-2 hover:bg-white/10 rounded-full transition-colors">
                        <X className="h-5 w-5 text-muted-foreground" />
                    </button>
                </div>

                {/* Content */}
                <div className="flex-1 overflow-y-auto p-4 space-y-6">
                    {loading ? (
                        <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
                            <Loader2 className="h-8 w-8 animate-spin mb-2" />
                            <p>Loading odds...</p>
                        </div>
                    ) : error ? (
                        <div className="text-center py-8 text-danger">
                            {error}
                        </div>
                    ) : (
                        <>
                            {/* Totals Section */}
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
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}
