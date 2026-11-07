'use client';

import { useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { X, ExternalLink, Lock } from 'lucide-react';
import type { TotalsComparison } from '@/lib/api';

interface MatchDetailsModalProps {
    isOpen: boolean;
    onClose: () => void;
    /** kept for future detail fetches; reserved */
    gameId?: string;
    homeTeam: string;
    awayTeam: string;
    commenceTime?: string;
    market?: string;
    /** When true, the body of the modal is locked behind the paywall. */
    isLocked?: boolean;
    predictionInfo?: {
        predicted_total_points?: number | null;
        recommended_bet?: string | null;
        edge?: number | null;
        odds?: string;
        home_score?: number | null;
        away_score?: number | null;
        profit?: number | null;
        won?: boolean | null;
        status?: string;
        book?: string;
        book_url?: string;
    };
    totalsComparisons?: TotalsComparison[];
}

const FOCUSABLE_SELECTOR =
    'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"]), input:not([disabled])';

export function MatchDetailsModal({
    isOpen,
    onClose,
    homeTeam,
    awayTeam,
    commenceTime,
    market,
    isLocked = false,
    predictionInfo,
    totalsComparisons = [],
}: MatchDetailsModalProps) {
    const dialogRef = useRef<HTMLDivElement>(null);
    const closeButtonRef = useRef<HTMLButtonElement>(null);

    // Close on Escape; trap Tab focus inside dialog.
    useEffect(() => {
        if (!isOpen) return;
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                e.stopPropagation();
                onClose();
                return;
            }
            if (e.key !== 'Tab') return;
            const dialog = dialogRef.current;
            if (!dialog) return;
            const focusables = Array.from(
                dialog.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
            ).filter((el) => !el.hasAttribute('disabled') && el.offsetParent !== null);
            if (focusables.length === 0) {
                e.preventDefault();
                return;
            }
            const first = focusables[0];
            const last = focusables[focusables.length - 1];
            const active = document.activeElement as HTMLElement | null;
            if (e.shiftKey) {
                if (active === first || !dialog.contains(active)) {
                    e.preventDefault();
                    last.focus();
                }
            } else {
                if (active === last || !dialog.contains(active)) {
                    e.preventDefault();
                    first.focus();
                }
            }
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => {
            window.removeEventListener('keydown', handleKeyDown);
        };
    }, [isOpen, onClose]);

    // Move focus into the dialog on open; restore on close.
    useEffect(() => {
        if (!isOpen) return;
        const previouslyFocused = document.activeElement as HTMLElement | null;
        // Defer to next tick so the dialog has been laid out before we focus.
        const id = window.setTimeout(() => {
            closeButtonRef.current?.focus();
        }, 0);
        return () => {
            window.clearTimeout(id);
            previouslyFocused?.focus?.();
        };
    }, [isOpen]);

    // Lock body scroll while modal is open and mark siblings inert so background
    // controls cannot be tabbed to or clicked.
    useEffect(() => {
        if (!isOpen) return;
        const original = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        const dialog = dialogRef.current;
        const siblings: Element[] = dialog?.parentElement
            ? Array.from(dialog.parentElement.children).filter((el) => el !== dialog)
            : Array.from(document.body.children).filter(
                  (el) => el.tagName !== 'SCRIPT' && el.tagName !== 'STYLE',
              );
        const restore: Array<() => void> = [];
        for (const el of siblings) {
            const html = el as HTMLElement;
            const hadInert = html.hasAttribute('inert');
            const hadAriaHidden = html.getAttribute('aria-hidden');
            html.setAttribute('inert', '');
            html.setAttribute('aria-hidden', 'true');
            restore.push(() => {
                if (!hadInert) html.removeAttribute('inert');
                if (hadAriaHidden === null) html.removeAttribute('aria-hidden');
                else html.setAttribute('aria-hidden', hadAriaHidden);
            });
        }
        return () => {
            document.body.style.overflow = original;
            for (const fn of restore) fn();
        };
    }, [isOpen]);

    if (!isOpen) return null;
    if (typeof document === 'undefined') return null;

    const formattedDate = commenceTime
        ? new Date(commenceTime).toLocaleDateString('en-US', {
              weekday: 'short',
              month: 'short',
              day: 'numeric',
              hour: 'numeric',
              minute: 'numeric',
          })
        : '';

    // Treat a server-masked recommendation as locked too.
    const lockedFromServer = predictionInfo?.recommended_bet === 'Premium Only';
    const showLock = isLocked || lockedFromServer;
    const isCompleted = predictionInfo?.status === 'Completed';
    const hasTotals = market !== 'moneyline';
    const hasScores = predictionInfo?.away_score != null && predictionInfo.home_score != null;

    const dialogNode = (
        <div
            ref={dialogRef}
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
                        ref={closeButtonRef}
                        type="button"
                        onClick={onClose}
                        aria-label="Close match details"
                        className="p-2 hover:bg-white/10 rounded-full transition-colors text-muted-foreground hover:text-foreground shrink-0 focus:outline-none focus:ring-2 focus:ring-primary/50"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>

                {/* Content */}
                <div className="flex-1 overflow-y-auto p-4 space-y-6 relative">
                    {/* Premium Lock Overlay */}
                    {showLock && (
                        <div className="flex flex-col items-center justify-center px-6 py-10 text-center">
                            <div className="bg-primary/10 p-4 rounded-full mb-4 ring-1 ring-primary/20">
                                <Lock className="h-8 w-8 text-primary" />
                            </div>
                            <h3 className="text-xl font-bold text-foreground mb-2">Premium Only</h3>
                            <p className="text-sm text-muted-foreground mb-6 max-w-[260px]">
                                Pro Access includes the published selection, sportsbook price, and model edge estimate.
                            </p>
                            <a
                                href="/pricing"
                                data-testid="modal-paywall-cta"
                                className="bg-primary hover:bg-primary/90 text-primary-foreground font-bold px-8 py-3 rounded-lg transition-all transform hover:scale-105 shadow-lg shadow-primary/25 focus:outline-none focus:ring-2 focus:ring-primary/50"
                            >
                                Subscribe to Unlock
                            </a>
                        </div>
                    )}

                    {/* Prediction Section */}
                    {!showLock && predictionInfo && (
                        <div className="grid grid-cols-2 gap-3 sm:gap-4 mb-6">
                            {/* Result Section (if completed) */}
                            {isCompleted && (
                                <>
                                    <div className="bg-white/5 border border-white/10 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                        <div className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-1">
                                            Final Total
                                        </div>
                                        <div className="text-lg font-black text-foreground">
                                            {hasScores ? (predictionInfo.away_score ?? 0) + (predictionInfo.home_score ?? 0) : '–'}
                                        </div>
                                        {hasScores && (
                                            <div className="text-xs text-muted-foreground mt-1">
                                                {predictionInfo.away_score} - {predictionInfo.home_score}
                                            </div>
                                        )}
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
                                            Simulated P&amp;L
                                        </div>
                                        <div
                                            className={`text-lg font-black ${
                                                predictionInfo.profit && predictionInfo.profit > 0
                                                    ? 'text-success'
                                                    : 'text-danger'
                                            }`}
                                        >
                                            {predictionInfo.profit?.toLocaleString('en-US', {
                                                style: 'currency', currency: 'USD', signDisplay: 'exceptZero',
                                            }) ?? '–'}
                                        </div>
                                    </div>
                                </>
                            )}

                            {/* Prediction Details */}
                            {!isCompleted && predictionInfo.book_url ? (
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
                                        {predictionInfo.recommended_bet}
                                    </div>
                                    <div className="text-xs text-muted-foreground">{predictionInfo.odds}</div>
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
                            ) : predictionInfo.recommended_bet && (
                                <div className="bg-primary/10 border border-primary/20 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                    <div className="text-xs font-medium text-primary uppercase tracking-wider mb-1">
                                        {isCompleted ? 'Model Selection' : 'Recommended Bet'}
                                    </div>
                                    <div className="text-lg font-black text-foreground">
                                        {predictionInfo.recommended_bet}
                                    </div>
                                    <div className="text-xs text-muted-foreground">{predictionInfo.odds}</div>
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
                                </div>
                            )}
                            {hasTotals && predictionInfo.predicted_total_points != null && (
                                <div className="bg-white/5 border border-white/10 rounded-lg p-4 flex flex-col items-center justify-center text-center">
                                    <div className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-1">
                                        Predicted Total
                                    </div>
                                    <div className="text-lg font-black text-foreground">
                                        {predictionInfo.predicted_total_points.toFixed(1)}
                                    </div>
                                    <div className="text-xs text-muted-foreground mt-1">Points</div>
                                </div>
                            )}
                        </div>
                    )}

                    {/* Odds Section */}
                    {!showLock && hasTotals && !isCompleted && totalsComparisons.length > 0 && (
                        <div>
                            <h3 className="text-sm font-medium mb-2">Pregame price comparison</h3>
                            <div className="grid gap-2">
                                <div className="grid grid-cols-4 text-xs font-medium text-muted-foreground px-3 mb-1">
                                    <div>Sportsbook</div>
                                    <div className="text-center">Line</div>
                                    <div className="text-center">Over</div>
                                    <div className="text-center">Under</div>
                                </div>
                                {totalsComparisons.map((comparison) => (
                                    <a
                                        key={comparison.book}
                                        href={comparison.url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="grid grid-cols-4 items-center bg-white/5 rounded-lg p-3 border border-white/5 hover:border-white/10 hover:bg-white/10 transition-all cursor-pointer group"
                                    >
                                        <div className="font-medium text-foreground flex items-center gap-2 group-hover:text-primary transition-colors">
                                            {comparison.book}
                                            <ExternalLink className="h-3 w-3 opacity-50 group-hover:opacity-100" aria-hidden="true" />
                                        </div>
                                        <div className="text-center font-bold text-foreground">{comparison.line}</div>
                                        <div className="text-center">
                                            <span className="text-xs font-medium text-muted-foreground group-hover:text-foreground">
                                                {comparison.over > 0 ? '+' : ''}{comparison.over}
                                            </span>
                                        </div>
                                        <div className="text-center">
                                            <span className="text-xs font-medium text-muted-foreground group-hover:text-foreground">
                                                {comparison.under > 0 ? '+' : ''}{comparison.under}
                                            </span>
                                        </div>
                                    </a>
                                ))}
                            </div>
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

    return createPortal(dialogNode, document.body);
}
