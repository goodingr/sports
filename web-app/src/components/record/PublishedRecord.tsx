'use client';

import { useEffect, useState } from 'react';
import { AlertCircle } from 'lucide-react';
import Link from 'next/link';
import { fetchAPI, type PublishedRecordResponse } from '@/lib/api';

function formatTimestamp(value: string): string {
    return new Date(value).toLocaleString('en-US', {
        month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit',
        timeZone: 'UTC', timeZoneName: 'short',
    });
}

function units(value: number): string {
    return `${value > 0 ? '+' : ''}${value.toFixed(2)}u`;
}

export function PublishedRecord({ summary = false }: { summary?: boolean }) {
    const [record, setRecord] = useState<PublishedRecordResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(false);
    const [request, setRequest] = useState({ page: 1 });
    const [loadedAt, setLoadedAt] = useState<string | null>(null);
    const limit = summary ? 1 : 20;

    useEffect(() => {
        let active = true;
        fetchAPI<PublishedRecordResponse>(`/api/bets/record?page=${request.page}&limit=${limit}`, {
            cache: 'no-store', signal: AbortSignal.timeout(10000),
        }).then((data) => {
            if (!active) return;
            const lastPage = Math.max(1, Math.ceil(data.total / limit));
            // A grade correction can remove a page between requests.
            if (request.page > lastPage) {
                setRequest({ page: lastPage });
                return;
            }
            setRecord(data);
            setLoadedAt(new Date().toISOString());
            setLoading(false);
        }).catch(() => {
            if (active) {
                setError(true);
                setLoading(false);
            }
        });
        return () => { active = false; };
    }, [limit, request]);

    function loadPage(page: number) {
        setLoading(true);
        setError(false);
        setRequest({ page });
    }

    if (summary) {
        const issued = record ? record.published_picks ?? record.total + (record.pending_picks ?? 0) : null;
        return (
            <section aria-labelledby="published-summary" className="rounded-xl border border-white/10 bg-card p-5 sm:p-6">
                <h1 id="published-summary" className="text-xl font-semibold text-foreground">Published pick record</h1>
                {loading ? (
                    <p role="status" className="mt-2 text-sm text-muted-foreground">Loading published results…</p>
                ) : error || !record ? (
                    <p role="alert" className="mt-2 text-sm text-muted-foreground">Published results are temporarily unavailable.</p>
                ) : issued === 0 ? (
                    <p className="mt-2 text-sm text-muted-foreground">No picks have been published yet.</p>
                ) : (
                    <>
                        <dl className="mt-4 grid grid-cols-3 gap-3">
                            <div><dt className="text-xs text-muted-foreground">Published picks</dt><dd className="font-semibold">{issued}</dd></div>
                            <div><dt className="text-xs text-muted-foreground">Graded picks</dt><dd className="font-semibold">{record.graded_picks}</dd></div>
                            <div><dt className="text-xs text-muted-foreground">Published ROI</dt><dd className="font-semibold">{record.roi_percent == null ? '–' : `${record.roi_percent.toFixed(2)}%`}</dd></div>
                        </dl>
                        <p className="mt-2 text-xs text-muted-foreground">One unit per pick at its original price, before subscription fees.</p>
                    </>
                )}
                <Link href="/record" className="mt-3 inline-block text-sm text-primary hover:underline">View the full published pick record</Link>
            </section>
        );
    }

    if (loading) {
        return <p role="status" className="py-12 text-center text-muted-foreground">Loading published results…</p>;
    }
    if (error || !record) {
        return (
            <div role="alert" className="rounded-xl border border-danger/20 bg-danger/10 p-6 text-center">
                <AlertCircle className="mx-auto mb-3 h-6 w-6 text-danger" aria-hidden="true" />
                <p>Published results are temporarily unavailable.</p>
                <button type="button" onClick={() => loadPage(request.page)} className="mt-4 rounded-lg border border-white/10 px-4 py-2 text-sm hover:bg-white/5">
                    Retry
                </button>
            </div>
        );
    }

    const metrics = [
        { label: 'Profit / loss', value: units(record.profit_units) },
        { label: 'ROI', value: record.roi_percent == null ? '–' : `${record.roi_percent.toFixed(2)}%` },
        { label: 'Win rate', value: record.win_rate_percent == null ? '–' : `${record.win_rate_percent.toFixed(1)}%` },
        { label: 'Win / loss / push', value: `${record.wins} / ${record.losses} / ${record.pushes}` },
    ];

    return (
        <div className="space-y-6">
            {record.total === 0 ? (
                <div className="rounded-xl border border-white/10 bg-card p-8 text-center">
                    <h2 className="text-lg font-semibold">No settled published picks yet</h2>
                    <p className="mt-2 text-sm text-muted-foreground">Results will appear after issued picks are settled.</p>
                </div>
            ) : (
                <>
                    <dl className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        {metrics.map((metric) => (
                            <div key={metric.label} className="border border-white/10 bg-card p-4">
                                <dt className="text-xs text-muted-foreground">{metric.label}</dt>
                                <dd className="mt-2 text-xl font-bold">{metric.value}</dd>
                            </div>
                        ))}
                    </dl>
                    <p className="text-xs leading-relaxed text-muted-foreground">
                        {record.basis}. ROI includes {record.graded_picks} graded picks, including pushes.
                        Win rate uses wins and losses. {record.voids} voided picks are excluded from both rates.
                    </p>

                    <ol aria-label="Settled published picks" className="space-y-3">
                        {record.data.map((pick) => {
                            const moneylineTeam = pick.side === 'home' ? pick.home_team : pick.away_team;
                            const selection = pick.market === 'moneyline'
                                ? moneylineTeam
                                : `${pick.side.toUpperCase()} ${pick.total_line ?? ''}`;
                            return (
                                <li key={`${pick.game_id}-${pick.market}`} className="border border-white/10 bg-card p-4 sm:p-5">
                                    <div className="flex items-start justify-between gap-3">
                                        <div className="min-w-0">
                                            <div className="text-xs uppercase tracking-wide text-muted-foreground">{pick.league} · {pick.market}</div>
                                            <h2 className="mt-1 font-semibold break-words">{pick.away_team} at {pick.home_team}</h2>
                                            <p className="mt-2 font-bold text-primary">{selection} · {pick.moneyline > 0 ? '+' : ''}{pick.moneyline}</p>
                                        </div>
                                        <div className="shrink-0 text-right">
                                            <p className="text-xs uppercase text-muted-foreground">{pick.outcome}</p>
                                            <p className="mt-1 font-semibold">{units(pick.profit_units)}</p>
                                        </div>
                                    </div>
                                    <dl className="mt-4 grid gap-2 text-xs text-muted-foreground sm:grid-cols-2">
                                        <div>
                                            <dt className="inline font-medium">Published </dt>
                                            <dd className="inline"><time dateTime={pick.published_at}>{formatTimestamp(pick.published_at)}</time></dd>
                                        </div>
                                        <div>
                                            <dt className="inline font-medium">Started </dt>
                                            <dd className="inline"><time dateTime={pick.start_time_utc}>{formatTimestamp(pick.start_time_utc)}</time></dd>
                                        </div>
                                        {pick.settled_at && (
                                            <div>
                                                <dt className="inline font-medium">Graded </dt>
                                                <dd className="inline"><time dateTime={pick.settled_at}>{formatTimestamp(pick.settled_at)}</time></dd>
                                            </div>
                                        )}
                                        <div>
                                            <dt className="inline font-medium">Book </dt>
                                            <dd className="inline">{pick.book}</dd>
                                        </div>
                                        {pick.outcome !== 'void' && pick.away_score != null && pick.home_score != null && (
                                            <div>
                                                <dt className="inline font-medium">Final score </dt>
                                                <dd className="inline">{pick.away_score} – {pick.home_score} (away – home)</dd>
                                            </div>
                                        )}
                                        <div className="sm:col-span-2 break-words">
                                            <dt className="inline font-medium">Rule </dt>
                                            <dd className="inline">{pick.rule_id}</dd>
                                        </div>
                                    </dl>
                                    {pick.outcome === 'void' && pick.reason && <p className="mt-2 text-xs text-muted-foreground">{pick.reason.replaceAll('_', ' ')}</p>}
                                </li>
                            );
                        })}
                    </ol>

                    {record.total > record.limit && (
                        <nav aria-label="Published results pagination" className="flex items-center justify-between gap-3 text-sm">
                            <button type="button" disabled={record.page <= 1} onClick={() => loadPage(record.page - 1)} className="rounded-lg border border-white/10 px-4 py-2 disabled:opacity-40">Previous</button>
                            <span>Page {record.page} of {Math.ceil(record.total / record.limit)}</span>
                            <button type="button" disabled={record.page * record.limit >= record.total} onClick={() => loadPage(record.page + 1)} className="rounded-lg border border-white/10 px-4 py-2 disabled:opacity-40">Next</button>
                        </nav>
                    )}
                </>
            )}

            <div className="border-t border-white/10 pt-4 text-xs leading-relaxed text-muted-foreground space-y-2">
                {record.last_processed_at && <p>Grading processed <time dateTime={record.last_processed_at}>{formatTimestamp(record.last_processed_at)}</time>.</p>}
                {(record.pending_picks ?? 0) > 0 && <p>{record.pending_picks} published picks awaiting settlement.</p>}
                {(record.grading_corrections ?? 0) > 0 && <p>{record.grading_corrections} grading corrections are included, with prior grades retained in the audit history.</p>}
                {loadedAt && <p>Loaded <time dateTime={loadedAt}>{formatTimestamp(loadedAt)}</time>. Times shown in UTC.</p>}
                <p>One unit is a fixed stake used to compare results before subscription fees. Customer returns depend on the price and stake obtained.</p>
                <Link href="/#model-history" className="inline-block text-primary hover:underline">View separate historical model tracking</Link>
            </div>
        </div>
    );
}
