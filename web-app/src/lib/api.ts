const API_BASE_URL = process.env.SPORTS_API_INTERNAL_URL || process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export class ApiError extends Error {
    status: number;
    constructor(message: string, status: number) {
        super(message);
        this.status = status;
        this.name = 'ApiError';
    }
}

export async function fetchAPI<T>(endpoint: string, options?: RequestInit): Promise<T> {
    const headers: Record<string, string> = {
        'Content-Type': 'application/json',
    };
    if (options?.headers) {
        for (const [key, value] of Object.entries(options.headers as Record<string, string>)) {
            if (value !== undefined && value !== null) headers[key] = value;
        }
    }

    const res = await fetch(`${API_BASE_URL}${endpoint}`, {
        ...options,
        headers,
    });

    if (!res.ok) {
        throw new ApiError(`API Error: ${res.statusText}`, res.status);
    }

    return res.json() as Promise<T>;
}

export async function isCheckoutAvailable(): Promise<boolean> {
    try {
        const status = await fetchAPI<{ checkout_available?: boolean }>('/api/bets/access', {
            cache: 'no-store',
            signal: AbortSignal.timeout(5000),
        });
        return status?.checkout_available === true;
    } catch {
        return false;
    }
}

// ============================================================================
// API Response Types
// ============================================================================

export interface StatsResponse {
    roi: number;
    win_rate: number;
    total_profit: number;
    total_bets: number;
}

export interface PublishedPick {
    game_id: string;
    league: string;
    home_team: string;
    away_team: string;
    start_time_utc: string;
    published_at: string;
    settled_at?: string;
    market: string;
    side: string;
    moneyline: number;
    total_line: number | null;
    book: string;
    rule_id: string;
    outcome: 'win' | 'loss' | 'push' | 'void';
    profit_units: number;
    home_score: number | null;
    away_score: number | null;
    reason: string | null;
}

export interface PublishedRecordResponse {
    data: PublishedPick[];
    total: number;
    page: number;
    limit: number;
    graded_picks: number;
    wins: number;
    losses: number;
    pushes: number;
    voids: number;
    profit_units: number;
    roi_percent: number | null;
    win_rate_percent: number | null;
    basis: string;
    published_picks?: number;
    pending_picks?: number;
    grading_corrections?: number;
    last_processed_at?: string | null;
}

export interface OddsRecord {
    market: string;
    outcome: string;
    line: number;
    moneyline: number;
    book: string;
    book_url?: string;
    fetched_at_utc: string;
    snapshot_id?: string;
    observed_start_utc?: string;
}

export interface TotalsComparison {
    book: string;
    url?: string;
    line: number;
    over: number;
    under: number;
    expiresAt: number;
}

const MAX_COMPARISON_AGE_MS = 15 * 60_000;
const MAX_START_DISAGREEMENT_MS = 10 * 60_000;
const UTC_OFFSET = /(?:Z|[+-]\d{2}:\d{2})$/i;

function normalizeTotalsComparisons(
    odds: OddsRecord[], start: number, receivedAt: number,
): TotalsComparison[] {
    const books = new Map<string, OddsRecord[]>();
    for (const quote of odds) {
        if (quote.market !== 'totals' || !quote.book) continue;
        const group = books.get(quote.book) ?? [];
        group.push(quote);
        books.set(quote.book, group);
    }

    const comparisons: TotalsComparison[] = [];
    for (const [book, group] of books) {
        // The warehouse returns the latest rows per side, including timestamp ties.
        if (group.length !== 2) continue;
        const over = group.find((quote) => quote.outcome?.toLowerCase() === 'over');
        const under = group.find((quote) => quote.outcome?.toLowerCase() === 'under');
        if (
            !over || !under || !over.snapshot_id || over.snapshot_id !== under.snapshot_id
            || !Number.isFinite(over.line) || over.line <= 0 || over.line !== under.line
            || over.fetched_at_utc !== under.fetched_at_utc || over.observed_start_utc !== under.observed_start_utc
        ) continue;
        if ([over.moneyline, under.moneyline].some((price) => !Number.isInteger(price) || Math.abs(price) < 100)) continue;

        const observedStart = over.observed_start_utc ?? '';
        if (!UTC_OFFSET.test(over.fetched_at_utc) || !UTC_OFFSET.test(observedStart)) continue;
        const fetchedAt = Date.parse(over.fetched_at_utc);
        const providerStart = Date.parse(observedStart);
        const expiresAt = Math.min(start, providerStart, fetchedAt + MAX_COMPARISON_AGE_MS);
        if (
            !Number.isFinite(expiresAt) || expiresAt <= receivedAt || fetchedAt > receivedAt
            || Math.abs(start - providerStart) > MAX_START_DISAGREEMENT_MS
        ) continue;

        comparisons.push({
            book, url: over.book_url, line: over.line,
            over: over.moneyline, under: under.moneyline, expiresAt,
        });
    }
    return comparisons;
}

/**
 * Raw bet shape returned by the backend. Some fields are aliased
 * (e.g. `description` -> `prediction`, `side` -> `bet_type`,
 * `moneyline` -> `odds`) so we keep both forms optional and normalize
 * via `normalizeBet` below.
 */
export interface RawBet {
    game_id: string;
    commence_time: string;
    home_team: string;
    away_team: string;
    market?: string;
    quote_expires_at?: string;
    bet_type?: string;
    side?: string;
    prediction?: string;
    description?: string;
    odds?: number;
    moneyline?: number;
    edge?: number;
    stake?: number;
    status?: string;
    is_locked?: boolean;
    result?: string;
    profit?: number;
    home_score?: number;
    away_score?: number;
    league?: string;
    book?: string;
    book_url?: string;
    predicted_total_points?: number;
    recommended_bet?: string;
    odds_data?: OddsRecord[];
}

export interface Bet {
    game_id: string;
    commence_time: string;
    home_team: string;
    away_team: string;
    market?: string;
    quote_expires_at?: string;
    bet_type: string;
    prediction: string;
    odds: number;
    edge: number;
    stake: number;
    status: string;
    is_locked?: boolean;
    result?: string;
    profit?: number;
    home_score?: number;
    away_score?: number;
    league?: string;
    book?: string;
    book_url?: string;
    predicted_total_points?: number;
    recommended_bet?: string;
    totalsComparisons?: TotalsComparison[];
}

export interface PaginatedBetsResponse {
    data: RawBet[];
    total?: number;
}

export interface UpcomingBetsResponse {
    data: RawBet[];
    is_premium?: boolean;
}

export function normalizeBet(bet: RawBet): Bet {
    const side = bet.side ?? bet.bet_type ?? '';
    const moneylineTeam = side === 'home' ? bet.home_team : side === 'away' ? bet.away_team : undefined;
    const recommendation = bet.recommended_bet || moneylineTeam;
    return {
        game_id: bet.game_id,
        commence_time: bet.commence_time,
        home_team: bet.home_team,
        away_team: bet.away_team,
        market: bet.market ?? (moneylineTeam ? 'moneyline' : undefined),
        quote_expires_at: bet.quote_expires_at,
        bet_type: side,
        prediction: bet.description || bet.prediction || recommendation || '',
        odds: Number(bet.moneyline ?? bet.odds ?? 0),
        edge: Number(bet.edge ?? 0),
        stake: Number(bet.stake ?? 0),
        status: bet.status ?? 'Pending',
        is_locked: bet.is_locked,
        result: bet.result,
        profit: bet.profit !== undefined ? Number(bet.profit) : undefined,
        home_score: bet.home_score,
        away_score: bet.away_score,
        league: bet.league,
        book: bet.book,
        book_url: bet.book_url,
        predicted_total_points: bet.predicted_total_points,
        recommended_bet: recommendation,
    };
}

export function normalizeUpcomingBets(bets: RawBet[], receivedAt = Date.now()): Bet[] {
    const upcoming: Bet[] = [];
    for (const raw of bets) {
        const bet = normalizeBet(raw);
        const start = Date.parse(bet.commence_time);
        const quoteExpiry = bet.quote_expires_at === undefined
            ? receivedAt + 60_000
            : Date.parse(bet.quote_expires_at);
        const expiresAt = Math.min(start, quoteExpiry);
        if (Number.isFinite(expiresAt) && expiresAt > receivedAt) {
            const totalsComparisons = bet.market === 'moneyline'
                ? undefined
                : normalizeTotalsComparisons(raw.odds_data ?? [], start, receivedAt);
            upcoming.push({ ...bet, quote_expires_at: new Date(expiresAt).toISOString(), totalsComparisons });
        }
    }
    return upcoming;
}
