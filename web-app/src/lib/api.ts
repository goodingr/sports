const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

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

// ============================================================================
// API Response Types
// ============================================================================

export interface StatsResponse {
    roi: number;
    win_rate: number;
    total_profit: number;
    total_bets: number;
}

export interface OddsRecord {
    market: string;
    outcome: string;
    line: number;
    moneyline: number;
    book: string;
    book_url?: string;
    fetched_at_utc: string;
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
    bet_type?: string;
    side?: string;
    prediction?: string;
    description?: string;
    odds?: number;
    moneyline?: number;
    edge?: number;
    stake?: number;
    status: string;
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
    predicted_total_points?: number;
    recommended_bet?: string;
    odds_data?: OddsRecord[];
}

export interface PaginatedBetsResponse {
    data: RawBet[];
    total?: number;
}

export interface UpcomingBetsResponse {
    data: RawBet[];
}

export function normalizeBet(bet: RawBet): Bet {
    return {
        game_id: bet.game_id,
        commence_time: bet.commence_time,
        home_team: bet.home_team,
        away_team: bet.away_team,
        bet_type: bet.side ?? bet.bet_type ?? '',
        prediction: bet.description ?? bet.prediction ?? '',
        odds: Number(bet.moneyline ?? bet.odds ?? 0),
        edge: Number(bet.edge ?? 0),
        stake: Number(bet.stake ?? 0),
        status: bet.status,
        result: bet.result,
        profit: bet.profit !== undefined ? Number(bet.profit) : undefined,
        home_score: bet.home_score,
        away_score: bet.away_score,
        league: bet.league,
        book: bet.book,
        book_url: bet.book_url,
        predicted_total_points: bet.predicted_total_points,
        recommended_bet: bet.recommended_bet,
        odds_data: bet.odds_data,
    };
}
