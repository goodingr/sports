import type { Page } from '@playwright/test';

const future = (offsetMin: number) => new Date(Date.now() + offsetMin * 60_000).toISOString();
const past = (offsetMin: number) => new Date(Date.now() - offsetMin * 60_000).toISOString();

export const MOCK_UPCOMING = [
    {
        game_id: 'mock-upcoming-1',
        commence_time: future(60),
        home_team: 'Mock Home FC',
        away_team: 'Mock Away FC',
        bet_type: 'OVER',
        prediction: 'OVER 215.5',
        odds: -110,
        edge: 0.042,
        stake: 100,
        status: 'Pending',
        league: 'NBA',
        book: 'DraftKings',
        recommended_bet: 'OVER 215.5',
        predicted_total_points: 224.2,
    },
    {
        game_id: 'mock-upcoming-2',
        commence_time: future(180),
        home_team: 'Demo Hawks',
        away_team: 'Demo Wolves',
        bet_type: 'UNDER',
        prediction: 'UNDER 220.5',
        odds: -105,
        edge: 0.031,
        stake: 100,
        status: 'Pending',
        league: 'NBA',
        book: 'FanDuel',
    },
];

export const MOCK_HISTORY = [
    {
        game_id: 'mock-history-1',
        commence_time: past(120),
        home_team: 'Past Home',
        away_team: 'Past Away',
        bet_type: 'OVER',
        prediction: 'OVER 210.5',
        odds: -110,
        edge: 0.05,
        stake: 100,
        status: 'Completed',
        result: 'Win',
        profit: 90.91,
        home_score: 110,
        away_score: 105,
        league: 'NBA',
    },
];

export const MOCK_STATS = {
    roi: 4.8,
    win_rate: 56,
    total_profit: 12450,
    total_bets: 312,
};

interface MockOptions {
    upcoming?: unknown[];
    history?: unknown[];
    stats?: unknown;
    historyTotal?: number;
}

/**
 * Routes the standard backend endpoints through Playwright route mocks so the
 * browser receives deterministic data without needing the Python backend.
 */
export async function mockBetsRoutes(page: Page, opts: MockOptions = {}): Promise<void> {
    const upcoming = opts.upcoming ?? MOCK_UPCOMING;
    const history = opts.history ?? MOCK_HISTORY;
    const stats = opts.stats ?? MOCK_STATS;
    const total = opts.historyTotal ?? history.length;

    await page.route('**/api/bets/stats', async (route) => {
        await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(stats) });
    });
    await page.route('**/api/bets/upcoming**', async (route) => {
        await route.fulfill({
            status: 200,
            contentType: 'application/json',
            body: JSON.stringify({ data: upcoming }),
        });
    });
    await page.route('**/api/bets/history**', async (route) => {
        await route.fulfill({
            status: 200,
            contentType: 'application/json',
            body: JSON.stringify({ data: history, total }),
        });
    });
}
