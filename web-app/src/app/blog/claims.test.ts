import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import AppOverviewPage from './app-overview/page';
import MoneylinePage from './moneyline/page';
import OverUnderPage from './over-under/page';

describe('public betting articles', () => {
    it.each([
        ['moneyline', MoneylinePage, 'that difference is an estimated edge', '150 / (150 + 100) = 60%'],
        ['totals', OverUnderPage, 'We evaluate model estimates against game results and closing prices', 'risk $110 to win $100'],
        ['app overview', AppOverviewPage, 'eligible pregame picks when available', 'that historical sample'],
    ] as const)('links %s claims to the published pick record', (_, Page, qualifier, example) => {
        const markup = renderToStaticMarkup(Page());

        expect(markup).toContain(qualifier);
        expect(markup).toContain(example);
        expect(markup).toContain('href="/record">View the published pick record</a>');
    });
});
