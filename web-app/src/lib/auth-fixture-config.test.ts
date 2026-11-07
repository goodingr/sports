import { afterEach, describe, expect, it, vi } from 'vitest';
import { PHASE_DEVELOPMENT_SERVER, PHASE_PRODUCTION_BUILD, PHASE_PRODUCTION_SERVER } from 'next/constants';

afterEach(() => {
    vi.unstubAllEnvs();
    vi.resetModules();
});

describe('Playwright authentication fixtures', () => {
    it.each([
        [PHASE_PRODUCTION_BUILD, '1', false],
        [PHASE_PRODUCTION_SERVER, '1', false],
        [PHASE_DEVELOPMENT_SERVER, '', false],
        [PHASE_DEVELOPMENT_SERVER, '1', true],
    ])('uses fixtures only for explicit development tests (%s, %s)', async (phase, enabled, expected) => {
        vi.stubEnv('NODE_ENV', 'development');
        vi.stubEnv('PLAYWRIGHT_MOCK_AUTH', enabled);
        vi.resetModules();

        const createConfig = (await import('../../next.config')).default;
        const config = createConfig(phase);

        expect(Boolean(config.turbopack?.resolveAlias?.['@clerk/nextjs'])).toBe(expected);
        expect(Boolean(config.turbopack?.resolveAlias?.['@clerk/nextjs/server'])).toBe(expected);
    });

    it('rejects the browser fixture if imported in production', async () => {
        vi.stubEnv('NODE_ENV', 'production');
        await expect(import('../../tests/e2e/_fixtures/clerk-client')).rejects.toThrow(
            'Clerk browser fixtures cannot run in production',
        );
    });

    it('rejects the server fixture if imported in production', async () => {
        vi.stubEnv('NODE_ENV', 'production');
        await expect(import('../../tests/e2e/_fixtures/clerk-server')).rejects.toThrow(
            'Clerk server fixtures cannot run in production',
        );
    });
});
