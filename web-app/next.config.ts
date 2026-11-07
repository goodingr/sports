import type { NextConfig } from "next";
import { PHASE_DEVELOPMENT_SERVER } from 'next/constants';

export default function createNextConfig(phase: string): NextConfig {
  return {
    ...(phase === PHASE_DEVELOPMENT_SERVER && process.env.PLAYWRIGHT_MOCK_AUTH === '1' ? {
      turbopack: {
        resolveAlias: {
          '@clerk/nextjs': './tests/e2e/_fixtures/clerk-client.tsx',
          '@clerk/nextjs/server': './tests/e2e/_fixtures/clerk-server.ts',
        },
      },
    } : {}),
  };
}
