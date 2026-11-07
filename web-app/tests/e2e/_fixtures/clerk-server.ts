import { cookies } from 'next/headers';
import { NextResponse, type NextRequest } from 'next/server';

if (process.env.NODE_ENV === 'production') {
    throw new Error('Clerk server fixtures cannot run in production');
}

export async function auth() {
    if (process.env.PLAYWRIGHT_PILOT === '1' && (await cookies()).get('playwright-pilot-session')?.value === 'active') {
        return { userId: 'user_pilot_fixture' };
    }
    return { userId: null };
}

export async function currentUser() {
    return null;
}

export async function clerkClient(): Promise<never> {
    throw new Error('Signed-out browser fixtures cannot access Clerk accounts');
}

export function createRouteMatcher(patterns: string[]) {
    const routes = patterns.map((pattern) => new RegExp(`^${pattern}$`));
    return (request: NextRequest) => routes.some((route) => route.test(request.nextUrl.pathname));
}

type FixtureAuth = typeof auth & { protect: () => Promise<void> };

export function clerkMiddleware(handler: (authenticate: FixtureAuth, request: NextRequest) => Promise<void>) {
    return async (request: NextRequest) => {
        let protectedRedirect: NextResponse | undefined;
        const authenticate = Object.assign(async () => auth(), {
            protect: async () => {
                const login = new URL('/login', request.url);
                login.searchParams.set('redirect_url', request.nextUrl.pathname);
                protectedRedirect = NextResponse.redirect(login);
            },
        });
        await handler(authenticate, request);
        return protectedRedirect ?? NextResponse.next();
    };
}
