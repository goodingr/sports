import { clerkMiddleware, createRouteMatcher } from "@clerk/nextjs/server";
import { NextResponse, type NextFetchEvent, type NextRequest } from "next/server";
import { interestConfigurationUnavailable, isInterestConfigurationReady, isPilotOnly, pilotBillingUnavailable } from "@/lib/pilot";

const isProtectedRoute = createRouteMatcher([
    '/dashboard(.*)',
    '/account(.*)',
]);

const isPilotLandingRedirect = createRouteMatcher([
    '/', '/account(.*)', '/dashboard(.*)', '/pricing', '/record', '/blog(.*)',
]);
const isBillingRoute = createRouteMatcher([
    '/api/checkout', '/api/billing/portal', '/api/webhooks/stripe',
]);
const isInterestRoute = createRouteMatcher(['/interest', '/api/interest']);
const isPilotReadinessRoute = createRouteMatcher(['/api/pilot/ready']);

const authenticate = clerkMiddleware(async (auth, req) => {
    if (isProtectedRoute(req)) await auth.protect();
});

export default function middleware(request: NextRequest, event: NextFetchEvent) {
    if (isPilotReadinessRoute(request)) return NextResponse.next();
    const pilotOnly = isPilotOnly();
    if (pilotOnly && isBillingRoute(request)) return pilotBillingUnavailable();
    if (
        (pilotOnly || isInterestRoute(request)) && process.env.COMMERCIAL_INTEREST_ENABLED === 'true'
        && !isInterestConfigurationReady()
    ) return interestConfigurationUnavailable();
    if (pilotOnly && isPilotLandingRedirect(request)) {
        const target = request.nextUrl.clone();
        target.pathname = '/interest';
        return NextResponse.redirect(target);
    }
    return authenticate(request, event);
}

export const config = {
    matcher: [
        // Skip Next.js internals and all static files, unless found in search params
        '/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)',
        // Always run for API routes
        '/(api|trpc)(.*)',
    ],
};
