import { auth } from "@clerk/nextjs/server";
import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { Container } from "@/components/ui/Container";
import { isPilotOnly } from "@/lib/pilot";
import { InterestControls } from "./InterestControls";

const SOURCES = new Set(["direct", "community", "referral", "email", "other"]);

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
    title: "Pro Access Interest | Sports Analytics",
    robots: { index: false, follow: false },
};

interface InterestPageProps {
    searchParams: Promise<{ source?: string | string[] }>;
}

function normalizeSource(value: string | string[] | undefined): string {
    if (value === undefined) return "direct";
    if (typeof value === "string" && SOURCES.has(value)) return value;
    return "other";
}

export default async function InterestPage({ searchParams }: InterestPageProps) {
    if (process.env.COMMERCIAL_INTEREST_ENABLED !== "true") notFound();

    const enrollmentClosed = process.env.COMMERCIAL_INTEREST_ENROLLMENT_CLOSED === "true";
    const [{ userId }, params] = await Promise.all([auth(), searchParams]);
    const source = normalizeSource(params.source);
    const returnUrl = encodeURIComponent("/interest?source=" + source);

    return (
        <Container className="max-w-2xl py-12 sm:py-16">
            <h1 className="mb-3 text-3xl font-bold text-foreground sm:text-4xl">
                {enrollmentClosed ? "Manage your Pro Access interest" : "Interested in Pro Access?"}
            </h1>
            <p className="mb-8 text-muted-foreground">
                {enrollmentClosed
                    ? "Interest enrollment is closed. You can still review or withdraw interest you previously recorded."
                    : "We’re measuring interest in a subscription for pregame picks and their sportsbook prices. Paid subscriptions are currently unavailable while we validate the picks."}
            </p>

            <section
                aria-labelledby="interest-offer"
                className="space-y-5 rounded-2xl border border-white/10 bg-card p-6 sm:p-8"
            >
                <div>
                    <h2 id="interest-offer" className="text-lg font-semibold text-foreground">
                        Proposed subscription price
                    </h2>
                    <p className="mt-1 text-3xl font-bold text-foreground">$29/month</p>
                </div>
                {!enrollmentClosed && (
                    <p className="text-sm text-muted-foreground">
                        We count signed-in visits once to measure interest in the offer.
                        Recording interest is voluntary and free. It creates no subscription
                        or payment obligation. You can withdraw your interest on this page.
                    </p>
                )}
                {isPilotOnly() && (
                    <p className="text-sm text-muted-foreground">
                        During enrollment, we record your first signed-in offer visit and link source
                        with your Clerk account. We also record any opt-in or withdrawal. We email you
                        for pilot research only while your opt-in is active.{" "}
                        <Link href="/privacy#pilot-data" className="text-primary underline">
                            Read how we use pilot data
                        </Link>
                    </p>
                )}

                {userId ? (
                    <InterestControls key={source} source={source} enrollmentClosed={enrollmentClosed} />
                ) : (
                    <div className="flex flex-wrap items-center gap-4 text-sm font-semibold">
                        <Link
                            href={"/login?redirect_url=" + returnUrl}
                            className="rounded-lg bg-primary px-4 py-2 text-primary-foreground"
                        >
                            {enrollmentClosed ? "Sign in to manage interest" : "Sign in to record interest"}
                        </Link>
                        {!enrollmentClosed && (
                            <Link
                                href={"/signup?redirect_url=" + returnUrl}
                                className="text-primary underline"
                            >
                                Create an account
                            </Link>
                        )}
                    </div>
                )}
            </section>
        </Container>
    );
}
