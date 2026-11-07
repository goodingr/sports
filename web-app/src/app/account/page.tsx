import { Container } from "@/components/ui/Container";
import { auth, currentUser } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";
import Link from "next/link";
import type { Metadata } from "next";
import { ManageSubscriptionButton } from "@/components/account/ManageSubscriptionButton";
import { isCheckoutSessionId } from "@/lib/stripe-entitlements";
import { CheckoutReturnStatus } from "./CheckoutReturnStatus";

export const metadata: Metadata = {
    title: "Account | Sports Analytics",
    description: "Manage your subscription, billing, and account.",
};

interface PageProps {
    searchParams: Promise<{ success?: string; session_id?: string | string[] }>;
}

function formatDate(epochSeconds: number): string {
    return new Date(epochSeconds * 1000).toLocaleDateString("en-US", {
        year: "numeric",
        month: "long",
        day: "numeric",
    });
}

export default async function AccountPage({ searchParams }: PageProps) {
    const params = await searchParams;
    const sessionId = params.success === "true" && isCheckoutSessionId(params.session_id)
        ? params.session_id : null;
    const { userId } = await auth();
    if (!userId) {
        const destination = sessionId ? `/account?success=true&session_id=${sessionId}` : "/account";
        redirect(`/login?redirect_url=${encodeURIComponent(destination)}`);
    }

    const user = await currentUser();
    const showLegacyStatus = params.success === "true" && params.session_id === undefined;

    const meta = (user?.publicMetadata ?? {}) as Record<string, unknown>;
    const isPremium = meta.is_premium === true || meta.isPremium === true;
    const status = typeof meta.subscription_status === "string" ? meta.subscription_status : null;
    const cancelScheduled = meta.subscription_cancel_at_period_end === true;
    const periodEnd =
        typeof meta.subscription_current_period_end === "number"
            ? meta.subscription_current_period_end
            : null;
    const hasCustomer =
        typeof meta.stripe_customer_id === "string" && meta.stripe_customer_id.length > 0;
    const email = user?.emailAddresses?.[0]?.emailAddress ?? "";

    return (
        <Container className="py-12 sm:py-16 px-4 max-w-2xl">
            <h1 className="text-3xl sm:text-4xl font-bold text-foreground mb-2">Account</h1>
            <p className="text-muted-foreground mb-8">
                {email ? <>Signed in as {email}.</> : <>Manage your subscription and billing.</>}
            </p>

            {sessionId && <CheckoutReturnStatus key={sessionId} sessionId={sessionId} />}
            {showLegacyStatus && isPremium && (
                <div
                    role="status"
                    className="mb-6 rounded-xl border border-success/30 bg-success/10 p-4 text-sm text-success"
                >
                    <strong className="font-semibold">Subscription confirmed.</strong> You have access
                    to published premium picks.
                </div>
            )}
            {showLegacyStatus && !isPremium && (
                <div role="status" className="mb-6 rounded-xl border border-white/10 bg-card p-4 text-sm text-muted-foreground">
                    Billing confirmation is pending. Refresh this page to check your access.
                </div>
            )}

            <section
                aria-labelledby="subscription-heading"
                className="rounded-2xl border border-white/10 bg-card p-6 sm:p-8"
            >
                <h2 id="subscription-heading" className="text-xl font-bold text-foreground mb-4">
                    Subscription
                </h2>

                <dl className="space-y-3 text-sm">
                    <div className="flex items-center justify-between gap-4">
                        <dt className="text-muted-foreground">Plan</dt>
                        <dd className="font-medium text-foreground">
                            {isPremium ? "Pro Access — $29 / month" : "Free"}
                        </dd>
                    </div>
                    <div className="flex items-center justify-between gap-4">
                        <dt className="text-muted-foreground">Status</dt>
                        <dd className="font-medium text-foreground capitalize">
                            {cancelScheduled && isPremium ? "Cancellation scheduled" : status ? status.replace(/_/g, " ") : "No active subscription"}
                        </dd>
                    </div>
                    {periodEnd && isPremium && (
                        <div className="flex items-center justify-between gap-4">
                            <dt className="text-muted-foreground">{cancelScheduled ? "Access until" : "Renews"}</dt>
                            <dd className="font-medium text-foreground">{formatDate(periodEnd)}</dd>
                        </div>
                    )}
                </dl>

                <div className="mt-6 flex flex-col sm:flex-row gap-3">
                    {hasCustomer ? (
                        <ManageSubscriptionButton />
                    ) : (
                        <Link
                            href="/pricing"
                            className="inline-flex items-center justify-center bg-primary text-primary-foreground font-bold px-6 py-3 rounded-lg hover:bg-amber-500 transition-colors"
                        >
                            View plans
                        </Link>
                    )}
                </div>

                <p className="mt-4 text-xs text-muted-foreground">
                    Manage payment method, update billing, or cancel at any time. Cancellation
                    takes effect at the end of the current billing period.
                </p>
            </section>

            <p className="mt-8 text-xs text-muted-foreground text-center leading-relaxed">
                Subscription provides access to analytics and educational content only. We do not
                accept wagers. See{" "}
                <Link href="/responsible-gaming" className="text-primary hover:underline">
                    Responsible Gaming
                </Link>{" "}
                and{" "}
                <Link href="/terms" className="text-primary hover:underline">
                    Terms
                </Link>
                .
            </p>
        </Container>
    );
}
