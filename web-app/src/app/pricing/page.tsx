import { currentUser } from '@clerk/nextjs/server';
import Link from 'next/link';
import { Check } from 'lucide-react';
import { Container } from '@/components/ui/Container';
import { configuredStripeScope, hasScopedPremium } from '@/lib/billing';
import { CheckoutButton } from './CheckoutButton';

export const dynamic = 'force-dynamic';

export default async function Page() {
    const scope = configuredStripeScope();
    const user = scope ? await currentUser() : null;
    const isPremium = hasScopedPremium(user?.publicMetadata ?? {}, scope);
    return (
        <Container className="py-12 sm:py-20 px-4">
            <div className="text-center mb-10 sm:mb-16">
                <h1 className="text-3xl sm:text-4xl font-bold text-foreground mb-4">
                    Simple, Transparent Pricing
                </h1>
                <p className="text-base sm:text-xl text-muted-foreground">
                    Access pregame picks that pass our model validation checks. Pick availability varies by slate.
                </p>
                <Link href="/record" className="mt-3 inline-block text-sm text-primary hover:underline">View the published pick record</Link>
            </div>

            <div className="max-w-md mx-auto bg-card border border-primary/20 rounded-2xl overflow-hidden shadow-2xl shadow-primary/10">
                <div className="p-6 sm:p-8">
                    <h2 className="text-xl sm:text-2xl font-bold text-foreground mb-2">Pro Access</h2>
                    <div className="flex items-baseline gap-1 mb-6">
                        <span className="text-3xl sm:text-4xl font-bold text-foreground">$29</span>
                        <span className="text-muted-foreground">/month</span>
                    </div>

                    <ul className="space-y-3 sm:space-y-4 mb-8">
                        <Feature text="Access to published pregame picks" />
                        <Feature text="Sportsbook prices and model edge estimates" />
                        <Feature text="Cancel Anytime" />
                    </ul>

                    <CheckoutButton isPremium={isPremium} />
                </div>
            </div>

            <div className="max-w-md mx-auto mt-8 text-xs text-muted-foreground text-center leading-relaxed px-2">
                <p>
                    Subscription provides access to analytics and educational content only. We do
                    not accept wagers. No outcome is guaranteed; betting involves real financial
                    risk. You must be of legal age and follow the laws of your jurisdiction. See{' '}
                    <Link href="/responsible-gaming" className="text-primary hover:underline">
                        Responsible Gaming
                    </Link>{' '}
                    and{' '}
                    <Link href="/terms" className="text-primary hover:underline">
                        Terms
                    </Link>
                    .
                </p>
            </div>
        </Container>
    );
}

function Feature({ text }: { text: string }) {
    return (
        <li className="flex items-center gap-3">
            <div className="p-1 bg-success/10 rounded-full">
                <Check className="h-4 w-4 text-success" aria-hidden="true" />
            </div>
            <span className="text-foreground">{text}</span>
        </li>
    );
}
