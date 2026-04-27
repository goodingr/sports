import { Container } from "@/components/ui/Container";
import type { Metadata } from "next";

export const metadata: Metadata = {
    title: "Terms of Service | Sports Analytics",
    description: "Terms of Service for Sports Analytics. Analytics and educational content only.",
};

const LAST_UPDATED = "April 26, 2026";

export default function TermsPage() {
    return (
        <Container className="py-16 max-w-3xl">
            <div className="mb-8 rounded-lg border border-amber-500/30 bg-amber-500/5 p-4 text-sm text-amber-200">
                <strong className="font-semibold">Draft — pending legal review.</strong> This document
                is a working draft and must be reviewed and approved by qualified legal counsel before
                public paid launch.
            </div>

            <h1 className="text-4xl font-bold text-foreground mb-2">Terms of Service</h1>
            <p className="text-sm text-muted-foreground mb-10">Last updated: {LAST_UPDATED}</p>

            <div className="space-y-8 text-muted-foreground leading-relaxed">
                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">1. Acceptance of Terms</h2>
                    <p>
                        By accessing or using Sports Analytics (the &ldquo;Service&rdquo;), you agree to
                        these Terms of Service. If you do not agree, do not use the Service.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">2. Nature of the Service</h2>
                    <p>
                        Sports Analytics provides statistical analysis, model output, and educational
                        content related to sporting events. The Service is offered for{" "}
                        <strong className="text-foreground">informational and educational purposes only</strong>.
                        We are not a sportsbook, casino, broker, or licensed financial advisor. We do not
                        accept, place, or facilitate wagers.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">3. No Guaranteed Outcomes</h2>
                    <p>
                        Past performance is not indicative of future results. No content on the Service
                        is a guarantee, promise, or assurance of any specific outcome, profit, or return.
                        All betting and wagering activity carries inherent risk, including the risk of
                        losing the entire amount wagered. You are solely responsible for any decisions
                        you make based on information from the Service.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">4. Eligibility &amp; Legal Compliance</h2>
                    <p>
                        You must be at least the legal age of majority in your jurisdiction (and in any
                        case no younger than 18, or 21 where required) to use the Service. You are
                        responsible for ensuring that any use of sports betting, gambling, or related
                        activity is legal in your jurisdiction. The Service is not directed at users in
                        jurisdictions where such content is prohibited. We make no representation that
                        the Service is appropriate or available in any particular location.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">5. Accounts &amp; Subscriptions</h2>
                    <p>
                        Some features require a paid subscription. You are responsible for maintaining
                        the confidentiality of your account credentials and for all activity under your
                        account. Subscriptions renew automatically unless canceled. You may cancel at
                        any time; cancellation takes effect at the end of the current billing period.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">6. Refunds</h2>
                    <p>
                        Subscription fees are generally non-refundable except where required by
                        applicable law. Refund requests will be reviewed on a case-by-case basis.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">7. Third-Party Sportsbooks &amp; Affiliate Links</h2>
                    <p>
                        The Service may include links to third-party sportsbooks or other operators. We
                        do not control and are not responsible for the content, terms, odds, payouts, or
                        practices of any third-party site. Some links may be affiliate links that
                        generate revenue for us if you sign up or place a wager. See our{" "}
                        <a href="/responsible-gaming" className="text-primary hover:underline">
                            Responsible Gaming
                        </a>{" "}
                        page for affiliate disclosures.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">8. Acceptable Use</h2>
                    <p>
                        You agree not to: (a) reverse engineer, scrape, or resell Service output; (b)
                        share account credentials; (c) use the Service in violation of any law; or (d)
                        interfere with the operation of the Service.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">9. Disclaimers</h2>
                    <p>
                        THE SERVICE IS PROVIDED &ldquo;AS IS&rdquo; AND &ldquo;AS AVAILABLE&rdquo;
                        WITHOUT WARRANTIES OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
                        MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT. WE DO
                        NOT WARRANT THAT THE SERVICE WILL BE ACCURATE, ERROR-FREE, OR UNINTERRUPTED.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">10. Limitation of Liability</h2>
                    <p>
                        TO THE MAXIMUM EXTENT PERMITTED BY LAW, SPORTS ANALYTICS, ITS OFFICERS,
                        EMPLOYEES, AND AFFILIATES SHALL NOT BE LIABLE FOR ANY INDIRECT, INCIDENTAL,
                        SPECIAL, CONSEQUENTIAL, OR EXEMPLARY DAMAGES, INCLUDING LOSS OF PROFITS, BETS,
                        OR DATA, ARISING FROM YOUR USE OF THE SERVICE.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">11. Changes to These Terms</h2>
                    <p>
                        We may update these Terms from time to time. Material changes will be posted on
                        this page with an updated &ldquo;Last updated&rdquo; date. Continued use of the
                        Service after changes constitutes acceptance.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">12. Contact</h2>
                    <p>
                        Questions about these Terms can be sent to{" "}
                        <a href="mailto:support@sportsanalytics.example" className="text-primary hover:underline">
                            support@sportsanalytics.example
                        </a>
                        .
                    </p>
                </section>
            </div>
        </Container>
    );
}
