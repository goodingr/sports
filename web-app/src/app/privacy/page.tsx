import { Container } from "@/components/ui/Container";
import type { Metadata } from "next";

export const metadata: Metadata = {
    title: "Privacy Policy | Sports Analytics",
    description: "How Sports Analytics collects, uses, and protects your information.",
};

const LAST_UPDATED = "April 26, 2026";

export default function PrivacyPage() {
    return (
        <Container className="py-16 max-w-3xl">
            <div className="mb-8 rounded-lg border border-amber-500/30 bg-amber-500/5 p-4 text-sm text-amber-200">
                <strong className="font-semibold">Draft — pending legal review.</strong> This document
                is a working draft and must be reviewed and approved by qualified legal counsel before
                public paid launch.
            </div>

            <h1 className="text-4xl font-bold text-foreground mb-2">Privacy Policy</h1>
            <p className="text-sm text-muted-foreground mb-10">Last updated: {LAST_UPDATED}</p>

            <div className="space-y-8 text-muted-foreground leading-relaxed">
                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">1. Introduction</h2>
                    <p>
                        This Privacy Policy explains how Sports Analytics (&ldquo;we&rdquo;,
                        &ldquo;us&rdquo;) collects, uses, and shares information when you use our
                        website and services.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">2. Information We Collect</h2>
                    <ul className="list-disc pl-6 space-y-2">
                        <li>
                            <strong className="text-foreground">Account information:</strong> name,
                            email, and authentication identifiers provided through our identity
                            provider (Clerk).
                        </li>
                        <li>
                            <strong className="text-foreground">Billing information:</strong> processed
                            by our payment provider (Stripe). We do not store full card numbers on our
                            servers.
                        </li>
                        <li>
                            <strong className="text-foreground">Usage data:</strong> pages viewed,
                            features used, device and browser metadata, IP address, and approximate
                            location derived from IP.
                        </li>
                        <li>
                            <strong className="text-foreground">Communications:</strong> messages you
                            send to support.
                        </li>
                    </ul>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">3. How We Use Information</h2>
                    <ul className="list-disc pl-6 space-y-2">
                        <li>To operate, maintain, and improve the Service.</li>
                        <li>To process subscriptions, payments, and refunds.</li>
                        <li>To authenticate users and prevent fraud or abuse.</li>
                        <li>To communicate with you about your account or service updates.</li>
                        <li>To comply with legal obligations.</li>
                    </ul>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">4. Cookies &amp; Similar Technologies</h2>
                    <p>
                        We use cookies and similar technologies to keep you signed in, remember
                        preferences, and measure usage. You can control cookies through your browser
                        settings; disabling them may degrade some functionality.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">5. How We Share Information</h2>
                    <p>We share information only with:</p>
                    <ul className="list-disc pl-6 space-y-2 mt-2">
                        <li>
                            Service providers acting on our behalf (e.g., authentication, payment
                            processing, hosting, analytics, email delivery), under contractual
                            confidentiality obligations.
                        </li>
                        <li>
                            Authorities or other parties when required by law, subpoena, or to protect
                            rights and safety.
                        </li>
                        <li>
                            A successor entity in connection with a merger, acquisition, or sale of
                            assets.
                        </li>
                    </ul>
                    <p className="mt-2">We do not sell personal information.</p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">6. Data Retention</h2>
                    <p>
                        We retain personal information for as long as your account is active and as
                        needed to provide the Service, comply with legal obligations, resolve disputes,
                        and enforce agreements.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">7. Your Rights</h2>
                    <p>
                        Depending on your jurisdiction (e.g., GDPR, CCPA), you may have rights to
                        access, correct, delete, or port your personal information, and to object to
                        or restrict certain processing. To exercise these rights, contact{" "}
                        <a href="mailto:privacy@sportsanalytics.example" className="text-primary hover:underline">
                            privacy@sportsanalytics.example
                        </a>
                        .
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">8. Children</h2>
                    <p>
                        The Service is not intended for and may not be used by anyone under the legal
                        age of majority in their jurisdiction (and in any case no younger than 18). We
                        do not knowingly collect information from minors.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">9. Security</h2>
                    <p>
                        We use reasonable administrative, technical, and physical safeguards to protect
                        information. No method of transmission or storage is perfectly secure, and we
                        cannot guarantee absolute security.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">10. International Transfers</h2>
                    <p>
                        Your information may be processed in countries other than the one in which you
                        live. Where required, we use appropriate safeguards for cross-border transfers.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">11. Changes to This Policy</h2>
                    <p>
                        We may update this Privacy Policy from time to time. Material changes will be
                        posted on this page with an updated &ldquo;Last updated&rdquo; date.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">12. Contact</h2>
                    <p>
                        Questions about this Privacy Policy can be sent to{" "}
                        <a href="mailto:privacy@sportsanalytics.example" className="text-primary hover:underline">
                            privacy@sportsanalytics.example
                        </a>
                        .
                    </p>
                </section>
            </div>
        </Container>
    );
}
