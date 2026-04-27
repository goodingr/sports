import { Container } from "@/components/ui/Container";
import type { Metadata } from "next";

export const metadata: Metadata = {
    title: "Responsible Gaming | Sports Analytics",
    description: "Responsible gambling resources, risk disclosures, and affiliate disclosures.",
};

const LAST_UPDATED = "April 26, 2026";

export default function ResponsibleGamingPage() {
    return (
        <Container className="py-16 max-w-3xl">
            <div className="mb-8 rounded-lg border border-amber-500/30 bg-amber-500/5 p-4 text-sm text-amber-200">
                <strong className="font-semibold">Draft — pending legal review.</strong> This page is a
                working draft and must be reviewed and approved by qualified legal counsel before
                public paid launch.
            </div>

            <h1 className="text-4xl font-bold text-foreground mb-2">Responsible Gaming</h1>
            <p className="text-sm text-muted-foreground mb-10">Last updated: {LAST_UPDATED}</p>

            <div className="space-y-8 text-muted-foreground leading-relaxed">
                <section className="rounded-lg border border-white/10 bg-card p-6">
                    <h2 className="text-2xl font-bold text-foreground mb-3">Important Disclaimer</h2>
                    <ul className="list-disc pl-6 space-y-2">
                        <li>
                            Sports Analytics provides{" "}
                            <strong className="text-foreground">analytics and educational content only</strong>
                            . We are not a sportsbook and do not accept wagers.
                        </li>
                        <li>
                            <strong className="text-foreground">No outcome is guaranteed.</strong> Model
                            output, picks, edges, and historical performance metrics are not promises
                            or assurances of any future result.
                        </li>
                        <li>
                            <strong className="text-foreground">Betting involves real financial risk</strong>
                            , including the loss of the entire amount wagered. Never bet more than you
                            can afford to lose.
                        </li>
                        <li>
                            You must be of legal age in your jurisdiction (and in any case at least 18,
                            or 21 where required) and you are responsible for{" "}
                            <strong className="text-foreground">complying with all local laws</strong>{" "}
                            governing sports betting and gambling.
                        </li>
                    </ul>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">Recognizing Problem Gambling</h2>
                    <p className="mb-3">
                        Gambling can become harmful. Warning signs include:
                    </p>
                    <ul className="list-disc pl-6 space-y-1">
                        <li>Spending more time or money on gambling than intended.</li>
                        <li>Chasing losses or borrowing money to gamble.</li>
                        <li>Lying to family or friends about gambling activity.</li>
                        <li>Feeling anxious, irritable, or depressed when not gambling.</li>
                        <li>Gambling interfering with work, school, or relationships.</li>
                    </ul>
                    <p className="mt-3">
                        If any of these apply to you or someone you know, please reach out to one of
                        the resources below.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">Help &amp; Support Resources</h2>
                    <p className="mb-4">
                        These free, confidential services are available 24/7:
                    </p>
                    <div className="space-y-4">
                        <ResourceCard
                            name="National Council on Problem Gambling (USA)"
                            phone="1-800-GAMBLER (1-800-426-2537)"
                            text="Text 800GAM"
                            url="https://www.ncpgambling.org/"
                        />
                        <ResourceCard
                            name="SAMHSA National Helpline (USA)"
                            phone="1-800-662-4357"
                            url="https://www.samhsa.gov/find-help/national-helpline"
                        />
                        <ResourceCard
                            name="Gamblers Anonymous"
                            url="https://www.gamblersanonymous.org/"
                        />
                        <ResourceCard
                            name="GamCare (UK)"
                            phone="0808 8020 133"
                            url="https://www.gamcare.org.uk/"
                        />
                        <ResourceCard
                            name="Gambling Therapy (International)"
                            url="https://www.gamblingtherapy.org/"
                        />
                    </div>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">Self-Exclusion &amp; Limits</h2>
                    <p>
                        Most licensed sportsbooks offer self-exclusion programs and tools to set
                        deposit, wager, or session-time limits. If you are concerned about your
                        gambling, contact your sportsbook&rsquo;s support to enroll, or visit a
                        statewide self-exclusion program where available.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">Affiliate &amp; Sportsbook Link Disclosure</h2>
                    <p>
                        Sports Analytics may include links to third-party sportsbooks. Some of these
                        links are <strong className="text-foreground">affiliate links</strong>: if you
                        click through and create an account or place a wager, we may receive a
                        commission or referral fee from the sportsbook at no additional cost to you.
                    </p>
                    <p className="mt-3">
                        Affiliate compensation does not influence our model output, picks, or edge
                        calculations. We do not control and are not responsible for the odds, terms,
                        promotions, or practices of any third-party sportsbook. You should review the
                        terms and responsible-gaming policies of any sportsbook before using it.
                    </p>
                </section>

                <section>
                    <h2 className="text-2xl font-bold text-foreground mb-3">Healthy Habits</h2>
                    <ul className="list-disc pl-6 space-y-1">
                        <li>Set a budget before you bet, and stop when you reach it.</li>
                        <li>Treat any money wagered as entertainment expense, not income.</li>
                        <li>Do not chase losses.</li>
                        <li>Take regular breaks; do not gamble while upset, intoxicated, or tired.</li>
                        <li>Balance gambling with other activities you enjoy.</li>
                    </ul>
                </section>
            </div>
        </Container>
    );
}

function ResourceCard({
    name,
    phone,
    text,
    url,
}: {
    name: string;
    phone?: string;
    text?: string;
    url: string;
}) {
    return (
        <div className="rounded-lg border border-white/10 bg-card p-4">
            <a
                href={url}
                target="_blank"
                rel="noopener noreferrer"
                className="font-semibold text-foreground hover:text-primary transition-colors"
            >
                {name}
            </a>
            <div className="mt-1 text-sm space-y-0.5">
                {phone && <div>Call: <span className="text-foreground">{phone}</span></div>}
                {text && <div>{text}</div>}
            </div>
        </div>
    );
}
