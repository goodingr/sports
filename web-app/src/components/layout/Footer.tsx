import { Container } from '@/components/ui/Container';

export function Footer() {
    return (
        <footer className="border-t border-white/10 bg-black py-12 text-muted-foreground">
            <Container>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
                    <div className="col-span-1 md:col-span-2">
                        <h3 className="font-bold text-foreground mb-4 text-lg">Sports Analytics</h3>
                        <p className="text-sm max-w-xs">
                            Sports betting analytics and educational content powered by machine
                            learning models. For informational purposes only — no outcomes are
                            guaranteed.
                        </p>
                    </div>
                    <div>
                        <h3 className="font-bold text-foreground mb-4">Product</h3>
                        <ul className="space-y-2 text-sm">
                            <li><a href="/pricing" className="hover:text-primary transition-colors">Pricing</a></li>
                            <li><a href="/blog" className="hover:text-primary transition-colors">Blog</a></li>
                        </ul>
                    </div>
                    <div>
                        <h3 className="font-bold text-foreground mb-4">Legal</h3>
                        <ul className="space-y-2 text-sm">
                            <li><a href="/terms" className="hover:text-primary transition-colors">Terms of Service</a></li>
                            <li><a href="/privacy" className="hover:text-primary transition-colors">Privacy Policy</a></li>
                            <li><a href="/responsible-gaming" className="hover:text-primary transition-colors">Responsible Gaming</a></li>
                        </ul>
                    </div>
                </div>

                <div className="mt-10 pt-6 border-t border-white/10 space-y-3 text-xs leading-relaxed">
                    <p>
                        <strong className="text-foreground">Disclaimer:</strong> Sports Analytics
                        provides analytics and educational content only. We are not a sportsbook and
                        do not accept wagers. Past performance does not guarantee future results, and
                        no information on this site is a guarantee of any outcome. Betting involves
                        real financial risk, including loss of the amount wagered.
                    </p>
                    <p>
                        You must be of legal age (18+, or 21+ where required) and responsible for
                        complying with all laws in your jurisdiction. If you or someone you know has
                        a gambling problem, help is available 24/7 — call{" "}
                        <strong className="text-foreground">1-800-GAMBLER</strong> or visit our{" "}
                        <a href="/responsible-gaming" className="text-primary hover:underline">
                            Responsible Gaming
                        </a>{" "}
                        page.
                    </p>
                    <p>
                        Some links to sportsbooks may be affiliate links. We may earn a commission if
                        you sign up or place a wager. See our{" "}
                        <a href="/responsible-gaming" className="text-primary hover:underline">
                            affiliate disclosure
                        </a>
                        .
                    </p>
                </div>

                <div className="mt-8 pt-6 border-t border-white/10 flex flex-col md:flex-row md:items-center md:justify-between gap-2 text-xs">
                    <span>&copy; {new Date().getFullYear()} Sports Analytics. All rights reserved.</span>
                    <span className="text-muted-foreground/70">
                        Legal pages pending counsel review prior to public paid launch.
                    </span>
                </div>
            </Container>
        </footer>
    );
}
