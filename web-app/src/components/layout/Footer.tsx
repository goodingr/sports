import { Container } from '@/components/ui/Container';

export function Footer() {
    return (
        <footer className="border-t border-white/10 bg-black py-12 text-muted-foreground">
            <Container>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
                    <div className="col-span-1 md:col-span-2">
                        <h3 className="font-bold text-foreground mb-4 text-lg">Sports Analytics</h3>
                        <p className="text-sm max-w-xs">
                            Premium sports betting recommendations powered by advanced machine learning models.
                        </p>
                    </div>
                    <div>
                        <h3 className="font-bold text-foreground mb-4">Product</h3>
                        <ul className="space-y-2 text-sm">
                            <li><a href="#" className="hover:text-primary transition-colors">Features</a></li>
                            <li><a href="/pricing" className="hover:text-primary transition-colors">Pricing</a></li>
                            <li><a href="/history" className="hover:text-primary transition-colors">Results</a></li>
                        </ul>
                    </div>
                    <div>
                        <h3 className="font-bold text-foreground mb-4">Legal</h3>
                        <ul className="space-y-2 text-sm">
                            <li><a href="#" className="hover:text-primary transition-colors">Terms</a></li>
                            <li><a href="#" className="hover:text-primary transition-colors">Privacy</a></li>
                        </ul>
                    </div>
                </div>
                <div className="mt-12 pt-8 border-t border-white/10 text-center text-sm">
                    &copy; {new Date().getFullYear()} Sports Analytics. All rights reserved.
                </div>
            </Container>
        </footer>
    );
}
