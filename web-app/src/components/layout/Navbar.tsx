import Link from 'next/link';
import { Container } from '@/components/ui/Container';
import { Trophy } from 'lucide-react';
import { SignedIn, SignedOut, UserButton } from '@clerk/nextjs';

export function Navbar() {
    return (
        <nav className="border-b border-white/10 bg-black/50 backdrop-blur-md sticky top-0 z-50">
            <Container className="flex h-16 items-center justify-between">
                <Link href="/" className="flex items-center gap-2 font-bold text-xl text-primary">
                    <Trophy className="h-6 w-6" />
                    <span>Sports Analytics</span>
                </Link>

                <div className="hidden md:flex items-center gap-8 text-sm font-medium text-muted-foreground">
                    <Link href="/" className="hover:text-foreground transition-colors">Home</Link>
                    <Link href="/history" className="hover:text-foreground transition-colors">History</Link>
                    <Link href="/pricing" className="hover:text-foreground transition-colors">Pricing</Link>
                </div>

                <div className="flex items-center gap-4">
                    <SignedOut>
                        <Link href="/login" className="text-sm font-medium hover:text-primary transition-colors">
                            Login
                        </Link>
                        <Link
                            href="/signup"
                            className="bg-primary text-primary-foreground px-4 py-2 rounded-full text-sm font-bold hover:bg-amber-500 transition-colors"
                        >
                            Get Started
                        </Link>
                    </SignedOut>
                    <SignedIn>
                        <UserButton afterSignOutUrl="/" />
                    </SignedIn>
                </div>
            </Container>
        </nav>
    );
}
