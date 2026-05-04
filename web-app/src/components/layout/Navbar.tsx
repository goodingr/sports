'use client';

import { useState } from 'react';
import Link from 'next/link';
import { Container } from '@/components/ui/Container';
import { Trophy, Menu, X } from 'lucide-react';
import { SignedIn, SignedOut, UserButton } from '@clerk/nextjs';

const navLinks = [
    { href: '/', label: 'Home' },
    { href: '/blog', label: 'Blog' },
    { href: '/pricing', label: 'Pricing' },
];

export function Navbar() {
    const [mobileOpen, setMobileOpen] = useState(false);

    const closeMobile = () => setMobileOpen(false);

    return (
        <nav className="border-b border-white/10 bg-black/50 backdrop-blur-md sticky top-0 z-50">
            <Container className="flex h-16 items-center justify-between gap-2">
                <Link
                    href="/"
                    className="flex items-center gap-2 font-bold text-base sm:text-xl text-primary shrink-0"
                    onClick={closeMobile}
                >
                    <Trophy className="h-5 w-5 sm:h-6 sm:w-6" aria-hidden="true" />
                    <span>Sports Analytics</span>
                </Link>

                <div className="hidden md:flex items-center gap-8 text-sm font-medium text-muted-foreground">
                    {navLinks.map((l) => (
                        <Link
                            key={l.href}
                            href={l.href}
                            className="hover:text-foreground transition-colors"
                        >
                            {l.label}
                        </Link>
                    ))}
                </div>

                <div className="flex items-center gap-2 sm:gap-4 shrink-0">
                    <SignedOut>
                        <Link
                            href="/login"
                            className="hidden sm:inline text-sm font-medium hover:text-primary transition-colors"
                        >
                            Login
                        </Link>
                        <Link
                            href="/signup"
                            className="bg-primary text-primary-foreground px-3 sm:px-4 py-2 rounded-full text-xs sm:text-sm font-bold hover:bg-amber-500 transition-colors"
                        >
                            Get Started
                        </Link>
                    </SignedOut>
                    <SignedIn>
                        <Link
                            href="/account"
                            className="hidden sm:inline text-sm font-medium hover:text-primary transition-colors"
                        >
                            Account
                        </Link>
                        <UserButton afterSignOutUrl="/" />
                    </SignedIn>
                    <button
                        type="button"
                        className="md:hidden p-2 -mr-2 text-muted-foreground hover:text-foreground transition-colors"
                        aria-label={mobileOpen ? 'Close navigation menu' : 'Open navigation menu'}
                        aria-expanded={mobileOpen}
                        aria-controls="mobile-nav"
                        onClick={() => setMobileOpen((v) => !v)}
                    >
                        {mobileOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
                    </button>
                </div>
            </Container>

            {mobileOpen && (
                <div
                    id="mobile-nav"
                    className="md:hidden border-t border-white/10 bg-black/95 backdrop-blur-md"
                >
                    <Container className="py-2">
                        <ul className="flex flex-col text-sm font-medium">
                            {navLinks.map((l) => (
                                <li key={l.href}>
                                    <Link
                                        href={l.href}
                                        onClick={closeMobile}
                                        className="block py-3 text-muted-foreground hover:text-foreground transition-colors"
                                    >
                                        {l.label}
                                    </Link>
                                </li>
                            ))}
                            <SignedOut>
                                <li>
                                    <Link
                                        href="/login"
                                        onClick={closeMobile}
                                        className="block py-3 text-muted-foreground hover:text-foreground transition-colors"
                                    >
                                        Login
                                    </Link>
                                </li>
                            </SignedOut>
                            <SignedIn>
                                <li>
                                    <Link
                                        href="/account"
                                        onClick={closeMobile}
                                        className="block py-3 text-muted-foreground hover:text-foreground transition-colors"
                                    >
                                        Account
                                    </Link>
                                </li>
                            </SignedIn>
                        </ul>
                    </Container>
                </div>
            )}
        </nav>
    );
}
