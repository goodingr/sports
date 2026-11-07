import { ClerkProvider, SignedIn, UserButton } from "@clerk/nextjs";
import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import Link from "next/link";
import "./globals.css";
import { Navbar } from "@/components/layout/Navbar";
import { Footer } from "@/components/layout/Footer";
import { Container } from "@/components/ui/Container";
import { isPilotOnly } from "@/lib/pilot";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Sports Analytics | Premium Betting Insights",
  description: "AI-powered sports betting recommendations and analytics.",
  ...(isPilotOnly() ? { robots: { index: false, follow: false } } : {}),
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const pilotOnly = isPilotOnly();
  return (
    <ClerkProvider>
      <html lang="en">
        <body
          className={`${geistSans.variable} ${geistMono.variable} antialiased min-h-screen flex flex-col`}
        >
          {pilotOnly ? (
            <header className="border-b border-white/10 bg-black/50">
              <Container className="flex h-16 items-center justify-between gap-2">
                <Link href="/interest" className="text-lg font-bold text-primary">Sports Analytics</Link>
                <SignedIn><UserButton afterSignOutUrl="/interest" /></SignedIn>
              </Container>
            </header>
          ) : <Navbar />}
          <main className="flex-1">
            {children}
          </main>
          {pilotOnly ? (
            <footer className="border-t border-white/10 bg-black py-6 text-sm text-muted-foreground">
              <Container className="flex flex-wrap gap-4">
                <Link href="/terms" className="hover:text-primary">Terms of Service</Link>
                <Link href="/privacy" className="hover:text-primary">Privacy Policy</Link>
                <Link href="/responsible-gaming" className="hover:text-primary">Responsible Gaming</Link>
              </Container>
            </footer>
          ) : <Footer />}
        </body>
      </html>
    </ClerkProvider>
  );
}
