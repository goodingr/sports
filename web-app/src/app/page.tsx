import { HeroStats } from "@/components/home/HeroStats";
import { BetFeed } from "@/components/home/BetFeed";
import { Container } from "@/components/ui/Container";

export default function Home() {
  return (
    <div className="min-h-screen pb-20">
      <HeroStats />

      <Container className="mt-12">
        <div className="max-w-3xl mx-auto">
          <div className="flex items-center justify-between mb-8">
            <h2 className="text-2xl font-bold text-foreground">Latest Recommendations</h2>
            <div className="text-sm text-muted-foreground">
              Showing Over/Under bets
            </div>
          </div>

          {/* Feed */}
          <BetFeed />
        </div>
      </Container>
    </div>
  );
}

