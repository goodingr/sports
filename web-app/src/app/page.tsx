import { HeroStats } from "@/components/home/HeroStats";
import { BetFeed } from "@/components/home/BetFeed";
import { Container } from "@/components/ui/Container";

export default function Home() {
  return (
    <div className="min-h-screen pb-20">
      <HeroStats />

      <Container className="mt-12">
        <div className="max-w-3xl mx-auto">


          {/* Feed */}
          <BetFeed />
        </div>
      </Container>
    </div>
  );
}

