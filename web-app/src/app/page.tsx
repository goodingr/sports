import { BetFeed } from "@/components/home/BetFeed";
import { PublishedRecord } from "@/components/record/PublishedRecord";
import { Container } from "@/components/ui/Container";

export default function Home() {
  return (
    <div className="min-h-screen pb-20">
      <Container className="mt-12">
        <div className="max-w-3xl mx-auto space-y-8">
          <PublishedRecord summary />
          <div id="upcoming-picks" className="scroll-mt-20">
            <div id="model-history" className="scroll-mt-20">
              <BetFeed />
            </div>
          </div>
        </div>
      </Container>
    </div>
  );
}

