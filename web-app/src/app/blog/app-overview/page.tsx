import Link from 'next/link';

export default function AppOverviewPage() {
    return (
        <article className="blog-post prose prose-invert max-w-none">
            <Link href="/blog" className="text-blue-400 hover:text-blue-300 hover:underline mb-4 inline-block">
                ← Back to Blog
            </Link>

            <h1>App Overview &amp; Stats Guide</h1>
            <p className="lead">
                The app shows current pregame picks, the record of picks issued to subscribers, and historical totals model tracking.
            </p>

            <h2>Upcoming picks</h2>
            <p>
                When a selection passes publication checks and its price is current, it appears in Upcoming picks.
                Subscribers can see the selection, sportsbook, line, and quoted price. Availability varies by slate.
                Check the sportsbook price before deciding whether to bet. <Link href="/">Open the home page</Link>.
            </p>

            <h2>Published pick record</h2>
            <p>
                Each issued pick keeps its original published price and uses one unit for grading. ROI divides unit
                profit by units staked, counting pushes in the stake and excluding voids. Win rate counts wins and losses.
                Grading corrections can update the record. The figures are before subscription fees.
                {' '}<Link href="/record">View the published pick record</Link>.
            </p>

            <h2>Historical model tracking</h2>
            <p>
                Model history tracks totals model selections with simulated stakes. Its profit, loss, and ROI describe
                that historical sample. The published record measures the separate subscriber feed.
                {' '}<Link href="/#model-history">Open Model history</Link>.
            </p>

            <h2>Model edge and price</h2>
            <p>
                A model edge is an estimated probability difference relative to the market. The price determines how
                often a selection must win to break even. At −110, that rate is about 52.4% among wins and losses,
                before subscription costs. Held-out results and later same-book quotes help assess the estimate.
            </p>
        </article>
    );
}
