import Link from 'next/link'

export default function AppOverviewPage() {
    return (
        <article className="blog-post prose prose-invert max-w-none">
            <Link href="/blog" className="text-blue-400 hover:text-blue-300 hover:underline mb-4 inline-block">
                ← Back to Blog
            </Link>

            <h1>App Overview & Stats Guide</h1>

            <p className="lead">
                Learn how to navigate the app and understand the statistics shown on the dashboard.
            </p>

            <h2>Dashboard Sections</h2>

            <h3>Overview Tab</h3>

            <p>
                The Overview tab shows your model's overall performance:
            </p>

            <ul>
                <li><strong>Total Predictions</strong>: Number of games we've made predictions for</li>
                <li><strong>Completed Games</strong>: Games that have finished (with results)</li>
                <li><strong>Pending Games</strong>: Upcoming games with predictions</li>
                <li><strong>Recommended Bets</strong>: Predictions with edge above threshold (default 6%)</li>
                <li><strong>Win Rate</strong>: Percentage of recommended bets that won</li>
                <li><strong>ROI</strong>: Return on investment if you bet $100 on each recommended bet</li>
                <li><strong>Net Profit</strong>: Total profit/loss assuming $100 per bet</li>
                <li><strong>Current Bankroll</strong>: Starting bankroll + net profit</li>
            </ul>

            <h3>Predictions Tab</h3>

            <p>
                View all past predictions with detailed results:
            </p>

            <ul>
                <li><strong>Filter by league</strong>: Focus on specific sports</li>
                <li><strong>Filter by model</strong>: Compare different model types (Ensemble, Gradient Boosting, Random Forest)</li>
                <li><strong>Filter by version</strong>: See performance across different model versions</li>
                <li><strong>Won/Lost/Pending</strong>: See bet outcome and profit/loss</li>
            </ul>

            <p>
                <strong>Key Columns:</strong>
            </p>
            <ul>
                <li><strong>Edge</strong>: The difference between our probability and the market's</li>
                <li><strong>Predicted Prob</strong>: Our model's probability for the outcome</li>
                <li><strong>Implied Prob</strong>: Market's implied probability from the odds</li>
                <li><strong>Moneyline</strong>: The actual odds offered by sportsbooks</li>
            </ul>

            <h3>Recommended Tab</h3>

            <p>
                See upcoming games with positive edge:
            </p>

            <ul>
                <li>Only shows games where edge ≥ threshold (default 6%)</li>
                <li>Sorted by edge (highest first)</li>
                <li>Includes both moneyline and over/under recommendations</li>
                <li>Click a game to see available sportsbook odds</li>
            </ul>

            <h3>Over/Under Recommended Tab</h3>

            <p>
                Dedicated view for totals betting:
            </p>

            <ul>
                <li>Shows predicted total vs actual line</li>
                <li>Recommends Over or Under based on prediction</li>
                <li>Displays edge for the recommended bet</li>
            </ul>

            <h2>Understanding Key Metrics</h2>

            <h3>Edge</h3>

            <p>
                The most important metric for value betting:
            </p>

            <div className="bg-gray-900 border border-gray-700 p-4 rounded-lg my-4">
                <p className="font-mono text-sm">
                    Edge = Predicted Probability - Implied Probability
                </p>
            </div>

            <p>
                <strong>Example:</strong>
            </p>
            <ul>
                <li>Our model: 65% chance Lakers win</li>
                <li>Market odds: -150 (60% implied probability)</li>
                <li>Edge: +5% (0.05)</li>
            </ul>

            <p>
                A positive edge suggests value. Higher edge = stronger value.
            </p>

            <h3>Win Rate</h3>

            <p>
                Win Rate = (Winning Bets / Total Completed Bets) × 100%
            </p>

            <p>
                <strong>What's good?</strong> Depends on average odds:
            </p>
            <ul>
                <li>Favorites (negative odds): Need &gt;55% to break even</li>
                <li>Underdogs (positive odds): Can profit with &lt;50% win rate</li>
                <li>50% win rate at -110 odds ≈ break even</li>
            </ul>

            <h3>ROI (Return on Investment)</h3>

            <p>
                ROI = (Net Profit / Total Amount Wagered) × 100%
            </p>

            <p>
                <strong>What's good?</strong>
            </p>
            <ul>
                <li>ROI &gt; 0%: Profitable</li>
                <li>ROI &gt; 5%: Excellent (beating typical sportsbook edge)</li>
                <li>ROI &gt; 10%: Outstanding (rare long-term)</li>
            </ul>

            <h3>Cumulative Profit</h3>

            <p>
                The running total of all bet profits/losses over time. This helps visualize streaks and overall trajectory.
            </p>

            <h2>Tips for Using the App</h2>

            <ol>
                <li><strong>Focus on edge, not just win probability</strong>: A 55% chance at +200 odds can be better value than a 70% chance at -300.</li>
                <li><strong>Use the model filter</strong>: Compare Ensemble vs individual models to see which performs best for your preferred leagues.</li>
                <li><strong>Don't chase losses</strong>: Even high-edge bets lose sometimes. Variance is normal.</li>
                <li><strong>Check sportsbook odds</strong>: Our displayed odds might differ from current market. Always verify before betting.</li>
                <li><strong>Track your own bets</strong>: The dashboard shows model performance, but track your actual bets separately.</li>
            </ol>

            <div className="bg-blue-900/20 border border-blue-600 p-4 my-6 rounded-lg">
                <p className="font-semibold text-blue-200">Pro Tip</p>
                <p className="text-blue-300 mt-1">
                    Use the version filter to see how different model iterations perform. We continuously improve our models, so newer versions (v0.3) typically outperform older ones.
                </p>
            </div>

            <div className="mt-8 pt-8 border-t border-gray-700">
                <h3>Related Articles</h3>
                <ul className="space-y-2">
                    <li><Link href="/blog/moneyline" className="text-blue-400 hover:text-blue-300 hover:underline">What is Moneyline?</Link></li>
                    <li><Link href="/blog/over-under" className="text-blue-400 hover:text-blue-300 hover:underline">What is Over/Under?</Link></li>
                </ul>
            </div>
        </article>
    )
}
