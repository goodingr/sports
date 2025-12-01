import Link from 'next/link'

export default function OverUnderPage() {
    return (
        <article className="blog-post prose prose-invert max-w-none">
            <Link href="/blog" className="text-blue-400 hover:text-blue-300 hover:underline mb-4 inline-block">
                ← Back to Blog
            </Link>

            <h1>What is Over/Under Betting?</h1>

            <p className="lead">
                Over/Under betting, also called "totals" betting, is a wager on the combined score of both teams in a game. You're betting on whether the total points scored will be over or under a number set by the sportsbook.
            </p>

            <h2>How Totals Work</h2>

            <p>
                The sportsbook sets a "line" representing the predicted total points. You bet whether the actual total will be higher (over) or lower (under) than this line.
            </p>

            <h2>Example</h2>

            <div className="bg-gray-900 border border-gray-700 p-4 rounded-lg my-4">
                <p className="font-semibold mb-2">Lakers vs Warriors - Total: 215.5</p>
                <ul className="list-none space-y-1">
                    <li>Over 215.5: <span className="font-mono">-110</span></li>
                    <li>Under 215.5: <span className="font-mono">-110</span></li>
                </ul>
            </div>

            <p>
                In this example:
            </p>
            <ul>
                <li>If the final score is Lakers 110, Warriors 108 (total: 218), the <strong>Over</strong> wins</li>
                <li>If the final score is Lakers 105, Warriors 107 (total: 212), the <strong>Under</strong> wins</li>
                <li>The .5 ensures there's no push (tie)</li>
            </ul>

            <h2>Reading Total Lines</h2>

            <p>
                Common notation you'll see:
            </p>

            <ul>
                <li><strong>O 215.5 (-110)</strong>: Bet over 215.5 total points, risk $110 to win $100</li>
                <li><strong>U 215.5 (-110)</strong>: Bet under 215.5 total points, risk $110 to win $100</li>
            </ul>

            <h2>What Affects Totals?</h2>

            <p>
                Several factors influence the total line:
            </p>

            <ul>
                <li><strong>Pace of play</strong>: Fast-paced teams drive higher totals</li>
                <li><strong>Defense</strong>: Strong defensive teams lead to lower totals</li>
                <li><strong>Weather</strong>: (For outdoor sports) Wind, rain can lower totals</li>
                <li><strong>Injuries</strong>: Key player absences affect scoring</li>
                <li><strong>Recent trends</strong>: Teams on scoring streaks or slumps</li>
            </ul>

            <h2>Our Prediction Approach</h2>

            <p>
                Our system analyzes these factors and more to predict the actual total score. When we identify a significant difference between our prediction and the sportsbook's line, we flag it as a potential betting opportunity.
            </p>

            <div className="mt-8 pt-8 border-t border-gray-700">
                <h3>Related Articles</h3>
                <ul className="space-y-2">
                    <li><Link href="/blog/moneyline" className="text-blue-400 hover:text-blue-300 hover:underline">What is Moneyline?</Link></li>
                    <li><Link href="/blog/betting-types" className="text-blue-400 hover:text-blue-300 hover:underline">Different Types of Sports Bets</Link></li>
                </ul>
            </div>
        </article>
    )
}
