import Link from 'next/link'

export default function BettingTypesPage() {
    return (
        <article className="blog-post prose prose-invert max-w-none">
            <Link href="/blog" className="text-blue-400 hover:text-blue-300 hover:underline mb-4 inline-block">
                ← Back to Blog
            </Link>

            <h1>Different Types of Sports Bets</h1>

            <p className="lead">
                Sports betting offers many ways to wager beyond just picking a winner. Here&apos;s a comprehensive guide to the most common bet types.
            </p>

            <h2>1. Moneyline</h2>

            <p>
                <strong>What it is:</strong> Betting on which team will win the game outright.
            </p>

            <p>
                <strong>Example:</strong> Lakers -150, Warriors +130
            </p>

            <p>
                <strong>Best for:</strong> Games where you have a strong opinion on the winner, regardless of margin.
            </p>

            <p>
                <Link href="/blog/moneyline" className="text-blue-400 hover:text-blue-300 hover:underline">Learn more about moneyline betting →</Link>
            </p>

            <h2>2. Point Spread</h2>

            <p>
                <strong>What it is:</strong> Betting on the margin of victory. The favorite must win by more than the spread, the underdog must lose by less (or win).
            </p>

            <p>
                <strong>Example:</strong> Lakers -5.5 (-110), Warriors +5.5 (-110)
            </p>

            <p>
                <strong>How it works:</strong> If you bet Lakers -5.5, they must win by 6+ points. If you bet Warriors +5.5, they must lose by 5 or fewer points (or win).
            </p>

            <p>
                <strong>Best for:</strong> Games with a clear favorite where you want more balanced odds.
            </p>

            <h2>3. Over/Under (Totals)</h2>

            <p>
                <strong>What it is:</strong> Betting on whether the combined score will be over or under a set number.
            </p>

            <p>
                <strong>Example:</strong> Total 215.5 - Over -110, Under -110
            </p>

            <p>
                <strong>Best for:</strong> When you have an opinion on game pace but not the winner.
            </p>

            <p>
                <Link href="/blog/over-under" className="text-blue-400 hover:text-blue-300 hover:underline">Learn more about over/under betting →</Link>
            </p>

            <h2>4. Parlay</h2>

            <p>
                <strong>What it is:</strong> Combining multiple bets into one. All picks must win for the parlay to pay out.
            </p>

            <p>
                <strong>Example:</strong> Lakers ML + Over 215.5 + Warriors +5.5 (3-leg parlay)
            </p>

            <p>
                <strong>Pros:</strong> Higher potential payout from smaller stakes.
            </p>

            <p>
                <strong>Cons:</strong> Much harder to win - one loss kills the entire bet.
            </p>

            <p>
                <strong>Best for:</strong> Recreational betting with small stakes, not recommended for serious betting.
            </p>

            <h2>5. Prop Bets (Propositions)</h2>

            <p>
                <strong>What it is:</strong> Betting on specific events within a game (player stats, first scorer, etc).
            </p>

            <p>
                <strong>Examples:</strong>
            </p>
            <ul>
                <li>LeBron James over 27.5 points</li>
                <li>Stephen Curry to score first basket</li>
                <li>Total rebounds over 95.5</li>
            </ul>

            <p>
                <strong>Best for:</strong> Adding excitement to specific player performances or game events.
            </p>

            <h2>6. Live Betting (In-Play)</h2>

            <p>
                <strong>What it is:</strong> Placing bets during a game as odds update in real-time.
            </p>

            <p>
                <strong>Example:</strong> Betting on Lakers ML after they fall behind early at better odds.
            </p>

            <p>
                <strong>Pros:</strong> React to game flow, find value based on what you&apos;re watching.
            </p>

            <p>
                <strong>Cons:</strong> Requires watching the game, odds move quickly.
            </p>

            <p>
                <strong>Best for:</strong> Experienced bettors who watch games closely.
            </p>

            <h2>What We Focus On</h2>

            <p>
                Our prediction system specializes in <strong>moneyline</strong> and <strong>over/under (totals)</strong> betting. We focus on these because:
            </p>

            <ul>
                <li>They offer the most consistent data for machine learning</li>
                <li>Easier to model than props or parlays</li>
                <li>Available for all major sports</li>
                <li>Lower variance than exotic bets</li>
            </ul>

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
