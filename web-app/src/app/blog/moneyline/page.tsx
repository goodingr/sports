import Link from 'next/link'

export default function MoneylinePage() {
    return (
        <article className="blog-post prose prose-invert max-w-none">
            <Link href="/blog" className="text-blue-400 hover:text-blue-300 hover:underline mb-4 inline-block">
                ← Back to Blog
            </Link>

            <h1>What is Moneyline Betting?</h1>

            <p className="lead">
                Moneyline betting is the simplest form of sports betting. You simply pick which team will win the game outright, without any point spreads or handicaps.
            </p>

            <h2>How Moneyline Odds Work</h2>

            <p>
                Moneyline odds are expressed as either positive (+) or negative (-) numbers:
            </p>

            <ul>
                <li><strong>Negative odds (e.g., -150)</strong>: This is the favorite. The number shows how much you need to bet to win $100. For example, at -150, you'd need to bet $150 to win $100.</li>
                <li><strong>Positive odds (e.g., +130)</strong>: This is the underdog. The number shows how much you'd win on a $100 bet. For example, at +130, a $100 bet would win you $130.</li>
            </ul>

            <h2>Example</h2>

            <div className="bg-gray-900 border border-gray-700 p-4 rounded-lg my-4">
                <p className="font-semibold mb-2">Lakers vs Warriors</p>
                <ul className="list-none space-y-1">
                    <li>Lakers: <span className="font-mono">-150</span></li>
                    <li>Warriors: <span className="font-mono">+130</span></li>
                </ul>
            </div>

            <p>
                In this example:
            </p>
            <ul>
                <li>The Lakers are favored to win (negative odds)</li>
                <li>A $150 bet on the Lakers wins $100 if they win</li>
                <li>A $100 bet on the Warriors wins $130 if they win</li>
            </ul>

            <h2>Converting Odds to Probability</h2>

            <p>
                You can convert moneyline odds to implied probability:
            </p>

            <ul>
                <li><strong>Negative odds</strong>: Probability = |Odds| / (|Odds| + 100)<br />
                    Example: -150 = 150 / (150 + 100) = 60%</li>
                <li><strong>Positive odds</strong>: Probability = 100 / (Odds + 100)<br />
                    Example: +130 = 100 / (130 + 100) = 43.5%</li>
            </ul>

            <h2>Finding Value</h2>

            <p>
                The key to successful betting is finding "value" - situations where your predicted probability of a team winning is higher than the implied probability from the odds. Our prediction system uses machine learning to identify these opportunities.
            </p>

            <div className="mt-8 pt-8 border-t border-gray-700">
                <h3>Related Articles</h3>
                <ul className="space-y-2">
                    <li><Link href="/blog/over-under" className="text-blue-400 hover:text-blue-300 hover:underline">What is Over/Under?</Link></li>
                    <li><Link href="/blog/betting-types" className="text-blue-400 hover:text-blue-300 hover:underline">Different Types of Sports Bets</Link></li>
                </ul>
            </div>
        </article>
    )
}
