import Link from 'next/link'

const blogPosts = [
    {
        slug: 'moneyline',
        title: 'What is Moneyline?',
        description: 'Learn about moneyline betting and how odds work'
    },
    {
        slug: 'over-under',
        title: 'What is Over/Under?',
        description: 'Understanding totals betting and how to read the lines'
    },
    {
        slug: 'betting-types',
        title: 'Different Types of Sports Bets',
        description: 'Explore moneyline, spread, totals, and more betting options'
    },
    {
        slug: 'app-overview',
        title: 'App Overview & Stats Guide',
        description: 'Learn how to use the dashboard and understand our metrics'
    }
]

export default function BlogIndex() {
    return (
        <div className="blog-index">
            <h1 className="text-4xl font-bold mb-2 text-gray-100">Learn Sports Betting</h1>
            <p className="text-gray-400 mb-8">
                Educational resources to help you understand sports betting and our prediction system
            </p>

            <div className="grid gap-6 md:grid-cols-2">
                {blogPosts.map((post) => (
                    <Link
                        key={post.slug}
                        href={`/blog/${post.slug}`}
                        className="block p-6 border rounded-lg hover:shadow-lg transition-shadow bg-gray-900 border-gray-700 hover:border-gray-600"
                    >
                        <h2 className="text-2xl font-semibold mb-2 text-gray-100">{post.title}</h2>
                        <p className="text-gray-400">{post.description}</p>
                    </Link>
                ))}
            </div>
        </div>
    )
}
