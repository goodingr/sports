export default function BlogLayout({
    children,
}: {
    children: React.ReactNode
}) {
    return (
        <div className="blog-layout min-h-screen bg-gray-950 text-gray-100">
            <div className="max-w-4xl mx-auto px-4 py-12">
                {children}
            </div>
        </div>
    )
}
