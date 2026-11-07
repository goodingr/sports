import type { Metadata } from 'next';
import { Container } from '@/components/ui/Container';
import { PublishedRecord } from '@/components/record/PublishedRecord';

export const metadata: Metadata = {
    title: 'Published Pick Record | Sports Analytics',
    description: 'Settled results for picks issued through our publication gate at their original prices.',
};

export default function RecordPage() {
    return (
        <Container className="py-10 sm:py-16 max-w-4xl">
            <h1 className="text-3xl sm:text-4xl font-bold text-foreground">Published pick record</h1>
            <p className="mt-3 mb-8 text-sm sm:text-base text-muted-foreground max-w-2xl">
                Results for settled picks published through the subscription feed. Each selection uses
                one unit at its original published price.
            </p>
            <PublishedRecord />
        </Container>
    );
}
