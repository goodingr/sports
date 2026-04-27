import { SignIn } from '@clerk/nextjs';
import { Container } from '@/components/ui/Container';

export default function Page() {
    return (
        <Container className="flex items-center justify-center py-10 sm:py-16 min-h-[calc(100vh-4rem)] px-4">
            <SignIn signUpUrl="/signup" fallbackRedirectUrl="/" />
        </Container>
    );
}
