import { SignIn } from "@clerk/nextjs";
import { Container } from "@/components/ui/Container";

export default function Page() {
    return (
        <Container className="flex items-center justify-center min-h-[calc(100vh-4rem)]">
            <SignIn />
        </Container>
    );
}
