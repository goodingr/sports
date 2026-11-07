'use client';

import type { ReactNode } from 'react';

if (process.env.NODE_ENV === 'production') {
    throw new Error('Clerk browser fixtures cannot run in production');
}

const getToken = async () => null;

export function ClerkProvider({ children }: { children: ReactNode }) {
    return <>{children}</>;
}

export function SignedOut({ children }: { children: ReactNode }) {
    return <>{children}</>;
}

export function SignedIn() {
    return null;
}

export function UserButton() {
    return null;
}

export function useAuth() {
    return { isLoaded: true, isSignedIn: false, getToken };
}

export function useClerk() {
    return {
        openUserProfile() {
            throw new Error('Signed-out browser fixtures cannot open a user profile');
        },
    };
}

export function useUser() {
    return { isLoaded: true, isSignedIn: false, user: null };
}

export function SignIn() {
    return <section aria-label="Sign in test fixture"><h1>Sign in</h1><p>Signed-out browser test fixture</p></section>;
}

export function SignUp() {
    return <section aria-label="Sign up test fixture"><h1>Sign up</h1><p>Signed-out browser test fixture</p></section>;
}
