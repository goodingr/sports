import { auth } from "@clerk/nextjs/server";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import InterestPage from "./page";

vi.mock("@clerk/nextjs/server", () => ({ auth: vi.fn() }));
vi.mock("./InterestControls", () => ({
    InterestControls: ({ enrollmentClosed }: { enrollmentClosed: boolean }) =>
        enrollmentClosed ? "Withdrawal controls" : "Interest controls",
}));
vi.mock("next/navigation", () => ({
    notFound: () => { throw new Error("Not found"); },
}));

afterEach(() => {
    vi.unstubAllEnvs();
    vi.clearAllMocks();
});

describe("commercial interest page", () => {
    it("returns not found before looking up auth when disabled", async () => {
        vi.stubEnv("COMMERCIAL_INTEREST_ENABLED", "false");

        await expect(InterestPage({ searchParams: Promise.resolve({}) })).rejects.toThrow("Not found");
        expect(auth).not.toHaveBeenCalled();
    });

    it("shows the exact price and no-payment offer with an attributed login link", async () => {
        vi.stubEnv("COMMERCIAL_INTEREST_ENABLED", "true");
        vi.stubEnv("COMMERCIAL_INTEREST_ENROLLMENT_CLOSED", "false");
        vi.mocked(auth).mockResolvedValue({ userId: null } as Awaited<ReturnType<typeof auth>>);

        const markup = renderToStaticMarkup(await InterestPage({
            searchParams: Promise.resolve({ source: "community" }),
        }));

        expect(markup).toContain("$29/month");
        expect(markup).toContain("Paid subscriptions are currently unavailable");
        expect(markup).toContain("or payment obligation");
        expect(markup).toContain("/login?redirect_url=%2Finterest%3Fsource%3Dcommunity");
        expect(markup).not.toContain("Interest controls");
    });

    it("discloses the signed-in view count and renders the consent controls", async () => {
        vi.stubEnv("COMMERCIAL_INTEREST_ENABLED", "true");
        vi.stubEnv("COMMERCIAL_INTEREST_ENROLLMENT_CLOSED", "false");
        vi.mocked(auth).mockResolvedValue({ userId: "user_1" } as Awaited<ReturnType<typeof auth>>);

        const markup = renderToStaticMarkup(await InterestPage({
            searchParams: Promise.resolve({}),
        }));

        expect(markup).toContain("We count signed-in visits once");
        expect(markup).toContain("Interest controls");
        expect(markup).not.toContain("Sign in to record interest");
    });

    it("shows closed enrollment with a login to manage interest and no sign-up link", async () => {
        vi.stubEnv("COMMERCIAL_INTEREST_ENABLED", "true");
        vi.stubEnv("COMMERCIAL_INTEREST_ENROLLMENT_CLOSED", "true");
        vi.mocked(auth).mockResolvedValue({ userId: null } as Awaited<ReturnType<typeof auth>>);

        const markup = renderToStaticMarkup(await InterestPage({
            searchParams: Promise.resolve({ source: "community" }),
        }));

        expect(markup).toContain("Interest enrollment is closed");
        expect(markup).toContain("Sign in to manage interest");
        expect(markup).toContain("/login?redirect_url=%2Finterest%3Fsource%3Dcommunity");
        expect(markup).not.toContain("/signup");
        expect(markup).not.toContain("We count signed-in visits once");
    });

    it("passes closed enrollment to the signed-in withdrawal controls", async () => {
        vi.stubEnv("COMMERCIAL_INTEREST_ENABLED", "true");
        vi.stubEnv("COMMERCIAL_INTEREST_ENROLLMENT_CLOSED", "true");
        vi.mocked(auth).mockResolvedValue({ userId: "user_1" } as Awaited<ReturnType<typeof auth>>);

        const markup = renderToStaticMarkup(await InterestPage({
            searchParams: Promise.resolve({}),
        }));

        expect(markup).toContain("Withdrawal controls");
        expect(markup).not.toContain("Sign in to manage interest");
    });
});
