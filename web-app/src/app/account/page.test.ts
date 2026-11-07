import { auth, currentUser } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";
import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { reconcileCheckoutReturn } from "./checkout-return";
import AccountPage from "./page";

vi.mock("server-only", () => ({}));
vi.mock("@clerk/nextjs/server", () => ({ auth: vi.fn(), currentUser: vi.fn() }));
vi.mock("./checkout-return", () => ({ reconcileCheckoutReturn: vi.fn() }));
vi.mock("next/navigation", () => ({
    redirect: vi.fn(() => { throw new Error("Redirected"); }),
    useRouter: () => ({ refresh: vi.fn() }),
}));

beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(auth).mockResolvedValue({ userId: "user_buyer" } as Awaited<ReturnType<typeof auth>>);
    vi.mocked(currentUser).mockResolvedValue({
        id: "user_buyer", publicMetadata: {},
    } as Awaited<ReturnType<typeof currentUser>>);
});

describe("account checkout return", () => {
    it("renders the check without mutating entitlements on GET", async () => {
        const page = await AccountPage({
            searchParams: Promise.resolve({ success: "true", session_id: "cs_test_checkout" }),
        });

        expect(renderToStaticMarkup(page)).toContain("Checking your checkout with Stripe");
        expect(reconcileCheckoutReturn).not.toHaveBeenCalled();
    });

    it("preserves the legacy webhook-based pending state without a session ID", async () => {
        const page = await AccountPage({ searchParams: Promise.resolve({ success: "true" }) });

        expect(renderToStaticMarkup(page)).toContain("Billing confirmation is pending");
        expect(reconcileCheckoutReturn).not.toHaveBeenCalled();
    });

    it("preserves a valid Stripe return through sign-in", async () => {
        vi.mocked(auth).mockResolvedValueOnce({ userId: null } as Awaited<ReturnType<typeof auth>>);

        await expect(AccountPage({
            searchParams: Promise.resolve({ success: "true", session_id: "cs_test_checkout" }),
        })).rejects.toThrow("Redirected");
        expect(redirect).toHaveBeenCalledWith(
            "/login?redirect_url=%2Faccount%3Fsuccess%3Dtrue%26session_id%3Dcs_test_checkout",
        );
        expect(reconcileCheckoutReturn).not.toHaveBeenCalled();
    });
});
