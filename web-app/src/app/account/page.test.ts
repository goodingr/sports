import { auth, currentUser } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
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

afterEach(() => vi.unstubAllEnvs());

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

    it("shows account access only for the current Stripe scope", async () => {
        vi.stubEnv("STRIPE_SECRET_KEY", "sk_live_account_fixture");
        vi.stubEnv("STRIPE_ACCOUNT_ID", "acct_fixture");
        vi.stubEnv("STRIPE_PRICE_ID", "price_live");
        const publicMetadata = {
            is_premium: true, subscription_status: "active",
            subscription_current_period_end: Math.floor(Date.now() / 1000) + 3600,
            stripe_customer_id: "cus_fixture", stripe_customer_account_id: "acct_fixture",
            stripe_customer_livemode: false,
            stripe_entitlement: {
                customer_id: "cus_fixture", account_id: "acct_fixture", livemode: false, price_id: "price_test",
            },
        };
        vi.mocked(currentUser).mockResolvedValue({
            id: "user_buyer", publicMetadata,
        } as unknown as Awaited<ReturnType<typeof currentUser>>);
        const props = { searchParams: Promise.resolve({ success: "true" }) };
        const stale = renderToStaticMarkup(await AccountPage(props));
        expect(stale).toContain("No active subscription");
        expect(stale).toContain("Billing confirmation is pending");

        publicMetadata.stripe_customer_livemode = true;
        Object.assign(publicMetadata.stripe_entitlement, { livemode: true, price_id: "price_live" });
        const current = renderToStaticMarkup(await AccountPage(props));
        expect(current).toContain("Subscription confirmed.");
        expect(current).toContain("Pro Access");

        publicMetadata.subscription_current_period_end = Math.floor(Date.now() / 1000);
        const expired = renderToStaticMarkup(await AccountPage(props));
        expect(expired).toContain("No active subscription");
        expect(expired).toContain("Refresh subscription status");
        expect(expired).not.toContain("Pro Access");
    });
});
