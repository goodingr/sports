import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { auth, getUser, updateUserMetadata } = vi.hoisted(() => ({
    auth: vi.fn(),
    getUser: vi.fn(),
    updateUserMetadata: vi.fn(),
}));

vi.mock("@clerk/nextjs/server", () => ({
    auth,
    clerkClient: async () => ({ users: { getUser, updateUserMetadata } }),
}));

import { POST } from "./route";

const origin = "https://sports.example";
const key = "commercialPriceInterest";
const view = {
    offerVersion: "premium-29-usd-monthly-v1",
    viewedAt: "2026-11-06T23:00:00.000Z",
    source: "community",
};

function request(body: object, requestOrigin = origin) {
    return new Request(`${origin}/api/interest`, {
        method: "POST",
        headers: { origin: requestOrigin, "content-type": "application/json" },
        body: JSON.stringify(body),
    });
}

function clerkUser(record?: object, status = "verified") {
    return {
        privateMetadata: record ? { [key]: record } : {},
        primaryEmailAddressId: "primary",
        emailAddresses: [{ id: "primary", verification: { status } }],
    };
}

// Clerk merges nested metadata and removes keys patched to null.
function mergeMetadata(previous: object | undefined, patch: Record<string, unknown>) {
    const merged: Record<string, unknown> = { ...previous };
    for (const [field, value] of Object.entries(patch)) {
        if (value === null) delete merged[field];
        else if (typeof value === "object") {
            const nested = merged[field];
            merged[field] = mergeMetadata(
                typeof nested === "object" && nested !== null ? nested : undefined,
                value as Record<string, unknown>,
            );
        } else merged[field] = value;
    }
    return merged;
}

function storedInterest(initial: object, status = "verified") {
    let record = initial;
    getUser.mockImplementation(async () => clerkUser(record, status));
    updateUserMetadata.mockImplementation(async (_, patch) => {
        record = mergeMetadata(record, patch.privateMetadata[key]);
        return clerkUser(record, status);
    });
    return () => record;
}

function deferred() {
    let resolve!: () => void;
    const promise = new Promise<void>((done) => { resolve = done; });
    return { promise, resolve };
}

describe("price-interest API", () => {
    beforeEach(() => {
        vi.resetAllMocks();
        vi.stubEnv("COMMERCIAL_INTEREST_ENABLED", "true");
        vi.stubEnv("COMMERCIAL_INTEREST_ENROLLMENT_CLOSED", "false");
        vi.stubEnv("NEXT_PUBLIC_APP_URL", origin);
        auth.mockResolvedValue({ userId: "buyer" });
        getUser.mockResolvedValue(clerkUser());
    });

    afterEach(() => vi.unstubAllEnvs());

    it("requires the feature flag, same origin, and an authenticated account", async () => {
        vi.stubEnv("COMMERCIAL_INTEREST_ENABLED", "false");
        expect((await POST(request({ action: "view" }))).status).toBe(404);
        vi.stubEnv("COMMERCIAL_INTEREST_ENABLED", "true");
        expect((await POST(request({ action: "view" }, "https://wrong.example"))).status).toBe(403);
        auth.mockResolvedValue({ userId: null });
        expect((await POST(request({ action: "view" }))).status).toBe(401);
        expect(updateUserMetadata).not.toHaveBeenCalled();
    });

    it.each([
        ["test keys in default live mode", undefined, origin, "pk_test_fixture", "sk_test_fixture"],
        ["mixed key modes", undefined, origin, "pk_live_fixture", "sk_test_fixture"],
        ["empty key suffix", undefined, origin, "pk_live_fixture", "sk_live_"],
        ["missing configured origin", undefined, undefined, "pk_live_fixture", "sk_live_fixture"],
        ["HTTP live origin", undefined, "http://sports.example", "pk_live_fixture", "sk_live_fixture"],
        ["loopback HTTPS live origin", undefined, "https://localhost", "pk_live_fixture", "sk_live_fixture"],
        ["origin with a path", undefined, `${origin}/interest`, "pk_live_fixture", "sk_live_fixture"],
        ["origin with user info", undefined, "https://person@sports.example", "pk_live_fixture", "sk_live_fixture"],
        ["unknown mode", "staging", origin, "pk_live_fixture", "sk_live_fixture"],
        ["preview with live keys", "preview", origin, "pk_live_fixture", "sk_live_fixture"],
        ["preview with public HTTP", "preview", "http://sports.example", "pk_test_fixture", "sk_test_fixture"],
    ])("rejects %s before any Clerk calls in production", async (_, mode, appUrl, publicKey, secretKey) => {
        vi.stubEnv("NODE_ENV", "production");
        vi.stubEnv("COMMERCIAL_PILOT_MODE", mode);
        vi.stubEnv("NEXT_PUBLIC_APP_URL", appUrl);
        vi.stubEnv("NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY", publicKey);
        vi.stubEnv("CLERK_SECRET_KEY", secretKey);

        const result = await POST(request({ action: "join" }));

        expect(result.status).toBe(503);
        expect(result.headers.get("cache-control")).toBe("no-store");
        expect(await result.text()).toBe("Interest is currently unavailable.");
        expect(auth).not.toHaveBeenCalled();
        expect(getUser).not.toHaveBeenCalled();
        expect(updateUserMetadata).not.toHaveBeenCalled();
    });

    it.each([
        ["default live", undefined, origin, "live"],
        ["HTTPS preview", "preview", origin, "test"],
        ["loopback HTTP preview", "preview", "http://127.0.0.1:3100", "test"],
    ])("records interest in a configured %s environment", async (_, mode, appUrl, keyMode) => {
        vi.stubEnv("NODE_ENV", "production");
        vi.stubEnv("COMMERCIAL_PILOT_MODE", mode);
        vi.stubEnv("NEXT_PUBLIC_APP_URL", appUrl);
        vi.stubEnv("NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY", `pk_${keyMode}_fixture`);
        vi.stubEnv("CLERK_SECRET_KEY", `sk_${keyMode}_fixture`);
        updateUserMetadata.mockResolvedValue(clerkUser({
            ...view, joinedAt: view.viewedAt, contactConsent: { active: false },
        }));

        const result = await POST(request({ action: "join" }, appUrl));

        expect(result.status).toBe(200);
        expect(await result.json()).toEqual({ joined: true, contactAllowed: false });
        expect(updateUserMetadata).toHaveBeenCalledTimes(1);
    });

    it("records a first view and preserves source on repeat visits", async () => {
        updateUserMetadata.mockResolvedValue(clerkUser(view));
        const first = await POST(request({ action: "view", source: "community" }));
        expect(await first.json()).toEqual({ joined: false, contactAllowed: false });
        expect(updateUserMetadata).toHaveBeenCalledWith("buyer", {
            privateMetadata: { [key]: { ...view, viewedAt: expect.any(String) } },
        });
        getUser.mockResolvedValue(clerkUser(view));
        await POST(request({ action: "view", source: "referral" }));
        expect(updateUserMetadata).toHaveBeenCalledTimes(1);
    });

    it("serializes a delayed first read with later views and joins for that account", async () => {
        const readStarted = deferred();
        const releaseRead = deferred();
        const records = new Map<string, object>();
        getUser.mockImplementation(async (userId: string) => clerkUser(records.get(userId)));
        getUser.mockImplementationOnce(async (userId: string) => {
            const original = clerkUser(records.get(userId));
            readStarted.resolve();
            await releaseRead.promise;
            return original;
        });
        updateUserMetadata.mockImplementation(async (userId: string, update) => {
            const record = mergeMetadata(records.get(userId), update.privateMetadata[key]);
            records.set(userId, record);
            return clerkUser(record);
        });
        auth.mockResolvedValueOnce({ userId: "buyer" })
            .mockResolvedValueOnce({ userId: "buyer" })
            .mockResolvedValueOnce({ userId: "buyer" })
            .mockResolvedValueOnce({ userId: "independent" });

        const first = POST(request({ action: "view", source: "referral" }));
        await readStarted.promise;
        const repeated = POST(request({ action: "view", source: "direct" }));
        const join = POST(request({ action: "join", source: "direct", consent: true }));
        try {
            const independent = await POST(request({ action: "view", source: "email" }));
            expect(await independent.json()).toEqual({ joined: false, contactAllowed: false });
            expect(getUser.mock.calls).toEqual([["buyer"], ["independent"]]);
            expect(records.has("buyer")).toBe(false);
        } finally {
            releaseRead.resolve();
            await Promise.allSettled([first, repeated, join]);
        }

        expect(await (await first).json()).toEqual({ joined: false, contactAllowed: false });
        expect((await repeated).status).toBe(200);
        expect(await (await join).json()).toEqual({ joined: true, contactAllowed: true });
        expect(records.get("buyer")).toMatchObject({
            source: "referral", viewedAt: expect.any(String), joinedAt: expect.any(String),
        });
        const buyerWrites = updateUserMetadata.mock.calls.filter(([userId]) => userId === "buyer");
        expect(buyerWrites).toHaveLength(2);
        expect(buyerWrites[1][1].privateMetadata[key]).not.toHaveProperty("viewedAt");
    });

    it("releases the account queue when a Clerk update fails", async () => {
        const writeStarted = deferred();
        const releaseWrite = deferred();
        updateUserMetadata.mockImplementationOnce(async () => {
            writeStarted.resolve();
            await releaseWrite.promise;
            throw new Error("simulated Clerk failure");
        }).mockResolvedValueOnce(clerkUser(view));

        const first = POST(request({ action: "view", source: "community" }));
        await writeStarted.promise;
        const retry = POST(request({ action: "view", source: "community" }));
        releaseWrite.resolve();

        expect((await first).status).toBe(503);
        expect(await (await retry).json()).toEqual({ joined: false, contactAllowed: false });
        expect(updateUserMetadata).toHaveBeenCalledTimes(2);
    });

    it("records price intent without email permission and requires verification for follow-up", async () => {
        const record = storedInterest(view, "unverified");
        const join = await POST(request({ action: "join" }));
        expect(await join.json()).toEqual({ joined: true, contactAllowed: false });
        expect(record()).toMatchObject({ ...view, joinedAt: expect.any(String), contactConsent: { active: false } });

        expect((await POST(request({ action: "contact", consent: "true" }))).status).toBe(400);
        expect((await POST(request({ action: "contact", consent: true }))).status).toBe(422);
        expect(updateUserMetadata).toHaveBeenCalledTimes(1);
    });

    it("allows email grant, withdrawal, and re-grant without changing the first consent", async () => {
        const record = storedInterest(view);
        expect((await POST(request({ action: "contact", consent: true }))).status).toBe(409);
        await POST(request({ action: "join" }));

        const allow = { action: "contact", consent: true };
        const withdraw = { action: "contact", consent: false };
        expect(await (await POST(request(allow))).json()).toEqual({ joined: true, contactAllowed: true });
        const firstConsent = (record() as { contactConsent: { firstGrantedAt: string } }).contactConsent.firstGrantedAt;
        expect(await (await POST(request(allow))).json()).toEqual({ joined: true, contactAllowed: true });
        expect(updateUserMetadata).toHaveBeenCalledTimes(2);

        expect(await (await POST(request(withdraw))).json()).toEqual({ joined: true, contactAllowed: false });
        expect(await (await POST(request(withdraw))).json()).toEqual({ joined: true, contactAllowed: false });
        expect(record()).toMatchObject({ contactConsent: { active: false, firstGrantedAt: firstConsent } });
        expect(record()).not.toHaveProperty("contactConsent.grantedAt");
        expect(updateUserMetadata).toHaveBeenCalledTimes(3);

        expect(await (await POST(request(allow))).json()).toEqual({ joined: true, contactAllowed: true });
        expect(record()).toMatchObject({ ...view, contactConsent: { active: true, firstGrantedAt: firstConsent } });
    });

    it("preserves legacy interest and consent history without restoring permission on rejoin", async () => {
        const legacy = { ...view, joinedAt: "2026-11-06T23:01:00.000Z" };
        const record = storedInterest(legacy);
        expect(await (await POST(request({ action: "view" }))).json())
            .toEqual({ joined: true, contactAllowed: true });

        expect(await (await POST(request({ action: "leave" }))).json())
            .toEqual({ joined: false, contactAllowed: false });
        expect(await (await POST(request({ action: "join", source: "other" }))).json())
            .toEqual({ joined: true, contactAllowed: false });
        expect(record()).toEqual({
            ...legacy, contactConsent: { active: false, firstGrantedAt: legacy.joinedAt },
        });
        expect(updateUserMetadata).toHaveBeenCalledTimes(2);
    });

    it("closes enrollment without recording new views or blocking withdrawals", async () => {
        vi.stubEnv("COMMERCIAL_INTEREST_ENROLLMENT_CLOSED", "true");
        const firstView = await POST(request({ action: "view", source: "community" }));
        expect(await firstView.json()).toEqual({ joined: false, contactAllowed: false });
        expect(firstView.headers.get("cache-control")).toBe("no-store");
        expect(updateUserMetadata).not.toHaveBeenCalled();

        const blockedJoin = await POST(request({ action: "join", consent: true }));
        expect(blockedJoin.status).toBe(409);
        expect(await blockedJoin.json()).toEqual({ code: "enrollment_closed" });
        expect(blockedJoin.headers.get("cache-control")).toBe("no-store");
        expect(updateUserMetadata).not.toHaveBeenCalled();

        const active = { ...view, joinedAt: "2026-11-06T23:01:00.000Z" };
        const record = storedInterest(active);
        expect(await (await POST(request({ action: "view" }))).json())
            .toEqual({ joined: true, contactAllowed: true });
        expect(updateUserMetadata).not.toHaveBeenCalled();
        expect(await (await POST(request({ action: "contact", consent: false }))).json())
            .toEqual({ joined: true, contactAllowed: false });
        expect((await POST(request({ action: "contact", consent: true }))).status).toBe(409);
        const withdrawal = await POST(request({ action: "leave" }));
        expect(await withdrawal.json()).toEqual({ joined: false, contactAllowed: false });
        expect(withdrawal.headers.get("cache-control")).toBe("no-store");
        expect(record()).toMatchObject({
            ...active,
            withdrawnAt: expect.any(String),
            contactConsent: { active: false, firstGrantedAt: active.joinedAt },
        });
    });
});
