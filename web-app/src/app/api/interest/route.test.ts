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

    it("records a first view and preserves source on repeat visits", async () => {
        updateUserMetadata.mockResolvedValue(clerkUser(view));
        const first = await POST(request({ action: "view", source: "community" }));
        expect(await first.json()).toEqual({ joined: false });
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
            const record = { ...records.get(userId), ...update.privateMetadata[key] };
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
            expect(await independent.json()).toEqual({ joined: false });
            expect(getUser.mock.calls).toEqual([["buyer"], ["independent"]]);
            expect(records.has("buyer")).toBe(false);
        } finally {
            releaseRead.resolve();
            await Promise.allSettled([first, repeated, join]);
        }

        expect(await (await first).json()).toEqual({ joined: false });
        expect((await repeated).status).toBe(200);
        expect(await (await join).json()).toEqual({ joined: true });
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
        expect(await (await retry).json()).toEqual({ joined: false });
        expect(updateUserMetadata).toHaveBeenCalledTimes(2);
    });

    it("requires explicit consent and verified contact information", async () => {
        expect((await POST(request({ action: "join" }))).status).toBe(400);
        getUser.mockResolvedValue(clerkUser(view, "unverified"));
        expect((await POST(request({ action: "join", consent: true }))).status).toBe(422);
        expect(updateUserMetadata).not.toHaveBeenCalled();
    });

    it("supports opt-in, withdrawal, and rejoin without changing original attribution", async () => {
        const firstJoined = { ...view, joinedAt: "2026-11-06T23:01:00.000Z" };
        const withdrawn = { ...firstJoined, withdrawnAt: "2026-11-06T23:02:00.000Z" };
        getUser.mockResolvedValueOnce(clerkUser(view))
            .mockResolvedValueOnce(clerkUser(firstJoined))
            .mockResolvedValueOnce(clerkUser(withdrawn));
        updateUserMetadata.mockResolvedValueOnce(clerkUser(firstJoined))
            .mockResolvedValueOnce(clerkUser(withdrawn))
            .mockResolvedValueOnce(clerkUser(firstJoined));

        expect(await (await POST(request({ action: "join", consent: true }))).json())
            .toEqual({ joined: true });
        expect(await (await POST(request({ action: "leave" }))).json()).toEqual({ joined: false });
        expect(await (await POST(request({ action: "join", consent: true }))).json())
            .toEqual({ joined: true });
        expect(updateUserMetadata).toHaveBeenNthCalledWith(3, "buyer", {
            privateMetadata: { [key]: { joinedAt: firstJoined.joinedAt, withdrawnAt: null } },
        });
    });

    it("closes enrollment without recording new views or blocking withdrawals", async () => {
        vi.stubEnv("COMMERCIAL_INTEREST_ENROLLMENT_CLOSED", "true");
        const firstView = await POST(request({ action: "view", source: "community" }));
        expect(await firstView.json()).toEqual({ joined: false });
        expect(firstView.headers.get("cache-control")).toBe("no-store");
        expect(updateUserMetadata).not.toHaveBeenCalled();

        const blockedJoin = await POST(request({ action: "join", consent: true }));
        expect(blockedJoin.status).toBe(409);
        expect(await blockedJoin.json()).toEqual({ code: "enrollment_closed" });
        expect(blockedJoin.headers.get("cache-control")).toBe("no-store");
        expect(updateUserMetadata).not.toHaveBeenCalled();

        const active = { ...view, joinedAt: "2026-11-06T23:01:00.000Z" };
        getUser.mockResolvedValue(clerkUser(active));
        expect(await (await POST(request({ action: "view" }))).json()).toEqual({ joined: true });
        expect(updateUserMetadata).not.toHaveBeenCalled();
        updateUserMetadata.mockResolvedValue(clerkUser({ ...active, withdrawnAt: "2026-11-06T23:02:00.000Z" }));
        const withdrawal = await POST(request({ action: "leave" }));
        expect(await withdrawal.json()).toEqual({ joined: false });
        expect(withdrawal.headers.get("cache-control")).toBe("no-store");
        expect(updateUserMetadata).toHaveBeenCalledWith("buyer", {
            privateMetadata: { [key]: { withdrawnAt: expect.any(String) } },
        });
    });
});
