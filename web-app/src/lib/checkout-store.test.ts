import { once } from "node:events";
import { existsSync, mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Worker } from "node:worker_threads";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CheckoutStore, type CheckoutScope, type CheckoutSessionParams } from "./checkout-store";

vi.mock("server-only", () => ({}));

const owner: CheckoutScope = { account: "acct_project", livemode: false, userId: "user_buyer" };
const customerParams = { metadata: { userId: owner.userId } };
const customerId = "cus_buyer";
const firstSessionId = "cs_test_first";
const now = 1_800_000_000_000;

function sessionParams(price = "price_monthly"): CheckoutSessionParams {
    return {
        customer: customerId,
        currency: "usd",
        adaptive_pricing: { enabled: false },
        success_url: "https://example.test/account?success=true",
        cancel_url: "https://example.test/pricing?canceled=true",
        payment_method_types: ["card"],
        mode: "subscription",
        billing_address_collection: "auto",
        line_items: [{ price, quantity: 1 }],
        subscription_data: { metadata: { userId: owner.userId } },
        metadata: { userId: owner.userId },
    };
}

describe("CheckoutStore", () => {
    let directory: string;
    let path: string;
    let clock: number;
    const connections = new Set<CheckoutStore>();

    function open(requireExisting = false): CheckoutStore {
        const store = new CheckoutStore(path, { requireExisting, now: () => clock });
        connections.add(store);
        return store;
    }

    function close(store: CheckoutStore): void {
        store.close();
        connections.delete(store);
    }

    beforeEach(() => {
        directory = mkdtempSync(join(tmpdir(), "checkout-store-test-"));
        path = join(directory, "reservations.sqlite");
        clock = now;
    });

    afterEach(() => {
        for (const store of connections) close(store);
        rmSync(directory, { recursive: true });
    });

    it("requires a provisioned database when configured and opens it after initialization", () => {
        expect(() => open(true)).toThrow();
        expect(existsSync(path)).toBe(false);
        writeFileSync(path, "", { mode: 0o600 });
        expect(() => open(true)).toThrow("must be initialized");
        close(open());

        expect(open(true).getCustomer(owner)).toBeNull();
        if (process.platform !== "win32") expect(statSync(path).mode & 0o777).toBe(0o600);
    });

    it("stores only checkout parameters and preserves customer and session attempts across reopen", () => {
        const store = open();
        const requestedCustomer = {
            email: "unneeded@example.test",
            metadata: { userId: owner.userId, token: "not stored" },
        };
        const customer = store.reserveCustomer(owner, requestedCustomer);
        expect(customer).toMatchObject({ generation: 1, createdAt: now, stripeId: null, retryable: true });
        expect(customer.params).toEqual(customerParams);
        expect(store.saveCustomer(owner, customer.key, customerId)).toBe(true);
        const requestedSession = { ...sessionParams(), payment_method: "not stored" };
        const session = store.reserveSession(owner, requestedSession);
        requestedSession.line_items[0].price = "price_changed";
        expect(session.params).toEqual(sessionParams());
        expect(store.saveSession(owner, session.key, firstSessionId)).toBe(true);
        close(store);

        const reopened = open(true);
        expect(reopened.reserveCustomer(owner, customerParams)).toEqual({
            ...customer, stripeId: customerId, retryable: false,
        });
        expect(reopened.reserveSession(owner, sessionParams("price_other"))).toEqual({
            ...session, stripeId: firstSessionId, retryable: false,
        });
    });

    it("reuses the first customer reservation across connections and keeps its verified ID", () => {
        const first = open();
        const second = open(true);
        const pending = first.reserveCustomer(owner, customerParams);
        expect(second.reserveCustomer(owner, customerParams, "cus_legacy")).toEqual(pending);
        expect(second.saveCustomer(owner, "stale-key", customerId)).toBe(false);
        expect(first.saveCustomer(owner, pending.key, customerId)).toBe(true);
        expect(second.saveCustomer(owner, pending.key, customerId)).toBe(true);
        expect(second.saveCustomer(owner, pending.key, "cus_other")).toBe(false);
        expect(first.getCustomer(owner)?.stripeId).toBe(customerId);
    });

    it("commits one reservation when independent workers race on the same file", async () => {
        close(open());
        const code = `
            const { parentPort, workerData } = require("node:worker_threads");
            import(workerData.module).then(({ CheckoutStore }) => {
                const { path, owner, customerParams, customerId, sessionParams, now } = workerData;
                const store = new CheckoutStore(path, { requireExisting: true, now: () => now });
                parentPort.once("message", () => {
                    const customer = store.reserveCustomer(owner, customerParams, customerId);
                    const session = store.reserveSession(owner, sessionParams);
                    store.close();
                    parentPort.postMessage({ customer, session });
                });
                parentPort.postMessage("ready");
            }).catch((error) => { setImmediate(() => { throw error; }); });
        `;
        const workers = ["price_first", "price_second"].map((price) => new Worker(code, {
            eval: true,
            execArgv: ["--experimental-strip-types", "--conditions=react-server"],
            workerData: {
                module: new URL("./checkout-store.ts", import.meta.url).href,
                path, owner, customerParams, customerId, sessionParams: sessionParams(price), now: clock,
            },
        }));

        try {
            await Promise.all(workers.map((worker) => once(worker, "message")));
            const replies = workers.map((worker) => once(worker, "message"));
            for (const worker of workers) worker.postMessage("reserve");
            const [[first], [second]] = await Promise.all(replies);
            expect(second).toEqual(first);
            const store = open(true);
            expect(store.getCustomer(owner)).toEqual(first.customer);
            expect(store.getSession(owner)).toEqual(first.session);
        } finally {
            await Promise.all(workers.map((worker) => worker.terminate()));
        }
    });

    it("isolates account and mode scopes and prevents adopting one customer twice in the same scope", () => {
        const store = open();
        const adopted = store.reserveCustomer(owner, customerParams, customerId);
        expect(adopted).toMatchObject({ stripeId: customerId, retryable: false });

        for (const separate of [{ ...owner, account: "acct_other" }, { ...owner, livemode: true }]) {
            expect(store.getCustomer(separate)).toBeNull();
            expect(store.reserveCustomer(separate, customerParams, customerId).key).not.toBe(adopted.key);
        }
        const other = { ...owner, userId: "user_other" };
        expect(() => store.reserveCustomer(other, { metadata: { userId: other.userId } }, customerId)).toThrow();
        expect(store.getCustomer(other)).toBeNull();
    });

    it("replaces only the observed saved generation and discards stale writes", () => {
        const first = open();
        const second = open(true);
        first.reserveCustomer(owner, customerParams, customerId);
        const original = first.reserveSession(owner, sessionParams());
        expect(second.reserveSession(owner, sessionParams("price_other"))).toEqual(original);
        expect(first.saveSession(owner, original.key, firstSessionId)).toBe(true);
        expect(second.saveSession(owner, original.key, "cs_test_wrong")).toBe(false);

        const observed = { generation: original.generation, sessionId: firstSessionId };
        const replacement = first.reserveSession(owner, sessionParams("price_replacement"), observed);
        expect(replacement.generation).toBe(2);
        expect(replacement.key).not.toBe(original.key);
        expect(second.reserveSession(owner, sessionParams("price_other"), observed)).toEqual(replacement);
        expect(second.saveSession(owner, original.key, firstSessionId)).toBe(false);
        expect(first.saveSession(owner, replacement.key, "cs_test_second")).toBe(true);
        expect(second.saveSession(owner, replacement.key, "cs_test_second")).toBe(true);
    });

    it("retains aged unresolved customer and session attempts and marks them unsafe to retry", () => {
        const store = open();
        const pending = store.reserveCustomer(owner, customerParams);
        const sessionOwner = { ...owner, livemode: true };
        store.reserveCustomer(sessionOwner, customerParams, customerId);
        const pendingSession = store.reserveSession(sessionOwner, sessionParams());
        close(store);
        const reopened = open(true);

        clock += 23 * 60 * 60 * 1000 - 1;
        expect(reopened.getCustomer(owner)?.retryable).toBe(true);
        expect(reopened.getSession(sessionOwner)?.retryable).toBe(true);
        clock += 1;
        expect(reopened.reserveCustomer(owner, customerParams)).toEqual({ ...pending, retryable: false });
        expect(reopened.reserveSession(sessionOwner, sessionParams("price_other"), {
            generation: pendingSession.generation, sessionId: firstSessionId,
        })).toEqual({ ...pendingSession, retryable: false });
    });

    it("requires an existing customer and rejects a session for a different customer", () => {
        const store = open();
        expect(() => store.reserveSession(owner, sessionParams())).toThrow("requires the reserved Stripe customer");
        store.reserveCustomer(owner, customerParams, customerId);
        expect(() => store.reserveSession(owner, { ...sessionParams(), customer: "cus_other" }))
            .toThrow("requires the reserved Stripe customer");
        expect(store.getSession(owner)).toBeNull();
    });
});
