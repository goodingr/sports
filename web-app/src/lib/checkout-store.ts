import "server-only";

import { randomUUID } from "node:crypto";
import { closeSync, constants, fstatSync, openSync } from "node:fs";
import { isAbsolute } from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

export type CheckoutScope = {
    account: string;
    livemode: boolean;
    userId: string;
};

export type CheckoutCustomerParams = {
    metadata: { userId: string };
};

export type CheckoutSessionParams = {
    customer: string;
    currency: "usd";
    adaptive_pricing: { enabled: false };
    success_url: string;
    cancel_url: string;
    payment_method_types: ["card"];
    mode: "subscription";
    billing_address_collection: "auto";
    line_items: [{ price: string; quantity: 1 }];
    subscription_data: { metadata: { userId: string } };
    metadata: { userId: string };
};

export type CheckoutAttempt<T> = {
    generation: number;
    key: string;
    createdAt: number;
    params: T;
    stripeId: string | null;
    retryable: boolean;
};

type Replacement = { generation: number; sessionId: string };
type CheckoutStoreOptions = { requireExisting?: boolean; now?: () => number };
type SqlValue = string | number | null;

// This project uses Node 22 at runtime and @types/node 20 at build time.
interface CheckoutDatabase {
    exec(sql: string): void;
    prepare(sql: string): {
        get(...args: SqlValue[]): unknown;
        run(...args: SqlValue[]): { changes: number };
    };
    close(): void;
}

type Reservation = {
    customer_key: string;
    customer_created_at: number;
    customer_params: string;
    customer_id: string | null;
    session_generation: number;
    session_key: string | null;
    session_created_at: number | null;
    session_params: string | null;
    session_id: string | null;
};

// Leave an hour of margin before Stripe v1 can prune a saved idempotency key.
const SAFE_RETRY_MS = 23 * 60 * 60 * 1000;
const OWNER = "stripe_account = ? AND livemode = ? AND user_id = ?";

function scopeValues(scope: CheckoutScope): [string, number, string] {
    if (typeof scope.livemode !== "boolean"
        || !/^acct_[A-Za-z0-9_]+$/.test(scope.account) || !/^user_[A-Za-z0-9_]+$/.test(scope.userId)) {
        throw new Error("Invalid checkout reservation scope");
    }
    return [scope.account, scope.livemode ? 1 : 0, scope.userId];
}

function requireCustomerId(id: string): void {
    if (!/^cus_[A-Za-z0-9_]+$/.test(id)) throw new Error("Invalid Stripe customer ID");
}

function returnUrl(value: string): string {
    const url = new URL(value);
    if ((url.protocol !== "https:" && url.protocol !== "http:") || url.username || url.password) {
        throw new Error("Invalid checkout return URL");
    }
    return value;
}

function sessionPayload(scope: CheckoutScope, params: CheckoutSessionParams): CheckoutSessionParams {
    const lineItem = params.line_items[0];
    if (
        params.currency !== "usd" || params.adaptive_pricing.enabled !== false
        || params.payment_method_types.length !== 1 || params.payment_method_types[0] !== "card"
        || params.mode !== "subscription" || params.billing_address_collection !== "auto"
        || params.line_items.length !== 1 || !lineItem || lineItem.quantity !== 1
        || !/^price_[A-Za-z0-9_]+$/.test(lineItem.price)
        || params.metadata.userId !== scope.userId || params.subscription_data.metadata.userId !== scope.userId
    ) {
        throw new Error("Invalid checkout session parameters");
    }
    return {
        customer: params.customer,
        currency: params.currency,
        adaptive_pricing: { enabled: false },
        success_url: returnUrl(params.success_url),
        cancel_url: returnUrl(params.cancel_url),
        payment_method_types: ["card"],
        mode: "subscription",
        billing_address_collection: "auto",
        line_items: [{ price: lineItem.price, quantity: 1 }],
        subscription_data: { metadata: { userId: scope.userId } },
        metadata: { userId: scope.userId },
    };
}

function openPrivateDatabase(filename: string, requireExisting: boolean): CheckoutDatabase {
    if (!isAbsolute(filename)) throw new Error("Checkout database requires an absolute private path");

    const sqlite = process.getBuiltinModule?.("node:sqlite") as {
        DatabaseSync: new (path: string) => CheckoutDatabase;
    } | undefined;
    if (!sqlite) throw new Error("Checkout database requires Node 22 SQLite support");

    const flags = constants.O_RDWR | (constants.O_NOFOLLOW ?? 0) | (requireExisting ? 0 : constants.O_CREAT);
    const file = openSync(filename, flags, 0o600);
    try {
        const info = fstatSync(file);
        if (!info.isFile() || (process.platform !== "win32" && (info.mode & 0o077) !== 0)) {
            throw new Error("Checkout database must be a private regular file");
        }
        if (requireExisting && info.size === 0) throw new Error("Checkout database must be initialized");
    } finally {
        closeSync(file);
    }
    // SQLite must open the checked file without recreating it if the mount disappears.
    const uri = pathToFileURL(filename);
    uri.searchParams.set("mode", "rw");
    return new sqlite.DatabaseSync(uri.href);
}

export class CheckoutStore {
    private readonly db: CheckoutDatabase;
    private readonly now: () => number;

    constructor(filename: string, { requireExisting = false, now = Date.now }: CheckoutStoreOptions = {}) {
        this.now = now;
        this.db = openPrivateDatabase(filename, requireExisting);
        try {
            this.db.exec(`
                PRAGMA busy_timeout = 1500;
                PRAGMA synchronous = FULL;
                CREATE TABLE IF NOT EXISTS checkout_reservations (
                    stripe_account TEXT NOT NULL,
                    livemode INTEGER NOT NULL CHECK (livemode IN (0, 1)),
                    user_id TEXT NOT NULL,
                    customer_key TEXT NOT NULL UNIQUE,
                    customer_created_at INTEGER NOT NULL,
                    customer_params TEXT NOT NULL,
                    customer_id TEXT,
                    session_generation INTEGER NOT NULL DEFAULT 0,
                    session_key TEXT UNIQUE,
                    session_created_at INTEGER,
                    session_params TEXT,
                    session_id TEXT,
                    PRIMARY KEY (stripe_account, livemode, user_id),
                    UNIQUE (stripe_account, livemode, customer_id),
                    UNIQUE (stripe_account, livemode, session_id),
                    CHECK (
                        (session_generation = 0 AND session_key IS NULL AND session_created_at IS NULL
                         AND session_params IS NULL AND session_id IS NULL)
                        OR
                        (session_generation > 0 AND session_key IS NOT NULL AND session_created_at IS NOT NULL
                         AND session_params IS NOT NULL AND customer_id IS NOT NULL)
                    )
                ) STRICT;
            `);
        } catch (error) {
            this.db.close();
            throw error;
        }
    }

    close(): void {
        this.db.close();
    }

    getCustomer(scope: CheckoutScope): CheckoutAttempt<CheckoutCustomerParams> | null {
        const row = this.read(scope);
        return row ? this.customerAttempt(row) : null;
    }

    reserveCustomer(
        scope: CheckoutScope,
        params: CheckoutCustomerParams,
        knownCustomerId?: string,
    ): CheckoutAttempt<CheckoutCustomerParams> {
        if (params.metadata.userId !== scope.userId) throw new Error("Invalid checkout customer parameters");
        if (knownCustomerId !== undefined) requireCustomerId(knownCustomerId);
        return this.write(() => {
            const current = this.read(scope);
            if (current) return this.customerAttempt(current);

            const payload: CheckoutCustomerParams = { metadata: { userId: scope.userId } };
            const row = this.db.prepare(`
                INSERT INTO checkout_reservations (
                    stripe_account, livemode, user_id, customer_key, customer_created_at,
                    customer_params, customer_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING *
            `).get(...scopeValues(scope), randomUUID(), this.now(), JSON.stringify(payload), knownCustomerId ?? null);
            return this.customerAttempt(row as Reservation);
        });
    }

    saveCustomer(scope: CheckoutScope, key: string, customerId: string): boolean {
        requireCustomerId(customerId);
        const { changes } = this.db.prepare(`
            UPDATE checkout_reservations SET customer_id = ?
            WHERE ${OWNER} AND customer_key = ? AND (customer_id IS NULL OR customer_id = ?)
        `).run(customerId, ...scopeValues(scope), key, customerId);
        return changes === 1;
    }

    getSession(scope: CheckoutScope): CheckoutAttempt<CheckoutSessionParams> | null {
        return this.sessionAttempt(this.read(scope));
    }

    // The caller must verify the previous session is terminal at Stripe before replacing it.
    reserveSession(
        scope: CheckoutScope,
        params: CheckoutSessionParams,
        replace?: Replacement,
    ): CheckoutAttempt<CheckoutSessionParams> {
        return this.write(() => {
            const row = this.read(scope);
            if (!row?.customer_id || row.customer_id !== params.customer) {
                throw new Error("Checkout session requires the reserved Stripe customer");
            }

            const current = this.sessionAttempt(row);
            if (current && (
                !replace || !current.stripeId
                || current.generation !== replace.generation || current.stripeId !== replace.sessionId
            )) {
                return current;
            }
            if (!current && replace) throw new Error("Checkout session replacement has no predecessor");

            const payload = sessionPayload(scope, params);
            const next = this.db.prepare(`
                UPDATE checkout_reservations
                SET session_generation = session_generation + 1, session_key = ?, session_created_at = ?,
                    session_params = ?, session_id = NULL
                WHERE ${OWNER}
                RETURNING *
            `).get(randomUUID(), this.now(), JSON.stringify(payload), ...scopeValues(scope));
            const reserved = this.sessionAttempt(next as Reservation);
            if (!reserved) throw new Error("Checkout session could not be reserved");
            return reserved;
        });
    }

    saveSession(scope: CheckoutScope, key: string, sessionId: string): boolean {
        if (!/^cs_[A-Za-z0-9_]+$/.test(sessionId)) throw new Error("Invalid Stripe Checkout Session ID");
        const { changes } = this.db.prepare(`
            UPDATE checkout_reservations SET session_id = ?
            WHERE ${OWNER} AND session_key = ? AND (session_id IS NULL OR session_id = ?)
        `).run(sessionId, ...scopeValues(scope), key, sessionId);
        return changes === 1;
    }

    private read(scope: CheckoutScope): Reservation | undefined {
        return this.db.prepare(`SELECT * FROM checkout_reservations WHERE ${OWNER}`)
            .get(...scopeValues(scope)) as Reservation | undefined;
    }

    private write<T>(operation: () => T): T {
        this.db.exec("BEGIN IMMEDIATE");
        try {
            const result = operation();
            this.db.exec("COMMIT");
            return result;
        } catch (error) {
            this.db.exec("ROLLBACK");
            throw error;
        }
    }

    private customerAttempt(row: Reservation): CheckoutAttempt<CheckoutCustomerParams> {
        return this.attempt(1, row.customer_key, row.customer_created_at, row.customer_params, row.customer_id);
    }

    private sessionAttempt(row: Reservation | undefined): CheckoutAttempt<CheckoutSessionParams> | null {
        if (!row?.session_key) return null;
        if (row.session_created_at === null || row.session_params === null) {
            throw new Error("Stored checkout session reservation is incomplete");
        }
        return this.attempt(row.session_generation, row.session_key, row.session_created_at, row.session_params, row.session_id);
    }

    private attempt<T>(generation: number, key: string, createdAt: number, params: string, stripeId: string | null): CheckoutAttempt<T> {
        const age = this.now() - createdAt;
        return {
            generation, key, createdAt, params: JSON.parse(params) as T, stripeId,
            retryable: stripeId === null && age >= 0 && age < SAFE_RETRY_MS,
        };
    }
}
