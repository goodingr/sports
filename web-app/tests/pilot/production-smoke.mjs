import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { generateKeyPairSync, sign } from "node:crypto";
import { once } from "node:events";
import { existsSync } from "node:fs";
import { mkdtemp, rmdir, unlink, writeFile } from "node:fs/promises";
import { createServer, request } from "node:http";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { pathToFileURL } from "node:url";

const host = "pilot.example.invalid";
const origin = "https://" + host;
const clerkHost = "clerk.example.invalid";
const userId = "user_pilot_smoke";
const secret = "sk_live_pilot_smoke_fixture";
const next = "node_modules/next/dist/bin/next";

// Next and its workers preload this file to limit Node TCP connections to loopback.
function restrictConnections() {
    const connect = net.Socket.prototype.connect;
    net.Socket.prototype.connect = function (...args) {
        const values = Array.isArray(args[0]) ? args[0] : args;
        const hostname = typeof values[0] === "object" ? values[0]?.host : values[1];
        if (typeof hostname === "string" && !["127.0.0.1", "::1", "localhost"].includes(hostname)) {
            throw new Error("Non-loopback connection blocked in pilot smoke");
        }
        return connect.apply(this, args);
    };
}

async function listen(server) {
    await once(server.listen(0, "127.0.0.1"), "listening");
    return server.address().port;
}

function makeSession(publicKeyId, privateKey) {
    const now = Math.floor(Date.now() / 1000);
    const base64url = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
    const header = base64url({ alg: "RS256", typ: "JWT", kid: publicKeyId });
    const claims = base64url({
        iss: "https://" + clerkHost, sub: userId, sid: "sess_pilot_smoke",
        azp: origin, iat: now, nbf: now - 1, exp: now + 60,
    });
    const token = header + "." + claims;
    const signature = sign("RSA-SHA256", Buffer.from(token), privateKey).toString("base64url");
    return "__client_uat=" + now + "; __session=" + token + "." + signature;
}

function makeClerkFixture(jwk) {
    const user = {
        object: "user",
        id: userId,
        private_metadata: {},
        public_metadata: {},
        unsafe_metadata: {},
        primary_email_address_id: "email_pilot_smoke",
        email_addresses: [{
            object: "email_address",
            id: "email_pilot_smoke",
            email_address: "participant@example.invalid",
            verification: { status: "verified" },
            linked_to: [],
        }],
        phone_numbers: [],
        web3_wallets: [],
        external_accounts: [],
        organization_memberships: [],
    };
    const server = createServer(async (req, res) => {
        const route = req.method + " " + req.url;
        res.setHeader("content-type", "application/json");
        try {
            assert.equal(req.headers.authorization, "Bearer " + secret);
            if (route === "GET /v1/jwks") return res.end(JSON.stringify({ keys: [jwk] }));
            if (route === "GET /v1/users/" + userId) return res.end(JSON.stringify(user));
            assert.equal(route, "PATCH /v1/users/" + userId + "/metadata");
            let body = "";
            for await (const chunk of req) body += chunk;
            const patch = JSON.parse(body).private_metadata.commercialPriceInterest;
            Object.assign(user.private_metadata.commercialPriceInterest ??= {}, patch);
            return res.end(JSON.stringify(user));
        } catch (error) {
            console.error("Clerk fixture rejected request", route, error.message);
            res.statusCode = 500;
            return res.end("{}");
        }
    });
    return { server, user };
}

function getResponse(port, path, { body, cookie, requestOrigin = origin } = {}) {
    return new Promise((resolve, reject) => {
        // Simulate the production HTTPS proxy headers on a loopback-only Next process.
        const req = request({
            host: "localhost", port, path, method: body === undefined ? "GET" : "POST",
            headers: {
                host, "x-forwarded-host": host, "x-forwarded-proto": "https", "x-forwarded-port": "443",
                ...(cookie ? { cookie } : {}),
                ...(body === undefined ? {} : { origin: requestOrigin, "content-type": "application/json" }),
            },
        }, async (res) => {
            try {
                let text = "";
                for await (const chunk of res) text += chunk;
                resolve({ status: res.statusCode, headers: res.headers, text });
            } catch (error) {
                reject(error);
            }
        });
        req.on("error", reject);
        req.setTimeout(5000, () => req.destroy(new Error("Pilot HTTP request timed out")));
        req.end(body === undefined ? undefined : JSON.stringify(body));
    });
}

async function withPilot(env, port, enrollmentClosed, check) {
    const app = spawn(process.execPath, [next, "start", "--hostname", "localhost", "--port", String(port)], {
        env: { ...env, COMMERCIAL_INTEREST_ENROLLMENT_CLOSED: String(enrollmentClosed) },
        stdio: "inherit",
    });
    const completion = once(app, "close");
    try {
        for (let attempt = 0; attempt < 100; attempt++) {
            let page;
            try {
                page = await getResponse(port, "/interest");
            } catch (error) {
                if (error.code !== "ECONNREFUSED" || app.exitCode !== null) throw error;
                await delay(100);
                continue;
            }
            assert.equal(page.status, 200);
            await check(page);
            return;
        }
        throw new Error("Production pilot did not start");
    } finally {
        if (app.exitCode === null) app.kill();
        await completion;
    }
}

async function main() {
    // A clean checkout prevents dotenv from adding local provider configuration.
    for (const file of [".env", ".env.local", ".env.production", ".env.production.local"]) {
        assert.ok(!existsSync(file), "Run the pilot smoke in a checkout without " + file);
    }
    const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    const jwk = { ...publicKey.export({ format: "jwk" }), kid: "kid_pilot_smoke", alg: "RS256", use: "sig" };
    const { server, user } = makeClerkFixture(jwk);
    const dir = await mkdtemp(join(tmpdir(), "pilot-smoke-"));
    const fonts = join(dir, "fonts.json");

    try {
        const providerPort = await listen(server);
        const probe = createServer();
        const port = await listen(probe);
        await new Promise((resolve) => probe.close(resolve));

        await writeFile(fonts, JSON.stringify(Object.fromEntries(
            ["Geist", "Geist Mono"].map((name) => [
                "https://fonts.googleapis.com/css2?family=" + name.replace(" ", "+") + ":wght@100..900&display=swap",
                "@font-face {font-family: '" + name + "'; font-weight: 100 900; src: local('Arial');}",
            ]),
        )));
        const env = Object.fromEntries(Object.entries(process.env).filter(([name]) =>
            !/^(CLERK_|NEXT_PUBLIC_CLERK_|COMMERCIAL_|STRIPE_|PLAYWRIGHT_)/i.test(name),
        ));
        Object.assign(env, {
            NODE_ENV: "production",
            NODE_OPTIONS: "--import=" + import.meta.url,
            NEXT_TELEMETRY_DISABLED: "1",
            NEXT_FONT_GOOGLE_MOCKED_RESPONSES: fonts,
            NEXT_PUBLIC_CLERK_KEYLESS_DISABLED: "1",
            NEXT_PUBLIC_CLERK_SIGN_IN_URL: "/login",
            NEXT_PUBLIC_CLERK_SIGN_UP_URL: "/signup",
            COMMERCIAL_PILOT_ONLY: "true",
            COMMERCIAL_PILOT_MODE: "live",
            COMMERCIAL_INTEREST_ENABLED: "true",
            COMMERCIAL_PILOT_CONTACT_EMAIL: "support@example.invalid",
            NEXT_PUBLIC_APP_URL: origin,
            NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY: "pk_live_" + Buffer.from(clerkHost + "$").toString("base64"),
            CLERK_SECRET_KEY: secret,
            CLERK_API_URL: "http://127.0.0.1:" + providerPort,
            NEXT_PUBLIC_API_URL: "http://127.0.0.1:1",
        });
        const [buildStatus] = await once(spawn(process.execPath, [next, "build"], { env, stdio: "inherit" }), "close");
        assert.equal(buildStatus, 0, "Pilot production build failed");

        const cookie = () => makeSession(jwk.kid, privateKey);
        const post = (action, options = {}) => getResponse(port, "/api/interest", {
            body: { action, source: "referral", consent: options.consent },
            cookie: options.cookie ?? cookie(),
            requestOrigin: options.origin,
        });
        const expectState = async (result, joined, contactAllowed) => {
            const response = await result;
            assert.equal(response.status, 200, response.text);
            assert.equal(response.headers["cache-control"], "no-store");
            assert.deepEqual(JSON.parse(response.text), { joined, contactAllowed });
        };
        const expectReadiness = async (status) => {
            const response = await getResponse(port, "/api/pilot/ready");
            assert.equal(response.status, status);
            assert.equal(response.headers["cache-control"], "no-store");
            if (status === 503) assert.equal(response.text, "Interest is currently unavailable.");
        };
        let joinedRecord;

        await withPilot({ ...env, COMMERCIAL_PILOT_CONTACT_EMAIL: "operator@" }, port, false, async (page) => {
            await expectReadiness(503);
            assert.match(page.text, /Sign in to manage interest/);
            assert.doesNotMatch(page.text, /Create an account/);
            await expectState(post("view"), false, false);
            assert.equal((await post("join", { consent: false })).status, 409);
            assert.equal((await post("contact", { consent: true })).status, 409);
            assert.equal(user.private_metadata.commercialPriceInterest, undefined);
        });

        await withPilot(env, port, false, async (page) => {
            await expectReadiness(200);
            assert.match(page.text, /\$29\/month/);
            assert.match(page.text, /no subscription\s+or payment obligation/);
            const referral = await getResponse(port, "/?source=referral");
            assert.equal(referral.status, 307);
            const landing = new URL(referral.headers.location, origin);
            assert.equal(landing.href, origin + "/interest?source=referral");
            const signedOut = await getResponse(port, landing.pathname + landing.search);
            assert.equal(signedOut.status, 200);
            assert.match(signedOut.text, /href="\/login\?redirect_url=%2Finterest%3Fsource%3Dreferral"/);
            const signedIn = await getResponse(port, landing.pathname + landing.search, { cookie: cookie() });
            assert.equal(signedIn.status, 200);
            assert.doesNotMatch(signedIn.text, /Sign in to record interest/);
            assert.equal((await post("join", { cookie: "" })).status, 401);
            assert.equal((await post("join", { origin: "https://other.example.invalid" })).status, 403);
            await expectState(post("view"), false, false);
            await expectState(post("join", { consent: false }), true, false);
            await expectState(post("view"), true, false);
            await expectState(post("contact", { consent: true }), true, true);
            joinedRecord = { ...user.private_metadata.commercialPriceInterest };
            assert.equal(joinedRecord.offerVersion, "premium-29-usd-monthly-v1");
            assert.equal(joinedRecord.source, "referral");
            assert.ok(joinedRecord.viewedAt && joinedRecord.joinedAt);
            assert.equal(joinedRecord.contactConsent.active, true);

            for (const path of ["/api/checkout", "/api/billing/portal", "/api/webhooks/stripe"]) {
                const billing = await getResponse(port, path, { body: {}, cookie: cookie() });
                assert.equal(billing.status, 503, path);
                assert.equal(billing.headers["cache-control"], "no-store", path);
            }
        });

        await withPilot(env, port, true, async (page) => {
            await expectReadiness(200);
            assert.match(page.text, /Interest enrollment is closed/);
            await expectState(post("view"), true, true);
            assert.equal((await post("join", { consent: false })).status, 409);
        });

        await withPilot({ ...env, COMMERCIAL_PILOT_CONTACT_EMAIL: undefined }, port, false, async (page) => {
            await expectReadiness(503);
            assert.match(page.text, /Sign in to manage interest/);
            const signedIn = await getResponse(port, "/interest", { cookie: cookie() });
            assert.equal(signedIn.status, 200);
            assert.match(signedIn.text, /Manage your Pro Access interest/);
            assert.doesNotMatch(signedIn.text, /Sign in to manage interest/);
            assert.equal((await post("leave", { cookie: "" })).status, 401);
            assert.equal((await post("leave", { origin: "https://other.example.invalid" })).status, 403);
            await expectState(post("view"), true, true);
            await expectState(post("contact", { consent: false }), true, false);
            await expectState(post("leave"), false, false);
            await expectState(post("view"), false, false);
            const withdrawn = user.private_metadata.commercialPriceInterest;
            for (const field of ["offerVersion", "source", "viewedAt", "joinedAt"]) {
                assert.equal(withdrawn[field], joinedRecord[field]);
            }
            assert.ok(withdrawn.withdrawnAt);
            assert.equal(withdrawn.contactConsent.active, false);
        });

        console.log("Offline production pilot passed: readiness, referral, consent, withdrawal and billing closure.");
    } finally {
        if (server.listening) await new Promise((resolve) => server.close(resolve));
        if (existsSync(fonts)) await unlink(fonts);
        await rmdir(dir);
    }
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
    await main();
} else {
    restrictConnections();
}
