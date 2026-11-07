export function isPilotOnly(): boolean {
    return process.env.COMMERCIAL_PILOT_ONLY === "true";
}

const APP_ORIGIN = /^https?:\/\/[^/?#@\\\s]+\/?$/i;
const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
const DNS_LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i;

function hasKeyPrefix(key: string | undefined, prefix: string): key is string {
    return !!key && key.startsWith(prefix) && key.length > prefix.length && !/\s/.test(key);
}

function hasPublishableKey(key: string | undefined, mode: "live" | "test"): boolean {
    const prefix = `pk_${mode}_`;
    if (!hasKeyPrefix(key, prefix)) return false;
    try {
        // Clerk encodes its frontend API hostname followed by a dollar sign.
        const decoded = atob(key.slice(prefix.length));
        if (!decoded.endsWith("$")) return false;
        const hostname = decoded.slice(0, -1);
        const labels = hostname.split(".");
        return hostname.length <= 253 && labels.length > 1 && labels.every((label) => DNS_LABEL.test(label));
    } catch {
        return false;
    }
}

export function isInterestConfigurationReady(): boolean {
    if (process.env.NODE_ENV !== "production") return true;

    const mode = process.env.COMMERCIAL_PILOT_MODE ?? "live";
    if (mode !== "live" && mode !== "preview") return false;
    const configuredUrl = process.env.NEXT_PUBLIC_APP_URL;
    if (!configuredUrl || !APP_ORIGIN.test(configuredUrl)) return false;
    try {
        const url = new URL(configuredUrl);
        const loopback = LOOPBACK_HOSTS.has(url.hostname.replace(/\.$/, ""));
        if (url.protocol !== "https:" && !loopback) return false;
        if (mode === "live" && loopback) return false;
    } catch {
        return false;
    }

    const keyMode = mode === "live" ? "live" : "test";
    return hasPublishableKey(process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY, keyMode)
        && hasKeyPrefix(process.env.CLERK_SECRET_KEY, `sk_${keyMode}_`);
}

export function interestConfigurationUnavailable(): Response {
    return new Response("Interest is currently unavailable.", {
        status: 503,
        headers: { "Cache-Control": "no-store" },
    });
}

export function pilotBillingUnavailable(): Response {
    return new Response("Billing is unavailable during the interest pilot.", {
        status: 503,
        headers: { "Cache-Control": "no-store" },
    });
}
