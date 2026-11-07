import { auth, clerkClient } from "@clerk/nextjs/server";
import { NextResponse } from "next/server";

const OFFER_VERSION = "premium-29-usd-monthly-v1";
const METADATA_KEY = "commercialPriceInterest";
const SOURCES = new Set(["direct", "community", "referral", "email", "other"]);

type Interest = {
    offerVersion: string;
    source: string;
    viewedAt: string;
    joinedAt?: string | null;
    withdrawnAt?: string | null;
};

function readInterest(value: unknown): Interest | undefined {
    if (!value || typeof value !== "object") return undefined;
    const record = value as Partial<Interest>;
    if (record.offerVersion !== OFFER_VERSION || typeof record.viewedAt !== "string") {
        return undefined;
    }
    return record as Interest;
}

function response(joined: boolean) {
    return NextResponse.json({ joined }, { headers: { "Cache-Control": "no-store" } });
}

function allowsOrigin(request: Request): boolean {
    if (request.headers.get("sec-fetch-site") === "cross-site") return false;
    const origin = request.headers.get("origin");
    const configuredUrl = process.env.NEXT_PUBLIC_APP_URL;
    try {
        return origin === new URL(configuredUrl || request.url).origin;
    } catch {
        return false;
    }
}

export async function POST(request: Request) {
    if (process.env.COMMERCIAL_INTEREST_ENABLED !== "true") {
        return new NextResponse("Not found", { status: 404 });
    }
    if (!allowsOrigin(request)) return new NextResponse("Forbidden", { status: 403 });

    const { userId } = await auth();
    if (!userId) return new NextResponse("Sign in required", { status: 401 });

    const body: unknown = await request.json().catch(() => null);
    const payload = body && typeof body === "object"
        ? body as { action?: unknown; source?: unknown; consent?: unknown }
        : {};
    const { action, source, consent } = payload;
    if (action !== "view" && action !== "join" && action !== "leave") {
        return new NextResponse("Invalid action", { status: 400 });
    }
    const enrollmentClosed = process.env.COMMERCIAL_INTEREST_ENROLLMENT_CLOSED === "true";
    if (action === "join" && enrollmentClosed) {
        return NextResponse.json({ code: "enrollment_closed" }, {
            status: 409,
            headers: { "Cache-Control": "no-store" },
        });
    }
    if (action === "join" && consent !== true) {
        return new NextResponse("Contact consent required", { status: 400 });
    }

    try {
        const { users } = await clerkClient();
        const user = await users.getUser(userId);
        const stored = user.privateMetadata[METADATA_KEY];
        const interest = readInterest(stored);
        if (stored != null && !interest) {
            return new NextResponse("Interest record unavailable", { status: 409 });
        }
        const joined = Boolean(interest?.joinedAt && !interest.withdrawnAt);
        if ((action === "view" && (interest || enrollmentClosed)) || (action === "join" && joined)
            || (action === "leave" && !joined)) {
            return response(joined);
        }

        const now = new Date().toISOString();
        const patch: Partial<Interest> = interest ? {} : {
            offerVersion: OFFER_VERSION,
            viewedAt: now,
            source: source === undefined
                ? "direct"
                : typeof source === "string" && SOURCES.has(source) ? source : "other",
        };
        if (action === "join") {
            const primary = user.emailAddresses.find(
                (email) => email.id === user.primaryEmailAddressId
            );
            if (primary?.verification?.status !== "verified") {
                return new NextResponse("Verify your primary email before joining", { status: 422 });
            }
            patch.joinedAt = interest?.joinedAt ?? now;
            patch.withdrawnAt = null;
        }
        if (action === "leave") patch.withdrawnAt = now;

        const updated = await users.updateUserMetadata(userId, {
            privateMetadata: { [METADATA_KEY]: patch },
        });
        const saved = readInterest(updated.privateMetadata[METADATA_KEY]);
        return response(Boolean(saved?.joinedAt && !saved.withdrawnAt));
    } catch {
        return new NextResponse("Unable to save interest. Try again.", { status: 503 });
    }
}
