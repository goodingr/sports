import { auth, clerkClient } from "@clerk/nextjs/server";
import { NextResponse } from "next/server";

const OFFER_VERSION = "premium-29-usd-monthly-v1";
const METADATA_KEY = "commercialPriceInterest";
const SOURCES = new Set(["direct", "community", "referral", "email", "other"]);
const pendingRequestsByUser = new Map<string, Promise<void>>();

type Interest = {
    offerVersion: string;
    source: string;
    viewedAt: string;
    joinedAt?: string | null;
    withdrawnAt?: string | null;
    contactConsent?: ContactConsent;
};

type ContactConsent = {
    active: boolean;
    firstGrantedAt?: string | null;
    grantedAt?: string | null;
};

function readInterest(value: unknown): Interest | undefined {
    if (!value || typeof value !== "object") return undefined;
    const record = value as Partial<Interest>;
    if (record.offerVersion !== OFFER_VERSION || typeof record.viewedAt !== "string") {
        return undefined;
    }
    if ("contactConsent" in record && (
        !record.contactConsent || typeof record.contactConsent.active !== "boolean"
        || (record.contactConsent.firstGrantedAt != null && typeof record.contactConsent.firstGrantedAt !== "string")
        || (record.contactConsent.active && (
            typeof record.contactConsent.grantedAt !== "string" || typeof record.contactConsent.firstGrantedAt !== "string"
        ))
    )) return undefined;
    return record as Interest;
}

function interestState(interest: Interest | undefined) {
    const joined = Boolean(interest?.joinedAt && !interest.withdrawnAt);
    // Before email consent was separate, every successful join required it.
    return { joined, contactAllowed: joined && (interest?.contactConsent?.active ?? true) };
}

function response(interest: Interest | undefined) {
    return NextResponse.json(interestState(interest), { headers: { "Cache-Control": "no-store" } });
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
    if (action !== "view" && action !== "join" && action !== "leave" && action !== "contact") {
        return new NextResponse("Invalid action", { status: 400 });
    }
    if ((action === "contact" || (action === "join" && consent !== undefined)) && typeof consent !== "boolean") {
        return new NextResponse("Invalid email consent", { status: 400 });
    }
    const enrollmentClosed = process.env.COMMERCIAL_INTEREST_ENROLLMENT_CLOSED === "true";
    if (enrollmentClosed && (action === "join" || (action === "contact" && consent === true))) {
        return NextResponse.json({ code: "enrollment_closed" }, {
            status: 409,
            headers: { "Cache-Control": "no-store" },
        });
    }
    // The pilot runs in one Node process. Keep each account's Clerk read and patch together.
    const previousRequest = pendingRequestsByUser.get(userId);
    let releaseRequest!: () => void;
    const pendingRequest = new Promise<void>((resolve) => { releaseRequest = resolve; });
    pendingRequestsByUser.set(userId, pendingRequest);
    try {
        await previousRequest;
        const { users } = await clerkClient();
        const user = await users.getUser(userId);
        const stored = user.privateMetadata[METADATA_KEY];
        const interest = readInterest(stored);
        if (stored != null && !interest) {
            return new NextResponse("Interest record unavailable", { status: 409 });
        }
        const { joined, contactAllowed } = interestState(interest);
        if (action === "contact" && consent === true && !joined) {
            return new NextResponse("Record price interest before allowing email", { status: 409 });
        }
        if ((action === "view" && (interest || enrollmentClosed)) || (action === "join" && joined)
            || (action === "leave" && !joined) || (action === "contact" && contactAllowed === consent)) {
            return response(interest);
        }

        const now = new Date().toISOString();
        const patch: Partial<Interest> = interest ? {} : {
            offerVersion: OFFER_VERSION,
            viewedAt: now,
            source: source === undefined
                ? "direct"
                : typeof source === "string" && SOURCES.has(source) ? source : "other",
        };
        const grantContact = (action === "join" || action === "contact") && consent === true;
        if (grantContact) {
            const primary = user.emailAddresses.find(
                (email) => email.id === user.primaryEmailAddressId
            );
            if (primary?.verification?.status !== "verified") {
                return new NextResponse("Verify your primary email before allowing follow-up", { status: 422 });
            }
        }
        if (action === "join") {
            patch.joinedAt = interest?.joinedAt ?? now;
            patch.withdrawnAt = null;
        }
        if (action === "leave") patch.withdrawnAt = now;
        if (action !== "view") {
            const firstConsent = interest?.contactConsent
                ? interest.contactConsent.firstGrantedAt
                : interest?.joinedAt;
            patch.contactConsent = {
                active: grantContact,
                firstGrantedAt: firstConsent ?? (grantContact ? now : null),
                grantedAt: grantContact ? now : null,
            };
        }

        const updated = await users.updateUserMetadata(userId, {
            privateMetadata: { [METADATA_KEY]: patch },
        });
        const saved = readInterest(updated.privateMetadata[METADATA_KEY]);
        if (!saved) throw new Error("Interest record could not be read after save");
        return response(saved);
    } catch {
        return new NextResponse("Unable to save interest. Try again.", { status: 503 });
    } finally {
        releaseRequest();
        if (pendingRequestsByUser.get(userId) === pendingRequest) {
            pendingRequestsByUser.delete(userId);
        }
    }
}
