"use client";

import { useClerk } from "@clerk/nextjs";
import { useEffect, useState } from "react";

type InterestAction = "view" | "join" | "leave" | "contact";

type InterestState = { joined: boolean; contactAllowed: boolean };

class InterestRequestError extends Error {
    constructor(readonly status: number, readonly enrollmentClosed = false) {
        super("Interest request failed");
    }
}

async function requestInterest(
    action: InterestAction, source: string, { consent, signal }: { consent?: boolean; signal?: AbortSignal } = {},
): Promise<InterestState> {
    const response = await fetch("/api/interest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, source, consent }),
        signal,
    });

    if (!response.ok) {
        const error = response.status === 409 ? await response.json().catch(() => null) : null;
        throw new InterestRequestError(response.status, error?.code === "enrollment_closed");
    }
    const result = await response.json();
    if (typeof result.joined !== "boolean" || typeof result.contactAllowed !== "boolean") {
        throw new Error("Invalid interest response");
    }
    return result;
}

interface InterestFailure {
    action: InterestAction;
    status: number | null;
    enrollmentClosed: boolean;
}

function failureFor(action: InterestAction, cause: unknown): InterestFailure {
    return cause instanceof InterestRequestError
        ? { action, status: cause.status, enrollmentClosed: cause.enrollmentClosed }
        : { action, status: null, enrollmentClosed: false };
}

function failureMessage(failure: InterestFailure): string {
    if (failure.status === 422) {
        return "Verify your primary email address before allowing follow-up.";
    }
    if (failure.status === 401) {
        return "Your sign-in has expired.";
    }
    if (failure.status === 404) {
        return "The interest form is currently unavailable.";
    }
    if (failure.enrollmentClosed) {
        return "Interest enrollment is closed. Refresh this page to see your current status.";
    }
    if (failure.action === "view") return "We couldn’t load your interest status.";
    if (failure.action === "join") return "We couldn’t confirm that your interest was recorded.";
    if (failure.action === "contact") return "We couldn’t confirm your email preference.";
    return "We couldn’t confirm that your interest was withdrawn.";
}

export function InterestControls({
    source,
    enrollmentClosed,
    signInHref,
}: {
    source: string;
    enrollmentClosed: boolean;
    signInHref: string;
}) {
    const { openUserProfile } = useClerk();
    const [interest, setInterest] = useState<InterestState | null>(null);
    const [pending, setPending] = useState<InterestAction | null>("view");
    const [failure, setFailure] = useState<InterestFailure | null>(null);
    const [notice, setNotice] = useState("");
    const joined = interest?.joined ?? null;
    const contactAllowed = interest?.contactAllowed ?? false;

    useEffect(() => {
        const controller = new AbortController();
        void requestInterest("view", source, { signal: controller.signal })
            .then((state) => {
                if (!controller.signal.aborted) setInterest(state);
            })
            .catch((cause: unknown) => {
                if (!controller.signal.aborted) setFailure(failureFor("view", cause));
            })
            .finally(() => {
                if (!controller.signal.aborted) setPending(null);
            });
        return () => controller.abort();
    }, [source]);

    async function submit(action: InterestAction, consent?: boolean) {
        if (pending !== null || (action === "join" && enrollmentClosed)) return;
        setPending(action);
        setFailure(null);
        setNotice("");
        try {
            const state = await requestInterest(action, source, { consent });
            setInterest(state);
            if (action === "leave" && !state.joined) setNotice("Your interest has been withdrawn.");
            if (action === "contact") {
                setNotice(state.contactAllowed ? "Email follow-up is allowed." : "Email follow-up permission was withdrawn.");
            }
        } catch (cause) {
            const nextFailure = failureFor(action, cause);
            if (nextFailure.status === 401) setInterest(null);
            setFailure(nextFailure);
        } finally {
            setPending(null);
        }
    }

    return (
        <div className="space-y-3">
            <p role="status" className="text-sm text-foreground">
                {pending === "view" && "Checking your interest status…"}
                {pending === null && (notice || (joined === true && "Your $29/month interest is recorded.")
                    || (joined === false && enrollmentClosed && "You have no active interest on record."))}
            </p>
            {joined === true && (
                <div className="space-y-3">
                    <button
                        type="button"
                        onClick={() => void submit("leave")}
                        disabled={pending !== null}
                        className="rounded-lg border border-white/20 px-4 py-2 text-sm font-semibold text-foreground disabled:opacity-60"
                    >
                        {pending === "leave" ? "Withdrawing…" : "Withdraw interest"}
                    </button>
                    {(contactAllowed || !enrollmentClosed) && (
                        <div className="space-y-2">
                            <p className="text-sm text-muted-foreground">
                                Email follow-up about this price research is optional.
                            </p>
                            <button
                                type="button"
                                onClick={() => void submit("contact", !contactAllowed)}
                                disabled={pending !== null}
                                className="text-sm font-semibold text-primary underline disabled:opacity-60"
                            >
                                {pending === "contact" ? "Saving email preference…"
                                    : contactAllowed ? "Withdraw email consent" : "Allow email follow-up"}
                            </button>
                        </div>
                    )}
                </div>
            )}
            {joined === false && !enrollmentClosed && (
                <button
                    type="button"
                    onClick={() => void submit("join")}
                    disabled={pending !== null}
                    className="rounded-lg bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground disabled:opacity-60"
                >
                    {pending === "join" ? "Recording…" : "I’d consider subscribing at $29/month"}
                </button>
            )}
            {failure && (
                <div className="space-y-2">
                    <p role="alert" className="text-sm text-danger">{failureMessage(failure)}</p>
                    {failure.status === 401 && (
                        <a href={signInHref} className="text-sm font-semibold text-primary underline">
                            Sign in again
                        </a>
                    )}
                    {failure.action === "view" && failure.status !== 401 && failure.status !== 404 && (
                        <button
                            type="button"
                            onClick={() => void submit("view")}
                            className="text-sm font-semibold text-primary underline"
                        >
                            Try again
                        </button>
                    )}
                    {failure.status === 422 && (
                        <button
                            type="button"
                            onClick={() => openUserProfile()}
                            className="text-sm font-semibold text-primary underline"
                        >
                            Open profile
                        </button>
                    )}
                </div>
            )}
        </div>
    );
}
