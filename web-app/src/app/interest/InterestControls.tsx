"use client";

import { useClerk } from "@clerk/nextjs";
import { useEffect, useState } from "react";

type InterestAction = "view" | "join" | "leave";

class InterestRequestError extends Error {
    constructor(readonly status: number, readonly enrollmentClosed = false) {
        super("Interest request failed");
    }
}

async function requestInterest(action: InterestAction, source: string, signal?: AbortSignal) {
    const response = await fetch("/api/interest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, source, ...(action === "join" ? { consent: true } : {}) }),
        signal,
    });

    if (!response.ok) {
        const error = response.status === 409 ? await response.json().catch(() => null) : null;
        throw new InterestRequestError(response.status, error?.code === "enrollment_closed");
    }
    const result: { joined?: unknown } = await response.json();
    if (typeof result.joined !== "boolean") throw new Error("Invalid interest response");
    return result.joined;
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
        return "Verify your primary email address in your profile, then try again.";
    }
    if (failure.status === 401) {
        return "Your sign-in has expired. Refresh this page and sign in again.";
    }
    if (failure.status === 404) {
        return "The interest form is currently unavailable.";
    }
    if (failure.enrollmentClosed) {
        return "Interest enrollment is closed. Refresh this page to see your current status.";
    }
    if (failure.action === "view") return "We couldn’t load your interest status.";
    if (failure.action === "join") return "We couldn’t confirm that your interest was recorded.";
    return "We couldn’t confirm that your interest was withdrawn.";
}

export function InterestControls({
    source,
    enrollmentClosed,
}: {
    source: string;
    enrollmentClosed: boolean;
}) {
    const { openUserProfile } = useClerk();
    const [joined, setJoined] = useState<boolean | null>(null);
    const [consent, setConsent] = useState(false);
    const [pending, setPending] = useState<InterestAction | null>("view");
    const [failure, setFailure] = useState<InterestFailure | null>(null);
    const [notice, setNotice] = useState("");

    useEffect(() => {
        const controller = new AbortController();
        void requestInterest("view", source, controller.signal)
            .then((isJoined) => {
                if (!controller.signal.aborted) setJoined(isJoined);
            })
            .catch((cause: unknown) => {
                if (!controller.signal.aborted) setFailure(failureFor("view", cause));
            })
            .finally(() => {
                if (!controller.signal.aborted) setPending(null);
            });
        return () => controller.abort();
    }, [source]);

    async function submit(action: InterestAction) {
        if (pending !== null || (action === "join" && (!consent || enrollmentClosed))) return;
        setPending(action);
        setFailure(null);
        setNotice("");
        try {
            const isJoined = await requestInterest(action, source);
            setJoined(isJoined);
            if (action !== "view") {
                setConsent(false);
                if (action === "leave" && !isJoined) setNotice("Your interest has been withdrawn.");
            }
        } catch (cause) {
            setFailure(failureFor(action, cause));
        } finally {
            setPending(null);
        }
    }

    return (
        <div className="space-y-3">
            <p role="status" className="text-sm text-foreground">
                {pending === "view" && "Checking your interest status…"}
                {pending === null && joined === true && "Your interest is recorded."}
                {pending === null && joined === false
                    && (notice || (enrollmentClosed && "You have no active interest on record."))}
            </p>
            {joined === true && (
                <button
                    type="button"
                    onClick={() => void submit("leave")}
                    disabled={pending !== null}
                    className="rounded-lg border border-white/20 px-4 py-2 text-sm font-semibold text-foreground disabled:opacity-60"
                >
                    {pending === "leave" ? "Withdrawing…" : "Withdraw interest"}
                </button>
            )}
            {joined === false && !enrollmentClosed && (
                <>
                    <label className="flex items-start gap-2 text-sm text-foreground">
                        <input
                            type="checkbox"
                            checked={consent}
                            disabled={pending !== null}
                            onChange={(event) => setConsent(event.currentTarget.checked)}
                            className="mt-0.5 h-4 w-4 shrink-0 accent-primary"
                        />
                        I agree to be contacted by email about this price research.
                    </label>
                    <button
                        type="button"
                        onClick={() => void submit("join")}
                        disabled={!consent || pending !== null}
                        className="rounded-lg bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground disabled:opacity-60"
                    >
                        {pending === "join" ? "Recording…" : "Record my interest"}
                    </button>
                </>
            )}
            {failure && (
                <div className="space-y-2">
                    <p role="alert" className="text-sm text-danger">{failureMessage(failure)}</p>
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
