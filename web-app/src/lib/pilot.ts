export function isPilotOnly(): boolean {
    return process.env.COMMERCIAL_PILOT_ONLY === "true";
}

export function pilotBillingUnavailable(): Response {
    return new Response("Billing is unavailable during the interest pilot.", {
        status: 503,
        headers: { "Cache-Control": "no-store" },
    });
}
