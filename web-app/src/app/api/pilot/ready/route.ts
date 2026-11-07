import { interestConfigurationUnavailable, isInterestConfigurationReady, isPilotOnly } from "@/lib/pilot";
import { publicContactEmail } from "@/lib/public-contact";

export function GET(): Response {
    const headers = { "Cache-Control": "no-store" };
    if (!isPilotOnly()) return new Response(null, { status: 404, headers });
    if (
        process.env.COMMERCIAL_INTEREST_ENABLED !== "true"
        || !isInterestConfigurationReady()
        || !publicContactEmail()
    ) return interestConfigurationUnavailable();

    return new Response(null, { status: 200, headers });
}
