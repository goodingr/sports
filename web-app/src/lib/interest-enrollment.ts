import { isPilotOnly } from "./pilot";
import { publicContactEmail } from "./public-contact";

export function isInterestEnrollmentClosed(): boolean {
    return process.env.COMMERCIAL_INTEREST_ENROLLMENT_CLOSED === "true"
        || (process.env.NODE_ENV === "production" && isPilotOnly() && !publicContactEmail());
}
