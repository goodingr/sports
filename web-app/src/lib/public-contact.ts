import { isPilotOnly } from "./pilot";

const INBOX_ADDRESS = /^[a-z0-9._+-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+$/i;

export function publicContactEmail(): string | undefined {
    const configured = isPilotOnly() ? process.env.COMMERCIAL_PILOT_CONTACT_EMAIL : process.env.COMMERCIAL_CONTACT_EMAIL;
    const email = configured?.trim();
    return email && INBOX_ADDRESS.test(email) ? email : undefined;
}
