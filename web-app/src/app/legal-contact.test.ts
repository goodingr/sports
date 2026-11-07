import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import PrivacyPage from "./privacy/page";
import TermsPage from "./terms/page";

const fullContact = "support+privacy@example.invalid";
const pilotContact = "pilot@example.invalid";

function renderLegalPages() {
    return { privacy: renderToStaticMarkup(PrivacyPage()), terms: renderToStaticMarkup(TermsPage()) };
}

describe("public contact on privacy and terms", () => {
    beforeEach(() => {
        vi.stubEnv("COMMERCIAL_PILOT_ONLY", "false");
        vi.stubEnv("COMMERCIAL_CONTACT_EMAIL", fullContact);
        vi.stubEnv("COMMERCIAL_PILOT_CONTACT_EMAIL", pilotContact);
    });

    afterEach(() => vi.unstubAllEnvs());

    it("uses the configured full-app inbox on both pages", () => {
        const { privacy, terms } = renderLegalPages();
        for (const page of [privacy, terms]) {
            expect(page).toContain(`href="mailto:${fullContact}"`);
            expect(page).not.toContain(pilotContact);
        }
    });

    it("keeps the pilot contact separate from the full app", () => {
        vi.stubEnv("COMMERCIAL_PILOT_ONLY", "true");

        const { privacy, terms } = renderLegalPages();
        for (const page of [privacy, terms]) {
            expect(page).toContain(`href="mailto:${pilotContact}"`);
            expect(page).not.toContain(fullContact);
        }
    });

    it.each([undefined, "  ", "support@example.invalid?bcc=other@example.invalid"])(
        "shows no email link when the full app contact is unavailable (%s)",
        (value) => {
            vi.stubEnv("COMMERCIAL_CONTACT_EMAIL", value);

            const { privacy, terms } = renderLegalPages();
            expect(privacy).toContain("Privacy email is currently unavailable.");
            expect(terms).toContain("Support email is currently unavailable.");
            for (const page of [privacy, terms]) expect(page).not.toContain("mailto:");
        },
    );

    it("shows no email link when the pilot contact is missing", () => {
        vi.stubEnv("COMMERCIAL_PILOT_ONLY", "true");
        vi.stubEnv("COMMERCIAL_PILOT_CONTACT_EMAIL", undefined);

        const { privacy, terms } = renderLegalPages();
        for (const page of [privacy, terms]) expect(page).not.toContain("mailto:");
    });
});
