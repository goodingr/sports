# Commercial validation for paid picks

Evidence reviewed November 6, 2026.

The app displays $29 per month. The project owner confirmed there is no live deployment or customer payment history. The local setup uses test billing and has no configured Stripe price ID. The current publish report has no approved passing rules. Track subscriber economics and pick performance separately. [Pricing page](../web-app/src/app/pricing/page.tsx) · [Checkout route](../web-app/src/app/api/checkout/route.ts) · [Claim evidence](CLAIM_EVIDENCE.md)

## Monthly operating scenarios

At $29, standard U.S. domestic card processing at 2.9% + $0.30 and pay-as-you-go Billing at 0.7% leave $27.66 per full-price payment before refunds, disputes, support, acquisition, and taxes. The configured live Stripe price and actual fees are the sources of truth once available. [Stripe Payments pricing](https://stripe.com/pricing?locale=en-US) · [Stripe Billing pricing](https://stripe.com/billing/pricing)

| Illustrative monthly cost | Focused pilot | Full worker cadence |
| --- | ---: | ---: |
| VM | $24 | $48 |
| Odds data | $30 | $59 |
| Authentication | $0 | $25 |
| Remote backup allowance | $10 | $10 |
| Infrastructure total | $64 | $142 |
| Full-price subscribers to cover infrastructure at $27.66 each | 3 | 6 |
| Full-price subscribers to cover infrastructure at $23.19 each | 3 | 7 |

The focused pilot assumes usage fits within 20,000 monthly credits and that the smaller VM and free authentication tier pass the production readiness checks. The full estimate assumes ten leagues, two featured odds markets, one region, and completed scores. At 24 hourly passes plus one full daily pass, 30 days cost up to 30,000 credits before additional calls for research and retries. Recount on the deployed cadence and actual markets. The backup allowance assumes roughly 100 GB retained in daily snapshots and low request volume. Enter actual hosting, data, auth, backup, domain, and monitoring invoices before relying on the total. [Deployment requirements](DEPLOYMENT.md) · [Worker schedule](../scripts/worker_entrypoint.sh) · [Odds plans and pricing](https://the-odds-api.com/#get-access) · [Odds API request costs](https://the-odds-api.com/liveapi/guides/v4/) · [VM pricing](https://www.digitalocean.com/pricing/droplets) · [Clerk pricing](https://clerk.com/pricing) · [Backup pricing](https://developers.cloudflare.com/r2/pricing/)

The $23.19 planning case assumes refunds equal 3% of gross charges, variable support costs $2 per paying subscriber per month, and monthly churn of 8% with a $20 cost to acquire each replacement customer. Including another $300 per month for 10 hours of fixed operating work at $30 per hour moves the full cadence example to 20 subscribers. These are planning inputs. Use actual cohort retention, acquisition spend, and owner time to replace them. Compute break-even by dividing fixed monthly cost by contribution per paying subscriber and rounding up. A zero or negative contribution cannot cover fixed costs at any subscriber count.

The subscription also matters to the customer’s results. At the existing 5% research ROI threshold, $29 equals the expected profit on $580 of monthly paper turnover. That arithmetic illustrates the effect of price and pick frequency on net value. Measure the available picks per week, zero-pick weeks, and results after the subscription fee at stated flat-unit turnover before using a performance claim. [Prediction quality gates](PREDICTION_QUALITY.md) · [Claim evidence](CLAIM_EVIDENCE.md)

## Measure willingness to pay

While checkout is closed, run a no-charge pilot interest test for the intended product at $29 per month. State the price, that enrollment is closed while picks are validated, and that signing up is optional and creates no payment obligation. Get separate consent to contact people. Exclude team, test, and duplicate records.

The [interest page](../web-app/src/app/interest/page.tsx) is off by default. For a configured preview, set `COMMERCIAL_INTEREST_ENABLED=true` for the web server and make sure `NEXT_PUBLIC_APP_URL` matches its origin. Use a test Clerk instance for preview verification. The live pilot uses the production Clerk instance to count each signed-in account once per offer and to store opt-in and withdrawal in private metadata. Use links such as `/interest?source=community` and `/interest?source=referral` to attribute visits. At the end of enrollment, keep the page enabled and set `COMMERCIAL_INTEREST_ENROLLMENT_CLOSED=true`. This stops new joins and view recording while keeping withdrawal available.

| Measure | Definition |
| --- | --- |
| Qualified visitor | A unique signed-in account from the intended audience that viewed the offer and price. Count separately by acquisition source. This denominator omits people who saw the offer without signing in. |
| Price-intent lead | A unique opt-in after seeing the $29 price. Keep the offer version and acquisition source. |
| First paying subscriber | A distinct customer with a first positive, collected live payment for the subscription. Exclude tests, comps, and out-of-band invoices unless reconciled to actual cash. |
| First and second renewal | Collected payments among first-time payers whose first and second renewal dates have elapsed. Compare by acquisition cohort. |
| Monthly contribution | Gross collected subscription charges less refunds, dispute losses, payment and billing fees, acquisition spend, and variable support. Record sales tax separately. |
| Monthly operating result | Monthly contribution less hosting, data, auth, backups, domains, monitoring, and fixed operating labor. |

For a first read, use a 30-day window with one offer version. Aim to show it to 200 qualified unique visitors and interview five opt-ins about the picks they want, how often they expect them, and why they would pay $29. Treat fewer than 20 opt-ins as a reason to inspect the audience and offer before investing in acquisition. These are exploratory targets. Record conversion counts by source and keep opt-ins separate from collected sales.

Use the [interest report](../scripts/commercial_interest_report.py) with the Clerk credentials for the relevant instance. The command expects a live key by default and labels the report with its Clerk mode. Use `--expected-mode test` for development only. If you pass `--dotenv`, that file supplies the key independently of the shell. Pass the inclusive cohort start to `--viewed-from` and the exclusive end to `--viewed-before` as ISO timestamps with UTC offsets. Repeat `--exclude-user-id` for team and test accounts. The report emits aggregate counts by source without customer records.

For the screened audience totals, pass `--qualified-user-ids-file` with a private UTF-8 roster containing one screened Clerk user ID per nonblank line. Keep the roster outside the repository. Use `qualified_cohort.totals.viewed` and `qualified_cohort.totals.first_opt_ins_in_window` for the 200-visitor and 20-opt-in targets. The opt-in count uses each account's first consent within the declared window and includes later withdrawals. The top-level counts describe raw signed-in exposure. The qualified section also reports how many roster accounts remain eligible after exclusions and how many were found in Clerk. Use current `active` and `withdrawn` counts to understand consent status when the report runs, and verify individual consent before contacting anyone.

### Recruit the pilot

Start with participants already confirmed to fit the intended bettor audience, using individually authorized invitations with `source=referral`. Community visits and builder feedback are raw exposure until the participants' fit is established. Keep them separate when judging the 200-visitor target.

Covers has a Website Promotions board for service promotion, but its broader rules restrict external commercial links. Ask its moderators or Support to approve the exact pilot URL and copy before posting there. The sideprojects community allows substantive project showcases with affiliation disclosure and appropriate flair. Use that channel for product feedback, and screen bettor fit before treating a visit as demand evidence. [Covers guidelines](https://www.covers.com/forum/Guidelines) · [Covers Website Promotions](https://www.covers.com/forum/website-promotions-12) · [sideprojects rules](https://www.reddit.com/r/sideprojects/about/rules.json)

Use this invitation draft once the pilot URL and recipients are approved.

> I’m building a subscription for pregame picks and sportsbook prices, with a proposed price of $29 per month. Paid enrollment is closed while prospective results are validated. I’m inviting people who already use betting research to review the free interest page and discuss what would make the service useful. Participation is optional and creates no payment obligation.

For five consenting participants, discuss their most recent research workflow, which information was missing, what pick frequency would make a $29 subscription worthwhile, and what would cause them to cancel. Record reported behavior separately from hypothetical interest. Confirm their current opt-in status before contacting them.

### Host the pilot

The [web-only Compose profile](../docker-compose.pilot.yml) runs Next.js and Caddy on an owned HTTPS domain. It uses Clerk for signed-in interest records and keeps checkout, the billing portal, and Stripe webhooks closed. Point the pilot domain at the host and make ports 80 and 443 available. The full stack and pilot each bind these ports, so use separate hosts to run both at once.

Create a local `.env.pilot` on the host with the intended domain, a working contact address for privacy and support requests, and the matching Clerk credentials. The values below are placeholders.

```dotenv
PILOT_DOMAIN=pilot.example.com
PILOT_CONTACT_EMAIL=contact@example.com
ACME_EMAIL=operator@example.com
NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=pk_live_replace_me
CLERK_SECRET_KEY=sk_live_replace_me
COMMERCIAL_INTEREST_ENROLLMENT_CLOSED=false
```

```bash
docker compose --env-file .env.pilot -f docker-compose.pilot.yml config --quiet
docker compose --env-file .env.pilot -f docker-compose.pilot.yml up -d --build
```

The pilot root forwards source tags to `/interest`. Check that `/?source=referral` reaches the $29 offer, sign-in returns to that offer, the privacy and terms pages show the configured contact address, and `POST /api/checkout` returns 503. At the end of enrollment, set `COMMERCIAL_INTEREST_ENROLLMENT_CLOSED=true` and rerun `up -d web` with the same Compose file and environment file. Existing participants can still withdraw.

For a hosted Next.js service, build from `web-app` with `npm ci && npm run build`, then start with `npm run start`. Set `COMMERCIAL_PILOT_ONLY=true`, `COMMERCIAL_PILOT_CONTACT_EMAIL`, `NEXT_PUBLIC_APP_URL`, and the public Clerk key at build time. Provide the same settings at runtime, plus `COMMERCIAL_INTEREST_ENABLED=true`, `CLERK_SECRET_KEY`, and the `/login` and `/signup` Clerk routes. A development Clerk instance serves local and small test previews. Use a production instance for live recruitment. For an invitation-only pilot, restrict host access and account registration. The noindex setting limits search indexing. [Clerk production setup](https://clerk.com/docs/guides/development/deployment/production) · [Clerk environments](https://clerk.com/docs/guides/development/managing-environments) · [Clerk access settings](https://clerk.com/docs/guides/secure/restricting-access)

Run the pilot on one persistent Node app instance. Interest requests for the same account share an in-process queue so concurrent views, opt-ins, and withdrawals read the latest Clerk record before writing. Multiple workers or serverless replicas require shared coordination before they serve the same pilot.

The isolated production build and `next start` browser check passed on November 6, 2026 using the development Clerk instance. The open, closed-enrollment, and disabled page states were verified at mobile width, along with unauthenticated mutation rejection. A separate development browser check verified attributed view, opt-in, reload, and withdrawal. The disposable account was deleted. A subsequent aggregate query returned zero development offer viewers and opt-ins.

On November 7, 2026, the packaged pilot at commit `46e74f3` passed 30 loopback
checks through Caddy with a trusted internal TLS certificate and synthetic
Clerk credentials. Both enrollment states stayed healthy and served the
$29 offer. The checks covered the referral redirect, the contact links, the
signed-out and cross-origin consent responses, and closed billing endpoints.
Production Clerk sign-in and public certificate issuance remain launch checks.

After the release gates clear, export live Stripe Billing cohorts and the itemized Balance report for the same time zone and dates. Use the subscription price ID to isolate this product, reconcile charges, fees, refunds, and disputes by transaction, and calculate monthly contribution and operating result. Reconcile paid invoices to actual funds collected. Review two completed renewal cohorts before increasing acquisition spend. Keep customer-level exports out of the repository. [Stripe Billing analytics](https://docs.stripe.com/billing/subscriptions/analytics) · [Stripe Balance report](https://docs.stripe.com/reports/balance) · [Stripe event types](https://docs.stripe.com/api/events/types)

The [cash report](../scripts/commercial_cash_report.py) checks whether a paid
invoice for the intended price matches a positive charge in Stripe Balance
activity. Use the full live history from launch through the last month under
review. That history lets the report attribute later adjustments and find each
customer's first observed payment. Keep both exports outside the repository.

- Prepare one invoice-register CSV row per Stripe invoice. Include
  `invoice_id,customer_id,price_ids,livemode,status,currency,amount_paid_cents,tax_cents`.
  Use all invoice lines and join their price IDs with `|`, retaining an empty
  entry for any unpriced line. Copy the live flag and paid amount from the
  invoice. Enter tax in cents from the invoice's tax totals.
- Export itemized USD Stripe Balance activity with
  `balance_transaction_id,created_utc,currency,gross,fee,net,reporting_category,invoice_id,charge_id`.
  Use a decimal point for the USD amounts. Export all activity categories so
  refunds, reversals, and disputes stay visible. Include `source_id` to track
  separate refunds or disputes on the same charge. [Stripe Balance columns](https://docs.stripe.com/reports/report-types/balance)

```powershell
poetry run python scripts/commercial_cash_report.py --invoices C:/private/invoices.csv --balance C:/private/balance.csv --price-id price_replace_me
```

Repeat `--exclude-customer-id` for staff or other known internal accounts. The
report emits aggregates by UTC posting month and counts exceptions without
customer identifiers. The automatic match covers paid, live, single-price USD
invoices with zero tax and one charge for the exact paid amount. Refunds and
disputes must identify that charge and post at or after it. A reversal can
restore only the remaining principal and fees from the matching debit category
on that charge. When available, `source_id` must also match between debit and
credit. Excluded adjustments appear as `unmatched_charge_adjustments` or
`unreconciled_adjustment_credits` in the audit. Reconcile those exceptions,
excluded invoices, standalone fees, and taxes separately before relying on the
totals. The report measures Stripe balance activity. [Stripe refund failures](https://docs.stripe.com/refunds#failed-refunds) · [Stripe dispute resolution](https://docs.stripe.com/disputes/how-disputes-work#dispute-resolution)
Use actual business costs for operating profit.

### Measure first and second renewals

Use the [renewal report](../scripts/commercial_renewal_report.py) with the same
private invoice and Balance exports and a cycles CSV with one row per invoice.
The CSV header is
`invoice_id,subscription_id,billing_reason,period_start_utc,period_end_utc,proration,renewal_1_due_utc,renewal_2_due_utc`.
Copy the subscription ID and billing reason recorded on that invoice. Use the target
recurring invoice line's service period and proration flag. Use timestamps
with UTC offsets and `true` or `false` for proration. [Stripe invoice lines](https://docs.stripe.com/api/invoice-line-item/object)

Fill both due dates on each customer's first cash-matched invoice. The first
due date is the end of that invoice line's service period. Set the second
from the original monthly billing anchor and retain both dates after
cancellation. Leave the due fields blank on later invoices. [Stripe billing cycles](https://docs.stripe.com/billing/subscriptions/billing-cycle)

Verify that all exports come from the same live account and cover the history
from launch through `--observed-through`, including all invoice pages and
relevant Balance activity. The report relies on the operator's coverage declaration.
Choose `--as-of` within the export coverage and set the grace window before
reviewing results. Keep customer-level exports outside the repository.

```powershell
poetry run python -m scripts.commercial_renewal_report --invoices C:/private/invoices.csv --balance C:/private/balance.csv --cycles C:/private/cycles.csv --price-id price_replace_me --history-start 2027-01-01T00:00:00Z --as-of 2027-04-08T00:00:00Z --observed-through 2027-04-09T00:00:00Z --grace-days 7
```

The cohort month is the first collected charge's UTC month.
Each rate uses the original first-payer cohort members whose scheduled slot
and grace window have elapsed, including cancellations. The report counts one
cash-matched payment per original subscription and service period and shows
late recoveries separately. Reconcile missing schedules and other audit
exceptions before using the rates. An incomplete report suppresses rates.

## Paid launch evidence

- A current approved and passing pick rule and release audit under the existing claim policy. [Claim evidence](CLAIM_EVIDENCE.md)
- Merchant eligibility for this exact subscription and target markets, confirmed with the processor, plus a verified live $29 USD monthly price and working checkout. Stripe's prohibited list covers sports forecasting with a monetary or material prize. Eligibility for this analytics subscription still needs account-specific confirmation. [Stripe prohibited and restricted businesses](https://stripe.com/us/legal/restricted-businesses)
- The intended odds display falls within the provider terms. The Odds API permits commercial analytics and display and prohibits reselling raw data as a standalone feed. Review the customer odds endpoint in that context. [The Odds API terms](https://the-odds-api.com/terms-and-conditions.html)
- Collected revenue from independent customers, repeat payments from mature cohorts, and positive monthly operating results after actual costs.
