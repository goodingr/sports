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

The [interest page](../web-app/src/app/interest/page.tsx) is off by default. For a configured preview, set `COMMERCIAL_INTEREST_ENABLED=true` for the web server and make sure `NEXT_PUBLIC_APP_URL` matches its origin. Use a test Clerk instance for preview verification. The live pilot uses the production Clerk instance to count each signed-in account once per offer and to store opt-in and withdrawal in private metadata. Use links such as `/interest?source=community` and `/interest?source=referral` to attribute visits. Keep the withdrawal control available while using contact consents.

| Measure | Definition |
| --- | --- |
| Qualified visitor | A unique signed-in account from the intended audience that viewed the offer and price. Count separately by acquisition source. This denominator omits people who saw the offer without signing in. |
| Price-intent lead | A unique opt-in after seeing the $29 price. Keep the offer version and acquisition source. |
| First paying subscriber | A distinct customer with a first positive, collected live payment for the subscription. Exclude tests, comps, and out-of-band invoices unless reconciled to actual cash. |
| First and second renewal | Collected payments among first-time payers whose first and second renewal dates have elapsed. Compare by acquisition cohort. |
| Monthly contribution | Gross collected subscription charges less refunds, dispute losses, payment and billing fees, acquisition spend, and variable support. Record sales tax separately. |
| Monthly operating result | Monthly contribution less hosting, data, auth, backups, domains, monitoring, and fixed operating labor. |

For a first read, use a 30-day window with one offer version. Aim to show it to 200 qualified unique visitors and interview five opt-ins about the picks they want, how often they expect them, and why they would pay $29. Treat fewer than 20 opt-ins as a reason to inspect the audience and offer before investing in acquisition. These are exploratory targets. Record conversion counts by source and keep opt-ins separate from collected sales.

Use the [interest report](../scripts/commercial_interest_report.py) with the Clerk credentials for the relevant instance. Pass the cohort start to `--viewed-from` and its end to `--viewed-before` as ISO timestamps with UTC offsets. Repeat `--exclude-user-id` for team and test accounts. The report returns current opt-in and withdrawal counts for the selected first-view cohort by source. It emits aggregate counts without customer records.

After the release gates clear, export live Stripe Billing cohorts and the itemized Balance report for the same time zone and dates. Use the subscription price ID to isolate this product, reconcile charges, fees, refunds, and disputes by transaction, and calculate monthly contribution and operating result. Reconcile paid invoices to actual funds collected. Review two completed renewal cohorts before increasing acquisition spend. Keep customer-level exports out of the repository. [Stripe Billing analytics](https://docs.stripe.com/billing/subscriptions/analytics) · [Stripe Balance report](https://docs.stripe.com/reports/balance) · [Stripe event types](https://docs.stripe.com/api/events/types)

## Paid launch evidence

- A current approved and passing pick rule and release audit under the existing claim policy. [Claim evidence](CLAIM_EVIDENCE.md)
- Merchant eligibility for this exact subscription and target markets, confirmed with the processor, plus a verified live $29 USD monthly price and working checkout. Stripe's prohibited list covers sports forecasting with a monetary or material prize. Eligibility for this analytics subscription still needs account-specific confirmation. [Stripe prohibited and restricted businesses](https://stripe.com/us/legal/restricted-businesses)
- The intended odds display falls within the provider terms. The Odds API permits commercial analytics and display and prohibits reselling raw data as a standalone feed. Review the customer odds endpoint in that context. [The Odds API terms](https://the-odds-api.com/terms-and-conditions.html)
- Collected revenue from independent customers, repeat payments from mature cohorts, and positive monthly operating results after actual costs.
