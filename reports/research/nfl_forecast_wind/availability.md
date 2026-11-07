# NFL forecast wind and totals feasibility

The proposed experiment would test whether a pregame wind forecast improves NFL totals probabilities beyond the same-time no-vig market at fixed open-air venues. The local archive audit checked quote and fixture availability on November 7, 2026 UTC. No score values were read.

The quote screen required one provider event ID matched to a unique final warehouse fixture, matching ordered teams, and kickoff clocks within ten minutes. The decision was the first valid DraftKings totals pair 6–36 hours before the earlier kickoff clock. Both prices had to share the same line and original snapshot, with market or book update time no more than ten minutes old. A close required the same checks and book, a strictly newer provider update and fetch, and an observation before kickoff in the final 90 minutes.

| DraftKings availability | Earlier than August 1, 2026 | August 1 onward |
| --- | ---: | ---: |
| Matched decision snapshots | 27 | 94 |
| Consistent archival outdoor venue joins | 10 | 59 |
| Strict close matches across all venue types | 2 | 0 |
| Strict close matches with outdoor venue joins | 0 | 0 |

Of 264 original responses with the verified NFL endpoint, 146 matched a registered filename and exact UTC fetch time. Including the additional raw responses left the table unchanged. Broadening entry to 1–72 hours produced 122 later fixtures and zero strict later closes. FanDuel, BetMGM, and BetRivers also yielded zero strict later closes. Eighteen of the 94 DraftKings entries had a newer quote within three hours of kickoff, all more than 90 minutes before kickoff.

The NOAA High-Resolution Rapid Refresh archive provides an independent 3 km forecast. Ordinary CONUS runs reach 18 hours and four daily extended runs reach 48 hours. Two unauthenticated metadata checks confirmed a 24-hour forecast object from the 18 UTC cycle and its indexed 10 m wind components. Their S3 archive timestamps were 19:23:45 UTC on November 15, 2025, and 19:21:40 UTC on October 24, 2026. The underlying GRIB bodies were not transferred. [NOAA model description](https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/hrrr.php) · [NOAA Open Data license and archive](https://registry.opendata.aws/noaa-hrrr-pds/)

To make the test runnable, retain both entry and later same-book pairs for more independently verified fixed outdoor fixtures. The weather join must bind each stadium to an independently sourced location and use only a forecast archived before the entry, with its valid hour matched to kickoff. The archival roof joins are an upper bound on fixed open-air venue eligibility. The project requires at least 150 graded decisions and at least 120 verified closes at that sample size. The available 59 later outdoor entries fall below that floor even with complete weather and close data.
