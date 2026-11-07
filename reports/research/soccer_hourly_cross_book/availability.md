# Soccer hourly cross-book quote screen

The screen used the checksum-pinned 2016 Beat The Bookie archive. It covered 35 senior first-division labels in 33 UEFA countries outside England, France, Germany, Italy, and Spain. The scope was disjoint from the scored Big Five and second-division replays. Each of the 61,573 quote rows matched one metadata fixture with the same archived UTC kickoff.

At the six-hour marker, the screen required all three Bet365 prices to equal their seven-hour values and the largest increase in Pinnacle's proportional no-vig probability to be at least two percentage points. All four triplets had to contain decimal prices from 1.01 to 100 and an overround from 1.00 to 1.20. Home, draw, and away was the tie order. An actionable fixture required at least 2% modeled expected return at the observed Bet365 price and odds from 1.20 to 6.00. The later comparison used the same Bet365 triplet one hour before the archived kickoff.

| Outcome-blind counts | March through July | August through November 22 |
| --- | ---: | ---: |
| Scoped fixtures | 2,949 | 2,696 |
| Complete quotes at both markers | 1,813 | 1,901 |
| Bet365 triplet unchanged | 1,475 | 1,569 |
| Pinnacle move of at least two points | 86 | 82 |
| Both conditions | 32 | 31 |
| Both conditions on distinct UTC dates | 23 | 21 |
| Actionable at the Bet365 price | 7 | 7 |
| Actionable on distinct UTC dates | 6 | 6 |
| Actionable with a valid later Bet365 quote | 7 | 7 |

The later sample fell below the screen's 150 actionable fixtures and 30 UTC dates. The scarcity of joint quote movement and price advantage was the limiting factor. Score columns were excluded and the experiment did not advance to a scored protocol.

The author sampler carries forward the last active price at each hourly marker. The archive cannot verify original quote-update timing, live execution, or when Bet365 observed a Pinnacle move. The one-hour quote is a pre-kickoff proxy.

## Source league labels

```text
Austria: Tipico Bundesliga
Belgium: Jupiler League
Belarus: Vysshaya Liga
Bulgaria: A PFG
Bulgaria: Parva Liga
Croatia: 1. HNL
Cyprus: First Division
Czech Republic: Synot liga
Czech Republic: 1. Liga
Denmark: Superliga
Estonia: Meistriliiga
Finland: Veikkausliiga
Greece: Super League
Hungary: OTP Bank Liga
Iceland: Pepsideild
Ireland: Premier Division
Israel: Ligat ha'Al
Latvia: SynotTip Virsl?ga
Lithuania: A Lyga
Luxembourg: National Division
Netherlands: Eredivisie
Norway: Tippeligaen
Poland: Ekstraklasa
Portugal: Primeira Liga
Romania: Liga 1
Russia: Premier League
Scotland: Premiership
Serbia: Super Liga
Slovakia: Fortuna liga
Slovenia: Prva liga
Sweden: Allsvenskan
Switzerland: Super League
Turkey: Super Lig
Ukraine: Pari-Match League
Wales: Premier League
```

Data by Lisandro Kaunitz, Shenjun Zhong, and Javier Kreiner, [Beat The Bookie](https://www.kaggle.com/datasets/austro/beat-the-bookie-worldwide-football-dataset), [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). [Original sampler](https://github.com/Lisandro79/BeatTheBookie/blob/master/src/generate_odds_series_csv.php) · [Pinned archive checksums](../soccer_power_devig/protocol.json)
