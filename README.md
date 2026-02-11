# Summertime Sadness Fantasy Baseball

Analytics dashboard for the **Summertime Sadness** Yahoo Fantasy Baseball league. Est. 2007, entering Year 20 with 12 active managers.

Live site: [hotlikesauce.github.io/yahoo-fantasy-baseball-dynamo](https://hotlikesauce.github.io/yahoo-fantasy-baseball-dynamo/)

## Pages

| Page | Description | Generator |
|------|-------------|-----------|
| [Home](docs/index.html) | Dashboard with standings, power rankings, champions, title leaders | `gen_home.py` |
| [All-Time Records](docs/all_time_rankings.html) | Every manager's finish by year with champion markers | Hand-edited |
| [H2H Records](docs/h2h_records.html) | Manager vs manager head-to-head records (2023-2025), sortable matrix | `gen_h2h_records.py` |
| [Manager Profiles](docs/manager_profiles.html) | Career stats, power score charts, season history, H2H breakdowns per manager | `gen_manager_profiles.py` |
| [Trade Analyzer](docs/trade_analyzer.html) | In-season trade evaluator with roster context and category impact | `fetch_trade_data.py` |
| [Draft Capital](docs/draft_picks_2026.html) | 2026 draft pick distribution and value visualization | `gen_draft_picks.py` |
| [Season Trends (2025)](docs/season_trends_2025.html) | Power scores, xWins, batter/pitcher scatter, hot/cold, season bests | `gen_season_trends.py` |
| [Luck Analysis (2025)](docs/luck_analysis_2025.html) | Luck rankings, all-play H2H, blowouts, closest matchups | `gen_luck_analysis.py` |
| [Season Trends (2022-2024)](docs/) | Historical season trends from archived data | `gen_historical_trends.py` |

## Tech Stack

- **Data**: AWS DynamoDB (live season + historical), MongoDB Atlas (legacy archive)
- **Site**: Static HTML/CSS/JS hosted on GitHub Pages
- **Charts**: Chart.js
- **Scripts**: Python 3, boto3
- **API**: Yahoo Fantasy Sports API via yfpy (trade analyzer)

## DynamoDB Tables

| Table | Purpose |
|-------|---------|
| `FantasyBaseball-AllTimeRankings` | Every manager's finish by year (2007-2025) |
| `FantasyBaseball-SeasonTrends` | Live 2025 weekly data (power ranks, stats, results) |
| `FantasyBaseball-HistoricalSeasons` | Archived 2022-2025 weekly data (unified schema) |
| `FantasyBaseball-TeamInfo` | Current team number to name mappings |
| `FantasyBaseball-Schedule` | Game schedule data |

## H2H Scoring Categories (12)

Higher is better: R, H, HR, RBI, SB, OPS, K9, QS, SVH

Lower is better: ERA, WHIP, TB

## Scripts

**Page generators** (run to regenerate static HTML):
```bash
python scripts/gen_home.py
python scripts/gen_h2h_records.py
python scripts/gen_manager_profiles.py
python scripts/gen_season_trends.py          # 2025 (live data)
python scripts/gen_historical_trends.py 2023 # or 2024, 2022, all
python scripts/gen_luck_analysis.py
python scripts/gen_draft_picks.py
```

**Data scripts**:
```bash
python scripts/fetch_trade_data.py           # Yahoo API -> trade_data.json
python scripts/copy_2025_to_historical.py    # Snapshot live -> historical
python scripts/migrate_mongo_to_dynamo.py    # One-time MongoDB migration
python scripts/backfill_2023_scores.py       # One-time 2023 score backfill
```

**Config**:
- `scripts/team_config.py` - Team number to manager mappings by year
- `docs/nav.js` - Shared navigation bar injected into all pages

## Setup

```bash
pip install -r requirements.txt
```

AWS credentials must be configured (`aws configure` or env vars) with read access to the DynamoDB tables in `us-west-2`.

For the trade analyzer, create a `.env` with Yahoo OAuth credentials:
```
YAHOO_CLIENT_ID=your_consumer_key
YAHOO_CLIENT_SECRET=your_consumer_secret
```

## League Members

Austin, Bryant, Eric, Greg, James, Josh, Kevin, Kurtis, Mark, Mike, Mikey, Taylor

## License

[MIT License](https://choosealicense.com/licenses/mit/)
