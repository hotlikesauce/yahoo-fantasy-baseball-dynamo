# MongoDB to DynamoDB Collection Mapping

## Current MongoDB Collections (24 total)

### Live/Current Data (latest snapshot, overwrite pattern)
1. **live_standings** - Current standings
2. **playoff_status** - Current playoff status
3. **team_dict** - Team ID to name mapping
4. **remaining_sos** - Remaining strength of schedule
5. **seasons_best_long** - Season bests (long format)
6. **seasons_best_regular** - Season bests (regular format)
7. **playoff_probabilities** - Current playoff probabilities
8. **playoff_probabilities_static** - Static playoff probabilities

### Power Rankings (various calculations)
9. **power_ranks** - Main power rankings
10. **Power_Ranks** - Alternate power rankings (capitalized)
11. **power_ranks_lite** - Lightweight power rankings
12. **normalized_ranks** - Normalized rankings

### Coefficients
13. **coefficient** - Main coefficient data
14. **Coefficient_Last_Four** - Last 4 weeks coefficient
15. **Coefficient_Last_Two** - Last 2 weeks coefficient

### Weekly/Historical Data (week-by-week, append pattern)
16. **weekly_results** - Weekly matchup results
17. **weekly_stats** - Weekly statistics
18. **week_stats** - Week statistics (alternate)
19. **weekly_luck_analysis** - Weekly luck analysis
20. **running_normalized_ranks** - Running normalized ranks by week
21. **power_ranks_season_trend** - Power ranks over the season
22. **standings_season_trend** - Standings trend over season
23. **Running_ELO** - ELO ratings by week

### Schedule
24. **schedule** - League schedule

---

## Proposed DynamoDB Table Structure

### Option 1: Keep 5 Tables (Current Design)
- **LiveData** - All current/latest data (collections 1-15)
- **WeeklyTimeSeries** - All week-by-week historical (collections 16-23)
- **MatchupResults** - Weekly matchup results
- **Schedule** - League schedule
- **AllTimeHistory** - Cross-season data

### Option 2: More Granular (Recommended - 8-10 Tables)
Let's create a table for each logical grouping:

1. **Standings** - live_standings, playoff_status, playoff_probabilities
2. **PowerRankings** - power_ranks, Power_Ranks, power_ranks_lite, normalized_ranks
3. **Coefficients** - coefficient, Coefficient_Last_Four, Coefficient_Last_Two
4. **TeamData** - team_dict, remaining_sos, seasons_best_long, seasons_best_regular
5. **WeeklyResults** - weekly_results, weekly_luck_analysis
6. **WeeklyStats** - weekly_stats, week_stats
7. **SeasonTrends** - running_normalized_ranks, power_ranks_season_trend, standings_season_trend, Running_ELO
8. **Schedule** - schedule
9. **PlayoffData** - playoff_probabilities_static (if different from current)

### Option 3: 1-to-1 Mapping (24 Tables)
Create one DynamoDB table per MongoDB collection (simplest migration)

---

## Questions for Design:
1. Which option do you prefer?
2. Do some collections have similar schemas that should be grouped?
3. Are there collections that are rarely used and could be combined?
4. Which collections are most frequently queried together?
