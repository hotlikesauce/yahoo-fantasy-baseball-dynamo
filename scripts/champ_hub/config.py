"""Rules and dates for the 2026 two-week championship.

Yahoo only runs a one-week final, so week one is Yahoo's week 25 and week two
is hosted here. Everything the commissioner decided lives in this file so the
rules are stated once, not scattered through the engine.
"""
SEASON = 2026

# Week one is Yahoo's championship week and Yahoo scores it normally.
YAHOO_CHAMPIONSHIP_WEEK = 25
WEEK1_START = '2026-09-14'
WEEK1_END = '2026-09-20'

# Week two is ours. It is also the final week of the MLB regular season, which
# ends 2026-09-27 - there is no room to slip.
WEEK2_START = '2026-09-21'
WEEK2_END = '2026-09-27'

# Yahoo locks the whole league at week one's end, so the roster and free agent
# pool must be captured before this moment or they are gone for good.
FREEZE_AT = '2026-09-20T23:59:59-04:00'

# The title is one 12-category matchup over all 14 days, not best-of-two.
SCORING_MODE = 'cumulative'

# Miss the innings floor and all six pitching categories are forfeited. The
# league's weekly floor is 50; across two weeks that is 100.
MIN_IP_TOTAL = 100.0

# Free agents only, first come first served, from the pool frozen on Sept 20.
MAX_ADDS = 10
DROP_LOCKOUT_HOURS = 24

TABLE = 'FantasyBaseball-ChampionshipHub'
SNAPSHOT_JSON = 'championship_2026.json'


def week2_dates():
    from datetime import date, timedelta
    start = date.fromisoformat(WEEK2_START)
    end = date.fromisoformat(WEEK2_END)
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
