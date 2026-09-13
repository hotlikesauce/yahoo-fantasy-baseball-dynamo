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

# Acquisitions are capped at 10 during week two only (commissioner, 2026-09-13,
# replacing the earlier 20-across-both-weeks rule). Week one's Yahoo adds do not
# count; each finalist starts week two at 0. Free agents only, first come first
# served, from the Sept 20 snapshot.
MAX_ADDS = 10
ADDS_SPAN = 'week2_only'
DROP_LOCKOUT_HOURS = 24

# Commissioner's rulings, 2026-09-13.
#
# A player locks at his game's SCHEDULED first pitch, delay or not. MLB reports a
# rain-delayed game as Preview rather than Live, so the lock must key off the
# scheduled time - never the game state - or delayed players would stay open.
LOCK_AT = 'scheduled_first_pitch'

# If MLB marks the game Postponed or Cancelled (abstract state Final, but nobody
# played), the player unlocks for the rest of that day so he can be swapped out.
UNLOCK_ON_POSTPONEMENT = True

# Equal category wins (6-6, or 5-5 with two categories tied) go to the finalists'
# 2026 regular-season meetings, in this order:
#   1. head-to-head matchup record
#   2. total categories won across those head-to-head matchups
#   3. still level -> the pot is split
TIEBREAK = 'regular_season_h2h'
TIEBREAK_ORDER = ('h2h_matchups', 'h2h_categories', 'split')
REGULAR_SEASON_WEEKS = range(1, 23)          # playoffs begin week 23

# Every "what day is it" question - locks, live scoring, the baseline cut-off -
# is answered in US Eastern time, which is also MLB's official game date. UTC
# rolls to tomorrow at 8pm Eastern, mid-slate.
LEAGUE_TZ = 'America/New_York'


def league_now():
    from datetime import datetime, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(LEAGUE_TZ))
    except Exception:
        # No tz database (bare Windows Python). September 2026 is EDT throughout.
        return datetime.now(timezone(timedelta(hours=-4)))


def league_today():
    return league_now().date().isoformat()

TABLE = 'FantasyBaseball-ChampionshipHub'

# Week one's real lineups, captured nightly. A separate table on purpose: the
# nightly sandbox reset wipes the hub table, and must never be able to reach
# the record week one is scored from.
CAPTURE_TABLE = 'FantasyBaseball-ChampionshipCaptures'

# The two finalists, fixed once the semifinals settled (2026-09-13). Side 'a'
# is the higher seed.
FINALISTS = {'a': 3, 'b': 11}
FINALIST_MANAGERS = {3: 'Josh', 11: 'Kevin'}

# Week one is a testing playground on the real rosters: the hub runs the exact
# production code, and every move made through it is wiped nightly and reloaded
# from Yahoo. The freeze switches the table to production for week two, after
# which a reset refuses to run - by date, and by this mode flag, independently.
MODE_SANDBOX = 'sandbox'
MODE_PRODUCTION = 'production'
MODE_LOADING = 'loading'
SNAPSHOT_JSON = 'championship_2026.json'


def week2_dates():
    from datetime import date, timedelta
    start = date.fromisoformat(WEEK2_START)
    end = date.fromisoformat(WEEK2_END)
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
