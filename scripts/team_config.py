"""
Shared team number -> manager mappings for all scripts.

Team numbers are stable within a season but shuffle across years
when Yahoo resets the league. Team NAMES change frequently (multiple
times per day sometimes), so always use team number as the identifier.

To add a new season: add entries to YEAR_TN_TO_MANAGER below.
"""

# Canonical manager list (sorted alphabetically for stable ordering)
MANAGERS = [
    'Austin', 'Bryant', 'Eric', 'Greg', 'James', 'Josh',
    'Kevin', 'Kurtis', 'Mark', 'Mike', 'Mikey', 'Taylor',
]

# (year, team_number_string) -> manager name
# Team numbers are strings to match DynamoDB data format
YEAR_TN_TO_MANAGER = {
    # 2023
    (2023, '1'): 'Taylor',   # Heimlich Maneuver, Blackout Rage Gallen, McLainBang
    (2023, '2'): 'Austin',   # Moniebol
    (2023, '3'): 'Kurtis',   # Vinnie Pepperonis
    (2023, '4'): 'Bryant',   # TAX THE MONIEBOL, Bry Bry's Bible Bonkers
    (2023, '5'): 'Greg',     # [ABN] Rain, [z-ro], Mo City Don
    (2023, '6'): 'Josh',     # The Slobberknockers, Dollar General
    (2023, '7'): 'Eric',     # Ian Cumsler
    (2023, '8'): 'Mark',     # Movin' On Up, FIRE SALE
    (2023, '9'): 'James',    # Camp RFK, Camp Bichette
    (2023, '10'): 'Kevin',   # The Rosterbation Station
    (2023, '11'): 'Mikey',   # scoopski potatoes, ACES 4 DAAAAAYS
    (2023, '12'): 'Mike',    # \u00af\\_(\u30c4)_/\u00af, Canned Tuna
    # 2024
    (2024, '1'): 'Taylor',   # Pfaadt Tatis, Girthy Bohmer, O'Hoppe-timists
    (2024, '2'): 'James',    # FUCK TAYLOR HAMM, #JTimeFakeNews
    (2024, '3'): 'Bryant',   # #RyderSources, Sexual Harassment Pandas, Whale Tails
    (2024, '4'): 'Mark',     # Hatfield Hurlers
    (2024, '5'): 'Eric',     # Ian Cumsler
    (2024, '6'): 'Greg',     # OGglass-z13, OGnewnew4uu
    (2024, '7'): 'Austin',   # Moniebol
    (2024, '8'): 'Mikey',    # BTHO, CAPTAIN AHAB, basketball season
    (2024, '9'): 'Josh',     # Grand Salami Time
    (2024, '10'): 'Kevin',   # The Rosterbation Station
    (2024, '11'): 'Kurtis',  # Ready to Plow
    (2024, '12'): 'Mike',    # \u00af\\_(\u30c4)_/\u00af\U0001f3c6, I believe Ohtani
    # 2025
    (2025, '1'): 'Taylor',   # Serafini Hit Squad
    (2025, '2'): 'James',    # Tegridy
    (2025, '3'): 'Josh',     # Grand Salami Time
    (2025, '4'): 'Mark',     # Hatfield Hurlers
    (2025, '5'): 'Eric',     # Ian Cumsler
    (2025, '6'): 'Bryant',   # Football Szn
    (2025, '7'): 'Austin',   # Moniebol
    (2025, '8'): 'Greg',     # OG9
    (2025, '9'): 'Kurtis',   # Getting Plowed Again.
    (2025, '10'): 'Kevin',   # The Rosterbation Station
    (2025, '11'): 'Mike',    # \u00af\\_(\u30c4)_/\u00af
    (2025, '12'): 'Mikey',   # @DoodlesAnalytics
    # 2026
    (2026, '1'): 'Taylor',   # Serafini Hit Squad
    (2026, '2'): 'James',    # WEMBY SZN
    (2026, '3'): 'Josh',     # Floppy Salami Time
    (2026, '4'): 'Bryant',   # Dan Doodletard
    (2026, '5'): 'Kurtis',   # Getting Plowed Again.
    (2026, '6'): 'Mark',     # Hatfield Hurlers
    (2026, '7'): 'Eric',     # Ian Cumsler
    (2026, '8'): 'Austin',   # Moniebol
    (2026, '9'): 'Greg',     # OG9
    (2026, '10'): 'Mikey',   # SQUEEZE AGS
    (2026, '11'): 'Kevin',   # The Rosterbation Station
    (2026, '12'): 'Mike',    # \u00af\\_(\u30c4)_/\u00af
}


def get_manager(year, team_number):
    """Look up manager by year and team number.

    Args:
        year: int (e.g. 2026)
        team_number: int or str (e.g. 5 or '5')

    Returns:
        Manager name string, or 'Team {n}' if not found.
    """
    return YEAR_TN_TO_MANAGER.get((int(year), str(team_number)), f'Team {team_number}')


def get_tn_to_manager(year):
    """Get a dict of {team_number_int: manager_name} for a given year."""
    return {
        int(tn): mgr
        for (y, tn), mgr in YEAR_TN_TO_MANAGER.items()
        if y == int(year)
    }
