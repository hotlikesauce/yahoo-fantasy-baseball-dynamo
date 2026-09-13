"""The 12-category scoring engine for the two-week championship.

Ratio categories are the whole reason this exists: you cannot average seven
days of OPS or ERA. Raw components are summed across every day and every
started player, and the division happens once, at the end.
"""
HITTING = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS']
PITCHING = ['TB', 'ERA', 'WHIP', 'K9', 'QS', 'SVH']
CATS = HITTING + PITCHING

# TB here is Total Bases ALLOWED, so lower wins - same as superlatives_core.py.
LOWER_IS_BETTER = {'ERA', 'WHIP', 'TB'}

HIT_SLOTS = {'C', '1B', '2B', '3B', 'SS', 'OF', 'Util'}
PIT_SLOTS = {'SP', 'RP', 'P'}
BENCH_SLOTS = {'BN', 'IL', 'IL+', 'NA'}

# Miss the innings floor and all six pitching categories are forfeited. The
# league's weekly floor is 50, so a cumulative two-week title run needs 100.
MIN_IP_PER_WEEK = 50.0

BAT_COMPONENTS = ['R', 'H', 'HR', 'RBI', 'SB', 'AB', 'BB', 'HBP', 'SF', 'TB_bat']
PIT_COMPONENTS = ['TB', 'IP', 'ER', 'H_ALLOWED', 'BB_ALLOWED', 'K', 'QS', 'SVH']


def empty_totals():
    t = {k: 0 for k in BAT_COMPONENTS}
    t.update({k: 0 for k in PIT_COMPONENTS})
    t['IP'] = 0.0
    return t


def accumulate(totals, day_lines, lineup, player_index):
    """Add one day of a team's started players into its running totals.

    lineup: {player_key: slot} for that date.
    player_index: {player_key: player dict with mlb_id and stat_side}.
    day_lines: {mlb_id: {'bat': {...}, 'pit': {...}}} from mlb_api.boxscore_lines.
    """
    for player_key, slot in lineup.items():
        if slot in BENCH_SLOTS:
            continue
        player = player_index.get(player_key)
        if not player or not player.get('mlb_id'):
            continue
        line = day_lines.get(player['mlb_id'])
        if not line:
            continue
        side = player.get('stat_side')
        if slot in HIT_SLOTS and side != 'pit':
            for k in BAT_COMPONENTS:
                totals[k] += line['bat'].get(k, 0)
        elif slot in PIT_SLOTS and side != 'bat':
            for k in PIT_COMPONENTS:
                totals[k] += line['pit'].get(k, 0)
    return totals


def finalize(totals, weeks=2):
    """Turn summed components into the twelve scored category values."""
    ab, bb, hbp, sf = totals['AB'], totals['BB'], totals['HBP'], totals['SF']
    on_base_chances = ab + bb + hbp + sf
    ip = totals['IP']

    obp = (totals['H'] + bb + hbp) / on_base_chances if on_base_chances else 0.0
    slg = totals['TB_bat'] / ab if ab else 0.0

    return {
        'R': totals['R'], 'H': totals['H'], 'HR': totals['HR'],
        'RBI': totals['RBI'], 'SB': totals['SB'],
        'OPS': round(obp + slg, 4),
        'TB': totals['TB'],
        'ERA': round(9 * totals['ER'] / ip, 4) if ip else 0.0,
        'WHIP': round((totals['BB_ALLOWED'] + totals['H_ALLOWED']) / ip, 4) if ip else 0.0,
        'K9': round(9 * totals['K'] / ip, 4) if ip else 0.0,
        'QS': totals['QS'], 'SVH': totals['SVH'],
        'IP': round(ip, 2),
        'ip_minimum': MIN_IP_PER_WEEK * weeks,
        'forfeits_pitching': ip < MIN_IP_PER_WEEK * weeks,
    }


def matchup(a_final, b_final):
    """Compare two finalized stat lines. Returns per-category winners.

    A team under the innings floor loses every pitching category outright, even
    ones it would otherwise have won.
    """
    result = {}
    for cat in CATS:
        av, bv = a_final[cat], b_final[cat]
        a_out = cat in PITCHING and a_final['forfeits_pitching']
        b_out = cat in PITCHING and b_final['forfeits_pitching']
        if a_out and not b_out:
            winner = 'b'
        elif b_out and not a_out:
            winner = 'a'
        elif a_out and b_out:
            winner = 'tie'
        elif av == bv:
            winner = 'tie'
        elif cat in LOWER_IS_BETTER:
            # A team with zero innings has an ERA of 0.0, which must not read
            # as the best possible mark.
            if cat in ('ERA', 'WHIP') and (a_final['IP'] == 0 or b_final['IP'] == 0):
                winner = 'a' if b_final['IP'] == 0 else 'b'
            else:
                winner = 'a' if av < bv else 'b'
        else:
            winner = 'a' if av > bv else 'b'
        result[cat] = {'a': av, 'b': bv, 'winner': winner}

    a_wins = sum(1 for c in result.values() if c['winner'] == 'a')
    b_wins = sum(1 for c in result.values() if c['winner'] == 'b')
    ties = sum(1 for c in result.values() if c['winner'] == 'tie')
    return {'categories': result, 'a_cats': a_wins, 'b_cats': b_wins, 'ties': ties,
            'leader': 'a' if a_wins > b_wins else ('b' if b_wins > a_wins else 'tie')}


def decide(board, tiebreak_side=None):
    """Who wins the title from a finished matchup board.

    board: the dict returned by matchup(). tiebreak_side: the frozen ruling for
    this pairing from tiebreak.resolve() - 'a', 'b' or 'split' - used only when
    the two teams won the same number of categories. That covers 6-6 and also a
    5-5 with two categories dead level, which is just as much a tie.
    """
    if board['a_cats'] > board['b_cats']:
        return {'winner': 'a', 'decided_by': 'categories'}
    if board['b_cats'] > board['a_cats']:
        return {'winner': 'b', 'decided_by': 'categories'}
    if tiebreak_side in ('a', 'b'):
        return {'winner': tiebreak_side, 'decided_by': 'tiebreak'}
    return {'winner': 'split', 'decided_by': 'tiebreak'}
