"""
Lambda: Scrape 2027 traded draft picks from Yahoo Fantasy public page.
Stores computed draft capital in DynamoDB.
Triggered by: CloudWatch Events - run nightly (cron(0 8 * * ? *))
URL: https://baseball.fantasysports.yahoo.com/b1/8614/showtradedpicks?view=next
"""

import json
import logging
import re
import boto3
import requests
from datetime import datetime
from html.parser import HTMLParser

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-SeasonTrends')

SCRAPE_URL = 'https://baseball.fantasysports.yahoo.com/b1/8614/showtradedpicks?view=next'

TOTAL_ROUNDS  = 22
NUM_TEAMS     = 12
KEEPER_ROUNDS = 2
STANDARD_CAPITAL = 1000
DECAY = 0.98

COLORS = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]

DRAFT_ROUNDS = TOTAL_ROUNDS - KEEPER_ROUNDS
TOTAL_PICKS  = DRAFT_ROUNDS * NUM_TEAMS
total_raw = sum(DECAY ** i for i in range(TOTAL_PICKS))
SCALE = STANDARD_CAPITAL * NUM_TEAMS / total_raw


def overall_pick(rnd, pos):
    return (rnd - KEEPER_ROUNDS - 1) * NUM_TEAMS + pos


def pick_value(overall):
    raw = SCALE * DECAY ** (overall - 1)
    return round(raw) if raw >= 0.5 else 0


def avg_round_value(rnd):
    base = overall_pick(rnd, 1)
    return sum(pick_value(base + i) for i in range(NUM_TEAMS)) / NUM_TEAMS


p1_val   = pick_value(1)
p12_val  = pick_value(12)
p49_val  = pick_value(overall_pick(7, 1))
p240_val = pick_value(240)


class PicksTableParser(HTMLParser):
    """Parse the showtradedpicks page. Extracts rows from the picks table."""

    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row   = False
        self.in_cell  = False
        self.depth    = 0
        self.table_depth = None
        self.current_row = []
        self.current_cell = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        self.depth += 1
        attrs = dict(attrs)
        if tag == 'table':
            if not self.in_table:
                self.in_table = True
                self.table_depth = self.depth
        elif tag in ('tr',) and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ('td', 'th') and self.in_row:
            self.in_cell = True
            self.current_cell = []

    def handle_endtag(self, tag):
        if tag == 'table' and self.in_table and self.depth == self.table_depth:
            self.in_table = False
            self.table_depth = None
        elif tag == 'tr' and self.in_row:
            self.in_row = False
            if self.current_row:
                self.rows.append(self.current_row[:])
            self.current_row = []
        elif tag in ('td', 'th') and self.in_cell:
            self.in_cell = False
            text = ' '.join(self.current_cell).strip()
            self.current_row.append(text)
            self.current_cell = []
        self.depth -= 1

    def handle_data(self, data):
        if self.in_cell:
            t = data.strip()
            if t:
                self.current_cell.append(t)


def scrape_picks(html):
    """
    Parse the showtradedpicks page HTML and return {team_name: [picks_per_round]}.

    Yahoo's page structure has a table where:
    - First column: team name
    - Subsequent columns: one per round (R1..R22)
    - Cell content: team name of current pick owner (or blank/dash if standard)

    We parse ALL tables and pick the one that looks like a picks grid
    (has round headers matching R1..R22 or '1'..'22').
    """

    # Log first 4000 chars so we can debug structure if needed
    logger.info("HTML preview: " + html[:4000].replace('\n', ' '))

    # Try to find the picks table — look for a table containing round-number headers
    # Yahoo typically renders this as a grid: rows=teams, cols=rounds
    # Header row cells will be like "1", "2", ... "22" or "R1", "R2", etc.

    # Split into tables by finding <table ...> ... </table> blocks
    table_blocks = re.findall(r'<table[^>]*>.*?</table>', html, re.DOTALL | re.IGNORECASE)
    logger.info(f"Found {len(table_blocks)} table blocks")

    picks_table = None
    header_round_offset = None

    for i, block in enumerate(table_blocks):
        parser = PicksTableParser()
        try:
            parser.feed(block)
        except Exception:
            continue
        if not parser.rows:
            continue

        # Look for a header row with round numbers
        for row in parser.rows[:5]:
            nums = []
            for cell in row:
                c = cell.strip().lstrip('R').lstrip('r')
                try:
                    n = int(c)
                    nums.append(n)
                except ValueError:
                    pass
            if len(nums) >= 15:  # at least 15 round columns found
                logger.info(f"Table {i}: round header found with {len(nums)} rounds. First row: {row[:5]}")
                picks_table = parser.rows
                # Figure out which column offset the rounds start at
                header_row = row
                header_round_offset = None
                for ci, cell in enumerate(header_row):
                    c = cell.strip().lstrip('R').lstrip('r')
                    try:
                        if int(c) == 1:
                            header_round_offset = ci
                            break
                    except ValueError:
                        pass
                break
        if picks_table:
            break

    if not picks_table:
        # Fallback: log all table row samples so we can debug
        for i, block in enumerate(table_blocks):
            parser = PicksTableParser()
            try:
                parser.feed(block)
            except Exception:
                continue
            if parser.rows:
                logger.info(f"Table {i} sample rows: {parser.rows[:3]}")
        raise ValueError("Could not find picks grid table in page HTML")

    # Parse team rows
    # header_round_offset = column index where R1 data starts
    # Rows after the header: col 0 = team name, cols [offset..offset+TOTAL_ROUNDS-1] = pick owner

    pick_counts = {}  # team_name -> [count] * TOTAL_ROUNDS

    # Collect all team names first (skip header rows)
    header_seen = False
    for row in picks_table:
        if not row:
            continue
        # Skip header rows (cells that are all round numbers or empty)
        if not header_seen:
            nums = sum(1 for c in row if re.match(r'^[Rr]?\d+$', c.strip()))
            if nums >= 10:
                header_seen = True
                continue

        if not header_seen:
            continue

        team_name = row[0].strip() if row else ''
        if not team_name or len(team_name) < 2:
            continue

        counts = [0] * TOTAL_ROUNDS

        if header_round_offset is not None:
            # Read pick owner cells
            for rnd_idx in range(TOTAL_ROUNDS):
                col = header_round_offset + rnd_idx
                if col >= len(row):
                    break
                cell = row[col].strip()
                # Non-empty cell = this team owns a pick in this round
                # Some pages use '-' or '*' for traded away
                if cell and cell not in ('-', '—', ''):
                    counts[rnd_idx] += 1
        else:
            # No offset detected — just count non-empty cells after col 0
            for cell in row[1:1 + TOTAL_ROUNDS]:
                counts[row[1:1 + TOTAL_ROUNDS].index(cell)] += 1 if cell.strip() else 0

        if any(c > 0 for c in counts):
            pick_counts[team_name] = counts
            logger.info(f"  {team_name}: total={sum(counts)} picks")

    if not pick_counts:
        raise ValueError("No team pick data extracted from picks table")

    return pick_counts


def compute_capital(pick_counts):
    return round(sum(
        pick_counts[rnd - 1] * avg_round_value(rnd)
        for rnd in range(KEEPER_ROUNDS + 1, TOTAL_ROUNDS + 1)
    ))


def build_result(pick_counts_by_team):
    team_data = []
    for name, picks in pick_counts_by_team.items():
        capital = compute_capital(picks)
        team_data.append({
            'name': name,
            'picks': picks,
            'totalPicks': sum(picks),
            'capital': capital,
            'capitalVsStd': capital - STANDARD_CAPITAL,
            'extraHigh': sum(max(0, picks[i] - 1) for i in range(10)),
            'tradedAway': sum(1 for c in picks if c == 0),
        })

    team_data.sort(key=lambda x: x['capital'], reverse=True)
    for i, t in enumerate(team_data):
        t['color'] = COLORS[i % len(COLORS)]

    return {
        'generated': datetime.utcnow().isoformat() + 'Z',
        'totalRounds': TOTAL_ROUNDS,
        'keeperRounds': KEEPER_ROUNDS,
        'standardCapital': STANDARD_CAPITAL,
        'pickValues': {'p1': p1_val, 'p12': p12_val, 'p49': p49_val, 'p240': p240_val},
        'teams': team_data,
    }


def lambda_handler(event, context):
    try:
        # Get OAuth2 access token so Yahoo accepts our request
        secrets = yfl.get_secrets()
        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get Yahoo OAuth access token")

        logger.info(f"Scraping: {SCRAPE_URL}")
        headers = {
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        resp = requests.get(SCRAPE_URL, headers=headers, timeout=15)
        resp.raise_for_status()
        html = resp.text
        logger.info(f"Fetched page: {len(html)} chars, status {resp.status_code}")

        pick_counts = scrape_picks(html)
        result = build_result(pick_counts)

        # Store in DynamoDB
        table.put_item(Item={
            'TeamNumber': '0',
            'DataType#Week': 'computed#draft_capital',
            'Year': 2026,
            'Data': json.dumps(result, ensure_ascii=False),
            'Timestamp': datetime.utcnow().isoformat(),
        })

        logger.info(f"Stored draft capital for {len(result['teams'])} teams")
        return {'statusCode': 200, 'body': f"Computed draft capital: {len(result['teams'])} teams"}

    except Exception as e:
        logger.error(f"compute_draft_capital FAILED: {e}", exc_info=True)
        return {'statusCode': 500, 'body': str(e)}
