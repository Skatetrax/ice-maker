"""Parser for the Figure Skating Fandom wiki's US ice rink directory.

Source: https://figure-skating.fandom.com/wiki/List_of_ice_rinks_in_the_USA

This page is a curated, community-maintained list organised by state
with separate sections for active and defunct rinks.  Each state has
one or two MediaWiki tables (active, and optionally defunct) whose
columns vary slightly across states.

The Cloudflare-protected front end is bypassed by using the MediaWiki
parse API, which returns the rendered HTML directly as JSON.

Key parsing challenges handled here:
    - rowspan on City / County cells when a city has multiple rinks
    - variable column headers (County vs Borough vs Parish, Notes
      sometimes absent, Club vs Affiliated Club)
    - Clubs / Data summary tables at the bottom that are NOT rink data
    - rink website URLs embedded as <a> tags inside the Rink column
"""

import logging
import requests
from bs4 import BeautifulSoup, NavigableString

logger = logging.getLogger(__name__)

API_URL = 'https://figure-skating.fandom.com/api.php'
PAGE_TITLE = 'List_of_ice_rinks_in_the_USA'
USER_AGENT = 'ice-maker/0.1 (skatetrax rink directory builder)'

SKIP_SECTIONS = frozenset({
    'Clubs', 'Defunct Clubs', 'Data', 'Sources',
    'Contents', 'References', 'External links',
})

COUNTY_SYNONYMS = frozenset({'County', 'Borough', 'Parish'})


def _fetch_html():
    """Fetch rendered HTML from the MediaWiki parse API."""
    params = {
        'action': 'parse',
        'page': PAGE_TITLE,
        'format': 'json',
        'prop': 'text',
    }
    resp = requests.get(
        API_URL, params=params, timeout=60,
        headers={'User-Agent': USER_AGENT},
    )
    resp.raise_for_status()
    return resp.json()['parse']['text']['*']


def _resolve_table(table):
    """Parse an HTML table into structured rows, expanding rowspans.

    Returns (headers, rows) where:
        headers – list of column-name strings
        rows    – list of lists; each inner list has one (text, href)
                  tuple per column.  href is the first <a> href in the
                  cell, or None.
    """
    raw_rows = table.find_all('tr')
    if not raw_rows:
        return [], []

    headers = [th.get_text(strip=True) for th in raw_rows[0].find_all('th')]
    if not headers:
        return [], []

    ncols = len(headers)
    grid = []
    # Each slot is either None (free) or (rows_remaining, cell_value).
    active = [None] * ncols

    for tr in raw_rows[1:]:
        cells = tr.find_all(['td', 'th'])
        row = [('', None)] * ncols
        ci = 0  # index into *cells*

        for col in range(ncols):
            if active[col] is not None:
                remaining, val = active[col]
                row[col] = val
                active[col] = (remaining - 1, val) if remaining > 1 else None
            elif ci < len(cells):
                cell = cells[ci]
                ci += 1
                text = cell.get_text(separator=' | ', strip=True)
                link = cell.find('a')
                href = None
                if link:
                    h = link.get('href', '')
                    if h.startswith('http'):
                        href = h
                row[col] = (text, href)
                rs = int(cell.get('rowspan', 1))
                if rs > 1:
                    active[col] = (rs - 1, (text, href))

        grid.append(row)

    return headers, grid


def _map_columns(headers):
    """Return a dict mapping semantic role → column index.

    Recognised roles: city, county, rink, club, notes.
    Returns None if the table has no 'rink' column (not a rink table).
    """
    col = {}
    for i, h in enumerate(headers):
        h = h.strip()
        if h == 'City':
            col['city'] = i
        elif h in COUNTY_SYNONYMS:
            col['county'] = i
        elif h in ('Rink', 'Name'):
            col['rink'] = i
        elif 'Club' in h:
            col['club'] = i
        elif h == 'Notes':
            col['notes'] = i

    return col if 'rink' in col else None


def _cell_text(row, col_map, role):
    """Safely extract the text component from a (text, href) cell."""
    idx = col_map.get(role)
    if idx is None or idx >= len(row):
        return ''
    return row[idx][0]


def _cell_href(row, col_map, role):
    """Safely extract the href component from a (text, href) cell."""
    idx = col_map.get(role)
    if idx is None or idx >= len(row):
        return None
    return row[idx][1]


def pull_fandom_wiki():
    """Fetch and parse the Fandom wiki list of US ice rinks.

    Returns a list of dicts with keys:
        name      – rink name
        city      – city / municipality
        state     – US state (full name as listed on the wiki)
        county    – county / borough / parish
        club      – affiliated figure skating club(s)
        notes     – free-text notes from the wiki
        website   – rink website URL (if linked)
        is_defunct – True if the rink appeared in a "Defunct Rinks" section
    """
    logger.info("fandom_wiki: fetching page via MediaWiki API")
    html = _fetch_html()
    soup = BeautifulSoup(html, 'html.parser')
    div = soup.find('div', class_='mw-parser-output')

    if not div:
        logger.error("fandom_wiki: content div not found in API response")
        return []

    results = []
    current_state = None
    is_defunct = False

    for el in div.children:
        if isinstance(el, NavigableString):
            continue

        if el.name == 'h2':
            span = el.find('span', class_='mw-headline')
            if not span:
                continue
            name = span.get_text(strip=True)
            if name in SKIP_SECTIONS:
                current_state = None
                continue
            current_state = name
            is_defunct = False

        elif el.name == 'h3':
            span = el.find('span', class_='mw-headline')
            if span and 'defunct' in span.get_text(strip=True).lower():
                is_defunct = True

        elif el.name == 'table' and current_state:
            headers, rows = _resolve_table(el)
            col_map = _map_columns(headers)
            if col_map is None:
                logger.debug(
                    "fandom_wiki: skipping non-rink table under %s (headers: %s)",
                    current_state, headers,
                )
                continue

            for row in rows:
                rink_name = _cell_text(row, col_map, 'rink')
                if not rink_name or rink_name.lower() == 'none':
                    continue

                results.append({
                    'name': rink_name,
                    'city': _cell_text(row, col_map, 'city'),
                    'state': current_state,
                    'county': _cell_text(row, col_map, 'county'),
                    'club': _cell_text(row, col_map, 'club'),
                    'notes': _cell_text(row, col_map, 'notes'),
                    'website': _cell_href(row, col_map, 'rink'),
                    'is_defunct': is_defunct,
                })

    active = sum(1 for r in results if not r['is_defunct'])
    defunct = sum(1 for r in results if r['is_defunct'])
    logger.info(
        "fandom_wiki: %d rinks collected (%d active, %d defunct)",
        len(results), active, defunct,
    )
    return results
