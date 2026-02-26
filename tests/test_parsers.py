"""Tests for all 4 parsers against saved HTML/JSON fixtures."""

import json
import pytest
import responses
from pathlib import Path

FIXTURES = Path(__file__).parent / 'fixtures'


class TestSk8stuff:
    @responses.activate
    def test_parses_rinks_from_fixture(self):
        html = (FIXTURES / 'sk8stuff_page.html').read_text()
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body=html,
            status=200,
        )
        from parsers.sk8stuff import pull_sk8stuff
        rinks = pull_sk8stuff()

        names = [r['name'] for r in rinks]
        assert 'Polar Ice Raleigh' in names
        assert 'Greensboro Ice House' in names
        assert 'Twin Rinks of Pasadena' in names

    @responses.activate
    def test_junk_rink_filtered(self):
        html = (FIXTURES / 'sk8stuff_page.html').read_text()
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body=html,
            status=200,
        )
        from parsers.sk8stuff import pull_sk8stuff
        rinks = pull_sk8stuff()

        names = [r['name'] for r in rinks]
        assert 'Junk Rink Test' not in names

    @responses.activate
    def test_empty_name_filtered(self):
        html = (FIXTURES / 'sk8stuff_page.html').read_text()
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body=html,
            status=200,
        )
        from parsers.sk8stuff import pull_sk8stuff
        rinks = pull_sk8stuff()

        for r in rinks:
            assert r['name'], "Empty name should be filtered out"

    @responses.activate
    def test_rink_structure(self):
        html = (FIXTURES / 'sk8stuff_page.html').read_text()
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body=html,
            status=200,
        )
        from parsers.sk8stuff import pull_sk8stuff
        rinks = pull_sk8stuff()

        for r in rinks:
            assert 'name' in r
            assert 'street' in r
            assert 'city' in r
            assert 'state' in r

    @responses.activate
    def test_short_rows_skipped(self):
        html = (FIXTURES / 'sk8stuff_page.html').read_text()
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body=html,
            status=200,
        )
        from parsers.sk8stuff import pull_sk8stuff
        rinks = pull_sk8stuff()
        # "Short Row" has only 2 cells, should be skipped
        names = [r['name'] for r in rinks]
        assert 'Short Row' not in names


class TestArenaGuide:
    @responses.activate
    def test_extracts_cards_from_page(self):
        from parsers.arena_guide import _clean_address
        import bs4

        html = (FIXTURES / 'arena_guide_page.html').read_text()
        soup = bs4.BeautifulSoup(html, 'lxml')
        cards = soup.find_all('div', class_='jet-listing-grid__item')

        rinks = []
        for card in cards:
            name_tag = card.find('h2')
            addr_spans = card.find_all('span', class_='elementor-icon-list-text')
            name = name_tag.text.strip() if name_tag else None
            address = None
            for span in addr_spans:
                cleaned = _clean_address(span.text)
                if cleaned:
                    address = cleaned
                    break
            if name and address:
                rinks.append({'name': name, 'address': address})

        names = [r['name'] for r in rinks]
        assert 'Polar Ice Cary' in names
        assert 'Greensboro Ice House' in names
        assert 'Country Suffix Rink' in names

    def test_clean_address_strips_country(self):
        from parsers.arena_guide import _clean_address
        assert _clean_address("500 Main St, Springfield, IL United States") == \
            "500 Main St, Springfield, IL"

    def test_clean_address_strips_zip(self):
        from parsers.arena_guide import _clean_address
        result = _clean_address("500 Main St, Springfield, IL 62701")
        assert '62701' not in result

    def test_clean_address_rejects_urls(self):
        from parsers.arena_guide import _clean_address
        assert _clean_address("https://example.com/rink") is None

    def test_skipped_cards_no_address(self):
        """Cards with only a URL in the span should have address=None."""
        from parsers.arena_guide import _clean_address
        assert _clean_address("https://only-a-url.com") is None


class TestLearntoskate:
    @responses.activate
    def test_parses_programs_from_fixture(self):
        fixture = (FIXTURES / 'learntoskate_response.json').read_text()
        data = json.loads(fixture)

        responses.add(
            responses.POST,
            'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch',
            json=data,
            status=200,
        )

        from parsers.learntoskate import pull_lts_data, _build_session
        session = _build_session()
        programs = pull_lts_data(session, 1)

        assert len(programs) == 5

    @responses.activate
    def test_aggr_filters_incomplete(self):
        fixture = (FIXTURES / 'learntoskate_response.json').read_text()
        data = json.loads(fixture)

        for state_id in range(1, 51):
            responses.add(
                responses.POST,
                'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch',
                json=data if state_id == 1 else {'programs': []},
                status=200,
            )

        from parsers.learntoskate import aggr_lts
        results = aggr_lts()

        # 2 incomplete entries should be skipped (missing street / missing city)
        assert len(results) == 3

        names = [r['name'] for r in results]
        assert 'Polar Ice Garner LTS' in names
        assert 'Missing Street Org' not in names
        assert 'No City Org' not in names

    @responses.activate
    def test_extracts_zip_and_coords(self):
        fixture = (FIXTURES / 'learntoskate_response.json').read_text()
        data = json.loads(fixture)

        for _ in range(50):
            responses.add(
                responses.POST,
                'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch',
                json=data if _ == 0 else {'programs': []},
                status=200,
            )

        from parsers.learntoskate import aggr_lts
        results = aggr_lts()

        garner = next(r for r in results if 'Garner' in r['name'])
        assert garner['zip'] == '27529'
        assert garner['lat'] == 35.7113
        assert garner['lng'] == -78.618

    @responses.activate
    def test_fallback_lat_lng_keys(self):
        fixture = (FIXTURES / 'learntoskate_response.json').read_text()
        data = json.loads(fixture)

        for _ in range(50):
            responses.add(
                responses.POST,
                'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch',
                json=data if _ == 0 else {'programs': []},
                status=200,
            )

        from parsers.learntoskate import aggr_lts
        results = aggr_lts()

        durham = next(r for r in results if 'Complete' in r['name'])
        assert durham['lat'] == 35.994
        assert durham['lng'] == -78.8986


class TestFandomWiki:
    @responses.activate
    def test_parses_rinks_from_fixture(self):
        html = (FIXTURES / 'fandom_wiki_response.html').read_text()
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            json={'parse': {'text': {'*': html}}},
            status=200,
        )

        from parsers.fandom_wiki import pull_fandom_wiki
        results = pull_fandom_wiki()

        names = [r['name'] for r in results]
        assert 'Polar Iceplex' in names
        assert 'Greensboro Ice House' in names
        assert 'Ben Boeke Ice Arena' in names

    @responses.activate
    def test_rowspan_cities_handled(self):
        html = (FIXTURES / 'fandom_wiki_response.html').read_text()
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            json={'parse': {'text': {'*': html}}},
            status=200,
        )

        from parsers.fandom_wiki import pull_fandom_wiki
        results = pull_fandom_wiki()

        raleigh_rinks = [r for r in results if r['city'] == 'Raleigh']
        assert len(raleigh_rinks) == 2

    @responses.activate
    def test_defunct_flag_set(self):
        html = (FIXTURES / 'fandom_wiki_response.html').read_text()
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            json={'parse': {'text': {'*': html}}},
            status=200,
        )

        from parsers.fandom_wiki import pull_fandom_wiki
        results = pull_fandom_wiki()

        ice_palace = next(r for r in results if 'Ice Palace' in r['name'])
        assert ice_palace['is_defunct'] is True

    @responses.activate
    def test_non_rink_tables_skipped(self):
        html = (FIXTURES / 'fandom_wiki_response.html').read_text()
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            json={'parse': {'text': {'*': html}}},
            status=200,
        )

        from parsers.fandom_wiki import pull_fandom_wiki
        results = pull_fandom_wiki()

        # "Raleigh FSC" should NOT appear as a rink name --
        # it's in the Clubs table which should be skipped
        names = [r['name'] for r in results]
        assert 'Raleigh FSC' not in names

    @responses.activate
    def test_website_extracted(self):
        html = (FIXTURES / 'fandom_wiki_response.html').read_text()
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            json={'parse': {'text': {'*': html}}},
            status=200,
        )

        from parsers.fandom_wiki import pull_fandom_wiki
        results = pull_fandom_wiki()

        polar = next(r for r in results if r['name'] == 'Polar Iceplex')
        assert polar['website'] == 'https://polarice.com/raleigh'

    @responses.activate
    def test_state_names_assigned(self):
        html = (FIXTURES / 'fandom_wiki_response.html').read_text()
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            json={'parse': {'text': {'*': html}}},
            status=200,
        )

        from parsers.fandom_wiki import pull_fandom_wiki
        results = pull_fandom_wiki()

        nc_rinks = [r for r in results if r['state'] == 'North Carolina']
        ak_rinks = [r for r in results if r['state'] == 'Alaska']
        assert len(nc_rinks) >= 4
        assert len(ak_rinks) >= 1

    @responses.activate
    def test_club_extracted(self):
        html = (FIXTURES / 'fandom_wiki_response.html').read_text()
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            json={'parse': {'text': {'*': html}}},
            status=200,
        )

        from parsers.fandom_wiki import pull_fandom_wiki
        results = pull_fandom_wiki()

        polar = next(r for r in results if r['name'] == 'Polar Iceplex')
        assert 'Raleigh FSC' in polar['club']
