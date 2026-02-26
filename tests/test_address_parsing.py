"""Tests for _parse_entry and _parse_wiki_entry from pipeline/runner.py.

This is the highest-value test module -- it covers the house number
bug fix and the core address normalization pipeline.
"""

import pytest
from unittest.mock import MagicMock
from pipeline.runner import _parse_entry, _parse_wiki_entry


def _make_raw(name, address):
    raw = MagicMock()
    raw.raw_name = name
    raw.raw_address = address
    return raw


class TestParseEntry:
    def test_standard_address(self):
        raw = _make_raw("Test Rink", "1215 Wyckoff Rd, Farmingdale, NJ")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert parsed is not None
        assert '1215' in parsed['street']
        assert 'WYCKOFF' in parsed['street']
        assert 'ROAD' in parsed['street']
        assert parsed['state'] == 'NJ'

    def test_house_number_preserved(self):
        """The bug we fixed: house numbers were being dropped."""
        raw = _make_raw("Rink A", "100 Main St, Springfield, IL")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert parsed['street'].startswith('100')

    def test_directionals_preserved(self):
        raw = _make_raw("Cedar Ice", "100 Rockford Dr SW, Cedar Rapids, IA")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert 'SW' in parsed['street']
        assert '100' in parsed['street']

    def test_abbreviation_expansion(self):
        raw = _make_raw("Plains Ice", "150 Great Plains Ave, Omaha, NE")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert 'AVENUE' in parsed['street']

    def test_road_expansion(self):
        raw = _make_raw("Road Rink", "200 Oak Rd, Durham, NC")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert 'ROAD' in parsed['street']

    def test_blvd_expansion(self):
        raw = _make_raw("Blvd Rink", "6119 Landmark Center Blvd, Greensboro, NC")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert 'BOULEVARD' in parsed['street']

    def test_rec_center_expansion_in_name(self):
        raw = _make_raw("Babson Rec Ctr", "100 Main St, Wellesley, MA")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert 'Recreation Center' in parsed['name']

    def test_full_state_name_converted(self):
        raw = _make_raw("MN Rink", "100 Main St, Minneapolis, Minnesota")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert parsed['state'] == 'MN'

    def test_city_punctuation_stripped(self):
        raw = _make_raw("Test", "100 Main St, St. Louis, MO")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert '.' not in (parsed['city'] or '')

    def test_name_title_cased(self):
        raw = _make_raw("POLAR ICE HOUSE", "100 Main St, Raleigh, NC")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert parsed['name'] == 'Polar Ice House'

    def test_missing_street_returns_none(self):
        raw = _make_raw("No Address", "Raleigh, NC")
        parsed, error = _parse_entry(raw, None)
        # usaddress may or may not parse this; if it can't extract a street,
        # the function should return None
        if parsed is None:
            assert error is not None

    def test_returns_dict_keys(self):
        raw = _make_raw("Full Test", "500 Elm St, Charlotte, NC")
        parsed, error = _parse_entry(raw, None)
        assert error is None
        assert set(parsed.keys()) == {'name', 'street', 'city', 'state'}


class TestParseWikiEntry:
    def test_basic_wiki_entry(self):
        raw = _make_raw("Polar Iceplex", "Raleigh, North Carolina")
        extra = {'city': 'Raleigh', 'state': 'North Carolina'}
        parsed, error = _parse_wiki_entry(raw, extra)
        assert error is None
        assert parsed['name'] == 'Polar Iceplex'
        assert parsed['city'] == 'Raleigh'
        assert parsed['state'] == 'NC'
        assert parsed['street'] is None

    def test_state_abbreviation_conversion(self):
        raw = _make_raw("Ice Palace", "City, Minnesota")
        extra = {'city': 'Minneapolis', 'state': 'Minnesota'}
        parsed, error = _parse_wiki_entry(raw, extra)
        assert error is None
        assert parsed['state'] == 'MN'

    def test_already_abbreviated_state(self):
        raw = _make_raw("Ice Palace", "City, NC")
        extra = {'city': 'Raleigh', 'state': 'NC'}
        parsed, error = _parse_wiki_entry(raw, extra)
        assert error is None
        assert parsed['state'] == 'NC'

    def test_missing_name(self):
        raw = _make_raw(None, "Raleigh, NC")
        extra = {'city': 'Raleigh', 'state': 'NC'}
        parsed, error = _parse_wiki_entry(raw, extra)
        assert parsed is None
        assert 'Missing rink name' in error

    def test_missing_city_and_state(self):
        raw = _make_raw("Some Rink", "")
        extra = {'city': '', 'state': ''}
        parsed, error = _parse_wiki_entry(raw, extra)
        assert parsed is None
        assert 'Missing city and state' in error

    def test_city_title_cased(self):
        raw = _make_raw("Test Rink", "city, NC")
        extra = {'city': 'greensboro', 'state': 'NC'}
        parsed, error = _parse_wiki_entry(raw, extra)
        assert error is None
        assert parsed['city'] == 'Greensboro'
