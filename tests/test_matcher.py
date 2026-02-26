"""Tests for pipeline/matcher.py -- all 3 dedup layers + normalization."""

import pytest
from pipeline.matcher import _normalize_for_dedup, _haversine_miles, find_duplicate


class TestNormalizeForDedup:
    def test_lowercases(self):
        assert _normalize_for_dedup("HELLO WORLD") == "hello world"

    def test_strips_punctuation(self):
        assert _normalize_for_dedup("Hello, World!") == "hello world"

    def test_collapses_whitespace(self):
        assert _normalize_for_dedup("ice   house") == "ice house"

    def test_strips_leading_trailing(self):
        assert _normalize_for_dedup("  test  ") == "test"

    def test_empty_string(self):
        assert _normalize_for_dedup("") == ""

    def test_none_returns_empty(self):
        assert _normalize_for_dedup(None) == ""

    def test_special_chars_removed(self):
        assert _normalize_for_dedup("St. Louis - MO") == "st louis mo"


class TestHaversineMiles:
    def test_same_point_is_zero(self):
        assert _haversine_miles(40.0, -74.0, 40.0, -74.0) == 0.0

    def test_nyc_to_newark(self):
        dist = _haversine_miles(40.7128, -74.0060, 40.7357, -74.1724)
        assert 8 < dist < 12

    def test_raleigh_to_greensboro(self):
        dist = _haversine_miles(35.7796, -78.6382, 36.0726, -79.7920)
        assert 60 < dist < 80


class TestFindDuplicateLayer1:
    """Layer 1: exact match on normalized street + city + state."""

    def test_exact_address_match(self, db_session, make_candidate):
        existing = make_candidate(
            name='Polar Ice Raleigh',
            street='100 MAIN STREET',
            city='Raleigh',
            state='NC',
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Polar Iceplex',
            street='100 MAIN STREET',
            city='Raleigh',
            state='NC',
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is not None
        assert match.id == existing.id
        assert layer == 'address_exact'

    def test_different_street_no_match(self, db_session, make_candidate):
        make_candidate(
            name='Greensboro Ice House',
            street='100 MAIN STREET',
            city='Raleigh',
            state='NC',
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Polar Iceplex',
            street='200 OAK AVENUE',
            city='Raleigh',
            state='NC',
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is None

    def test_empty_street_skips_layer1(self, db_session, make_candidate):
        make_candidate(
            name='Wiki Rink',
            street=None,
            city='Raleigh',
            state='NC',
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Wiki Rink',
            street=None,
            city='Raleigh',
            state='NC',
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        # Should match via layer 2 (fuzzy name), not layer 1
        if match:
            assert layer != 'address_exact'

    def test_source_verified_included_in_pool(self, db_session, make_candidate):
        existing = make_candidate(
            name='LTS Rink',
            street='100 MAIN STREET',
            city='Raleigh',
            state='NC',
            verification_status='source_verified',
        )
        new = make_candidate(
            name='Other Rink',
            street='100 MAIN STREET',
            city='Raleigh',
            state='NC',
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is not None
        assert match.id == existing.id


class TestFindDuplicateLayer2:
    """Layer 2: fuzzy name within same city + state."""

    def test_similar_names_match(self, db_session, make_candidate):
        existing = make_candidate(
            name='Greensboro Ice House',
            street='100 OAK STREET',
            city='Greensboro',
            state='NC',
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Greensboro Icehouse',
            street='200 ELM AVENUE',
            city='Greensboro',
            state='NC',
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is not None
        assert layer == 'fuzzy_name'

    def test_different_names_no_match(self, db_session, make_candidate):
        make_candidate(
            name='Ice Palace',
            street='100 OAK STREET',
            city='Raleigh',
            state='NC',
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Polar Iceplex',
            street='200 ELM AVENUE',
            city='Raleigh',
            state='NC',
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is None or layer != 'fuzzy_name'

    def test_same_name_different_state_no_match(self, db_session, make_candidate):
        make_candidate(
            name='Ice House',
            street='100 OAK STREET',
            city='Raleigh',
            state='NC',
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Ice House',
            street='200 ELM AVENUE',
            city='Raleigh',
            state='VA',
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is None

    def test_no_street_uses_relaxed_threshold(self, db_session, make_candidate):
        """Wiki entries (no street) use FUZZY_NAME_THRESHOLD_NO_STREET (0.6)."""
        existing = make_candidate(
            name='Polar Ice Raleigh',
            street=None,
            city='Raleigh',
            state='NC',
            verification_status='unverified',
        )
        new = make_candidate(
            name='Polar Iceplex Raleigh',
            street=None,
            city='Raleigh',
            state='NC',
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        # With relaxed threshold (0.6), these should match
        assert match is not None
        assert layer == 'fuzzy_name'


class TestFindDuplicateLayer3:
    """Layer 3: geographic proximity."""

    def test_nearby_coordinates_match(self, db_session, make_candidate):
        existing = make_candidate(
            name='Rink A',
            street='100 MAIN STREET',
            city='Raleigh',
            state='NC',
            geo_lat=35.7796,
            geo_lon=-78.6382,
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Totally Different Name',
            street='200 OAK AVENUE',
            city='Raleigh',
            state='NC',
            geo_lat=35.7798,
            geo_lon=-78.6380,
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is not None
        assert layer == 'geo_proximity'

    def test_far_apart_no_geo_match(self, db_session, make_candidate):
        make_candidate(
            name='Rink A',
            street='100 MAIN STREET',
            city='Raleigh',
            state='NC',
            geo_lat=35.7796,
            geo_lon=-78.6382,
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Rink B',
            street='200 OAK AVENUE',
            city='Charlotte',
            state='NC',
            geo_lat=35.2271,
            geo_lon=-80.8431,
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is None or layer != 'geo_proximity'

    def test_missing_coords_skips_layer3(self, db_session, make_candidate):
        make_candidate(
            name='Rink A',
            street='100 MAIN STREET',
            city='Charlotte',
            state='NC',
            geo_lat=None,
            geo_lon=None,
            verification_status='geocode_match',
        )
        new = make_candidate(
            name='Different Rink',
            street='200 OAK AVENUE',
            city='Durham',
            state='NC',
            geo_lat=35.9940,
            geo_lon=-78.8986,
            verification_status='unverified',
        )
        match, layer = find_duplicate(db_session, new)
        assert match is None or layer != 'geo_proximity'
