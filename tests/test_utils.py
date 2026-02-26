"""Tests for utils/common.py -- pure functions, no DB or network."""

import pytest
from utils.common import country_us, reset_utf8


class TestLookupWords:
    def test_expands_street_abbreviations(self):
        assert country_us._lookup_words("100 E MAIN ST") == "100 E MAIN STREET"

    def test_expands_multiple_abbreviations(self):
        result = country_us._lookup_words("LANDMARK CENTER BLVD")
        assert result == "LANDMARK CENTER BOULEVARD"

    def test_expands_road(self):
        assert country_us._lookup_words("WYCKOFF RD") == "WYCKOFF ROAD"

    def test_expands_avenue(self):
        assert country_us._lookup_words("GREAT PLAINS AVE") == "GREAT PLAINS AVENUE"

    def test_expands_drive(self):
        assert country_us._lookup_words("OAK VALLEY DR") == "OAK VALLEY DRIVE"

    def test_unknown_words_pass_through(self):
        assert country_us._lookup_words("REILLY ROAD") == "REILLY ROAD"

    def test_empty_string(self):
        assert country_us._lookup_words("") == ""

    def test_none_input(self):
        assert country_us._lookup_words(None) is None


class TestRemovePunctuation:
    def test_strips_commas(self):
        assert country_us._remove_punctuation("Raleigh, NC") == "Raleigh NC"

    def test_strips_periods(self):
        assert country_us._remove_punctuation("St. Louis") == "St Louis"

    def test_strips_hyphens(self):
        assert country_us._remove_punctuation("Winston-Salem") == "WinstonSalem"

    def test_preserves_spaces(self):
        assert country_us._remove_punctuation("Ice House") == "Ice House"

    def test_empty_string(self):
        assert country_us._remove_punctuation("") == ""

    def test_none_input(self):
        assert country_us._remove_punctuation(None) is None


class TestExpandRecCtrs:
    def test_expands_rec_ctr(self):
        assert country_us._expand_rec_ctrs("Babson Rec Ctr") == "Babson Recreation Center"

    def test_preserves_unrelated_words(self):
        assert country_us._expand_rec_ctrs("Ice House") == "Ice House"

    def test_case_insensitive(self):
        assert country_us._expand_rec_ctrs("BABSON REC CTR") == "Babson Recreation Center"

    def test_empty_string(self):
        assert country_us._expand_rec_ctrs("") == ""

    def test_none_input(self):
        assert country_us._expand_rec_ctrs(None) is None


class TestResetUtf8:
    def test_already_utf8(self):
        assert reset_utf8("Polar Ice") == "Polar Ice"

    def test_none_input(self):
        assert reset_utf8(None) is None

    def test_empty_string(self):
        assert reset_utf8("") == ""

    def test_plain_ascii(self):
        assert reset_utf8("Hello World 123") == "Hello World 123"


class TestStateAbbreviations:
    def test_full_name_to_abbrev(self):
        assert country_us.us_state_to_abbrev["North Carolina"] == "NC"

    def test_abbrev_to_itself(self):
        assert country_us.us_state_to_abbrev["NC"] == "NC"

    def test_all_50_states_present(self):
        full_names = [k for k in country_us.us_state_to_abbrev
                      if len(k) > 2 and k not in (
                          "District of Columbia", "American Samoa", "Guam",
                          "Northern Mariana Islands", "Puerto Rico",
                          "United States Minor Outlying Islands",
                          "U.S. Virgin Islands")]
        assert len(full_names) == 50

    def test_dc_present(self):
        assert country_us.us_state_to_abbrev["District of Columbia"] == "DC"
        assert country_us.us_state_to_abbrev["DC"] == "DC"
