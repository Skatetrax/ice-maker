"""Tests for resilience: offline sources, malformed data, unexpected formats."""

import json
import pytest
import responses
from pathlib import Path
from unittest.mock import MagicMock

FIXTURES = Path(__file__).parent / 'fixtures'


class TestSourceOffline:
    @responses.activate
    def test_sk8stuff_connection_error(self):
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body=responses.ConnectionError("Connection refused"),
        )
        from parsers.sk8stuff import pull_sk8stuff
        with pytest.raises(Exception):
            pull_sk8stuff()

    @responses.activate
    def test_learntoskate_connection_error(self):
        responses.add(
            responses.POST,
            'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch',
            body=responses.ConnectionError("Connection refused"),
        )
        from parsers.learntoskate import pull_lts_data, _build_session
        session = _build_session()
        result = pull_lts_data(session, 1)
        assert result == []

    @responses.activate
    def test_fandom_wiki_connection_error(self):
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            body=responses.ConnectionError("Connection refused"),
        )
        from parsers.fandom_wiki import pull_fandom_wiki
        with pytest.raises(Exception):
            pull_fandom_wiki()


class TestSourceHTTPErrors:
    @responses.activate
    def test_sk8stuff_404(self):
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            status=404,
        )
        from parsers.sk8stuff import pull_sk8stuff
        with pytest.raises(Exception):
            pull_sk8stuff()

    @responses.activate
    def test_sk8stuff_500(self):
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            status=500,
        )
        from parsers.sk8stuff import pull_sk8stuff
        with pytest.raises(Exception):
            pull_sk8stuff()

    @responses.activate
    def test_learntoskate_500(self):
        responses.add(
            responses.POST,
            'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch',
            status=500,
        )
        from parsers.learntoskate import pull_lts_data, _build_session
        session = _build_session()
        result = pull_lts_data(session, 1)
        assert result == []


class TestEmptyHTML:
    @responses.activate
    def test_sk8stuff_empty_table(self):
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body='<html><body><table><tr><th>Name</th></tr></table></body></html>',
            status=200,
        )
        from parsers.sk8stuff import pull_sk8stuff
        rinks = pull_sk8stuff()
        assert rinks == []

    @responses.activate
    def test_sk8stuff_no_table(self):
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body='<html><body><p>No data</p></body></html>',
            status=200,
        )
        from parsers.sk8stuff import pull_sk8stuff
        rinks = pull_sk8stuff()
        assert rinks == []

    @responses.activate
    def test_fandom_wiki_empty_content(self):
        responses.add(
            responses.GET,
            'https://figure-skating.fandom.com/api.php',
            json={'parse': {'text': {'*': '<div class="mw-parser-output"></div>'}}},
            status=200,
        )
        from parsers.fandom_wiki import pull_fandom_wiki
        results = pull_fandom_wiki()
        assert results == []


class TestChangedStructure:
    @responses.activate
    def test_sk8stuff_different_column_count(self):
        html = '''<html><body><table>
        <tr><th>Name</th><th>City</th></tr>
        <tr><td>Rink</td><td>Raleigh</td></tr>
        </table></body></html>'''
        responses.add(
            responses.GET,
            'https://sk8stuff.com/utility/lister_rinks.php',
            body=html,
            status=200,
        )
        from parsers.sk8stuff import pull_sk8stuff
        rinks = pull_sk8stuff()
        # Rows with < 3 cells are skipped
        assert len(rinks) == 0

    @responses.activate
    def test_learntoskate_unexpected_json_shape(self):
        responses.add(
            responses.POST,
            'https://www.learntoskateusa.com/umbraco/surface/Map/GetPointsFromSearch',
            json={'unexpected_key': 'value'},
            status=200,
        )
        from parsers.learntoskate import pull_lts_data, _build_session
        session = _build_session()
        result = pull_lts_data(session, 1)
        assert result == []


class TestMalformedAddresses:
    def test_unicode_in_address(self):
        raw = MagicMock()
        raw.raw_name = "Café Rink"
        raw.raw_address = "100 Ñoño St, Charlotte, NC"
        from pipeline.runner import _parse_entry
        parsed, error = _parse_entry(raw, None)
        # Should not crash, may or may not parse successfully

    def test_html_entities_in_name(self):
        raw = MagicMock()
        raw.raw_name = "Rink &amp; Ice"
        raw.raw_address = "100 Main St, Raleigh, NC"
        from pipeline.runner import _parse_entry
        parsed, error = _parse_entry(raw, None)
        # Should not crash

    def test_newlines_in_address(self):
        raw = MagicMock()
        raw.raw_name = "Test Rink"
        raw.raw_address = "100 Main St\nRaleigh\nNC"
        from pipeline.runner import _parse_entry
        parsed, error = _parse_entry(raw, None)
        # Should not crash

    def test_extremely_long_string(self):
        raw = MagicMock()
        raw.raw_name = "A" * 5000
        raw.raw_address = "100 Main St, Raleigh, NC"
        from pipeline.runner import _parse_entry
        parsed, error = _parse_entry(raw, None)
        # Should not crash

    def test_none_address(self):
        raw = MagicMock()
        raw.raw_name = "Test"
        raw.raw_address = None
        from pipeline.runner import _parse_entry
        parsed, error = _parse_entry(raw, None)
        # Should return error, not crash
        assert parsed is None


class TestGeocoderUnexpectedJSON:
    @responses.activate
    def test_missing_keys_in_result(self):
        from unittest.mock import patch as _patch
        with _patch('pipeline.geocoder.time.sleep'):
            import pipeline.geocoder
            pipeline.geocoder._last_request_time = 0.0

            responses.add(
                responses.GET,
                'https://nominatim.openstreetmap.org/search',
                json=[{'lat': '35.0', 'lon': '-78.0'}],
                status=200,
            )
            from pipeline.geocoder import geocode
            result = geocode('Test St', 'Test City', 'NC')
            assert result is not None
            assert result['postcode'] is None


class TestFingerprintCollision:
    def test_different_content_different_hash(self):
        from pipeline.fingerprint import compute_fingerprint
        fp1 = compute_fingerprint(1, "Rink A", "100 Main St")
        fp2 = compute_fingerprint(1, "Rink B", "200 Oak Ave")
        assert fp1 != fp2

    def test_source_id_scoping(self):
        from pipeline.fingerprint import compute_fingerprint
        fp1 = compute_fingerprint(1, "Same Rink", "Same Address")
        fp2 = compute_fingerprint(2, "Same Rink", "Same Address")
        assert fp1 != fp2
