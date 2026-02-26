"""Tests for pipeline/geocoder.py with mocked Nominatim responses."""

import pytest
import responses
from unittest.mock import patch, MagicMock
from pipeline.geocoder import geocode, _score_address, geocode_candidate
from config import NOMINATIM_URL, GEOCODE_CONFIDENCE_THRESHOLD


@pytest.fixture(autouse=True)
def reset_rate_limit():
    """Reset the module-level rate limiter between tests."""
    import pipeline.geocoder
    pipeline.geocoder._last_request_time = 0.0


@pytest.fixture(autouse=True)
def skip_rate_limit():
    """Skip the actual sleep to keep tests fast."""
    with patch('pipeline.geocoder.time.sleep'):
        yield


class TestGeocode:
    @responses.activate
    def test_successful_response(self):
        responses.add(
            responses.GET, NOMINATIM_URL,
            json=[{
                'lat': '35.7796',
                'lon': '-78.6382',
                'display_name': '100 Main St, Raleigh, NC 27601, USA',
                'address': {
                    'road': 'Main Street',
                    'city': 'Raleigh',
                    'state': 'North Carolina',
                    'postcode': '27601',
                    'ISO3166-2-lvl4': 'US-NC',
                },
            }],
            status=200,
        )
        result = geocode('100 Main St', 'Raleigh', 'NC')
        assert result is not None
        assert result['lat'] == 35.7796
        assert result['lon'] == -78.6382
        assert result['postcode'] == '27601'

    @responses.activate
    def test_empty_response_returns_none(self):
        responses.add(responses.GET, NOMINATIM_URL, json=[], status=200)
        result = geocode('999 Nowhere St', 'FakeCity', 'XX')
        assert result is None

    @responses.activate
    def test_http_error_returns_none(self):
        responses.add(responses.GET, NOMINATIM_URL, status=500)
        result = geocode('100 Main St', 'Raleigh', 'NC')
        assert result is None

    @responses.activate
    def test_timeout_returns_none(self):
        responses.add(
            responses.GET, NOMINATIM_URL,
            body=responses.ConnectionError("timeout"),
        )
        result = geocode('100 Main St', 'Raleigh', 'NC')
        assert result is None


class TestScoreAddress:
    def test_perfect_match(self):
        detail = {
            'road': '100 Main Street',
            'city': 'Raleigh',
            'state': 'North Carolina',
            'ISO3166-2-lvl4': 'US-NC',
        }
        score = _score_address('100 MAIN STREET', 'Raleigh', 'NC', detail)
        assert score > 0.8

    def test_wrong_street_lowers_score(self):
        detail = {
            'road': 'Oak Avenue',
            'city': 'Raleigh',
            'state': 'North Carolina',
            'ISO3166-2-lvl4': 'US-NC',
        }
        score_wrong = _score_address('100 MAIN STREET', 'Raleigh', 'NC', detail)
        score_right = _score_address('Oak Avenue', 'Raleigh', 'NC', detail)
        assert score_wrong < score_right

    def test_iso_state_code_matching(self):
        detail = {
            'road': 'Main Street',
            'city': 'Des Moines',
            'state': 'Iowa',
            'ISO3166-2-lvl4': 'US-IA',
        }
        score = _score_address('Main Street', 'Des Moines', 'IA', detail)
        assert score >= 0.9

    def test_no_iso_code_falls_back(self):
        detail = {
            'road': 'Main Street',
            'city': 'Raleigh',
            'state': 'NC',
        }
        score = _score_address('Main Street', 'Raleigh', 'NC', detail)
        assert score >= 0.9

    def test_empty_detail_returns_zero(self):
        assert _score_address('Main St', 'Raleigh', 'NC', {}) == 0.0

    def test_missing_components_handled(self):
        detail = {'road': 'Main Street'}
        score = _score_address('Main Street', '', '', detail)
        assert score > 0.0


class TestGeocodeCandidate:
    @responses.activate
    def test_updates_candidate_on_match(self):
        responses.add(
            responses.GET, NOMINATIM_URL,
            json=[{
                'lat': '35.7796',
                'lon': '-78.6382',
                'display_name': 'Raleigh, NC',
                'address': {
                    'road': '100 Main Street',
                    'city': 'Raleigh',
                    'state': 'North Carolina',
                    'postcode': '27601',
                    'ISO3166-2-lvl4': 'US-NC',
                },
            }],
            status=200,
        )
        cand = MagicMock()
        cand.street = '100 MAIN STREET'
        cand.city = 'Raleigh'
        cand.state = 'NC'
        cand.zip = None

        status = geocode_candidate(cand)
        assert status == 'geocode_match'
        assert cand.geo_lat == 35.7796
        assert cand.geo_lon == -78.6382
        assert cand.zip == '27601'

    @responses.activate
    def test_sets_geocode_failed_on_no_result(self):
        responses.add(responses.GET, NOMINATIM_URL, json=[], status=200)
        cand = MagicMock()
        cand.street = 'Bad Address'
        cand.city = 'Nowhere'
        cand.state = 'XX'

        status = geocode_candidate(cand)
        assert status == 'geocode_failed'
        assert cand.verification_status == 'geocode_failed'

    @responses.activate
    def test_sets_geocode_mismatch_on_low_confidence(self):
        responses.add(
            responses.GET, NOMINATIM_URL,
            json=[{
                'lat': '35.0',
                'lon': '-78.0',
                'display_name': 'Wrong Place',
                'address': {
                    'road': 'Completely Different Road',
                    'city': 'OtherTown',
                    'state': 'Virginia',
                    'postcode': '99999',
                    'ISO3166-2-lvl4': 'US-VA',
                },
            }],
            status=200,
        )
        cand = MagicMock()
        cand.street = '100 MAIN STREET'
        cand.city = 'Raleigh'
        cand.state = 'NC'
        cand.zip = None

        status = geocode_candidate(cand)
        assert status == 'geocode_mismatch'

    def test_rate_limiting(self):
        """Two rapid calls should trigger sleep."""
        import pipeline.geocoder
        with patch('pipeline.geocoder.time') as mock_time:
            mock_time.time.side_effect = [0.0, 0.0, 0.5, 0.5]
            pipeline.geocoder._last_request_time = 0.0
            mock_time.sleep = MagicMock()
            pipeline.geocoder._rate_limit()
            pipeline.geocoder._rate_limit()
            assert mock_time.sleep.called
