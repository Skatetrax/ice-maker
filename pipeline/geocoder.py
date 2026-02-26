import time
import logging
import requests
from difflib import SequenceMatcher
from config import (
    NOMINATIM_URL, NOMINATIM_RATE_LIMIT, NOMINATIM_USER_AGENT,
    GEOCODE_CONFIDENCE_THRESHOLD
)

logger = logging.getLogger(__name__)

_last_request_time = 0.0


def _rate_limit():
    """Enforce Nominatim's 1 req/sec policy."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < NOMINATIM_RATE_LIMIT:
        time.sleep(NOMINATIM_RATE_LIMIT - elapsed)
    _last_request_time = time.time()


def geocode(street, city, state, country='US'):
    """Query Nominatim with structured address params.

    Returns a dict with lat, lon, display_name, postcode, confidence
    or None if the query failed or returned no results.
    """
    _rate_limit()

    params = {
        'street': street,
        'city': city,
        'state': state,
        'country': country,
        'format': 'json',
        'addressdetails': 1,
        'limit': 1,
    }

    headers = {'User-Agent': NOMINATIM_USER_AGENT}

    try:
        resp = requests.get(NOMINATIM_URL, params=params, headers=headers,
                            timeout=10)
        resp.raise_for_status()
        results = resp.json()
    except requests.RequestException as e:
        logger.warning("Nominatim request failed for '%s, %s, %s': %s",
                       street, city, state, e)
        return None

    if not results:
        logger.debug("No Nominatim results for '%s, %s, %s'",
                     street, city, state)
        return None

    hit = results[0]
    address_detail = hit.get('address', {})

    return {
        'lat': float(hit['lat']),
        'lon': float(hit['lon']),
        'display_name': hit.get('display_name', ''),
        'postcode': address_detail.get('postcode'),
        'address_detail': address_detail,
    }


def _score_address(candidate_street, candidate_city, candidate_state,
                   geo_address_detail):
    """Score how well the geocoded address matches our parsed address.
    Returns a 0-1 similarity score based on address components only.

    Rink names are intentionally excluded -- names like "The Factory"
    or "The Bog" will never match what Nominatim knows, and that's fine.
    The address is what matters for verification.
    """
    geo_road = geo_address_detail.get('road', '')
    geo_city = (geo_address_detail.get('city') or
                geo_address_detail.get('town') or
                geo_address_detail.get('village') or '')
    geo_state = geo_address_detail.get('state', '')

    scores = []

    if candidate_street and geo_road:
        scores.append(SequenceMatcher(
            None, candidate_street.lower(), geo_road.lower()
        ).ratio())

    if candidate_city and geo_city:
        scores.append(SequenceMatcher(
            None, candidate_city.lower(), geo_city.lower()
        ).ratio())

    if candidate_state:
        st = candidate_state.strip().upper()
        geo_iso = geo_address_detail.get('ISO3166-2-lvl4', '')
        if geo_iso:
            geo_abbrev = geo_iso.split('-')[-1].upper()
            scores.append(1.0 if st == geo_abbrev else 0.0)
        elif geo_state:
            geo_st = geo_state.strip().upper()
            scores.append(1.0 if st == geo_st or st.startswith(geo_st[:2]) else
                          SequenceMatcher(None, st, geo_st).ratio())

    if not scores:
        return 0.0

    return sum(scores) / len(scores)


def geocode_candidate(candidate):
    """Geocode a candidate row and update its fields in place.

    Confidence is based on address matching only, not rink name.
    Returns the verification status string.
    """
    result = geocode(
        street=candidate.street or '',
        city=candidate.city or '',
        state=candidate.state or '',
    )

    if result is None:
        candidate.verification_status = 'geocode_failed'
        return 'geocode_failed'

    candidate.geo_lat = result['lat']
    candidate.geo_lon = result['lon']
    candidate.geo_matched_name = result['display_name']

    if result['postcode']:
        candidate.zip = result['postcode']

    confidence = _score_address(
        candidate.street, candidate.city, candidate.state,
        result.get('address_detail', {})
    )
    candidate.geo_confidence = confidence

    if confidence >= GEOCODE_CONFIDENCE_THRESHOLD:
        candidate.verification_status = 'geocode_match'
        return 'geocode_match'
    else:
        candidate.verification_status = 'geocode_mismatch'
        return 'geocode_mismatch'
