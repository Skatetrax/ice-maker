import re
import math
import logging
from difflib import SequenceMatcher
from sqlalchemy.orm import Session
from pipeline.staging import Candidates, LocationSources
from config import (
    FUZZY_NAME_THRESHOLD, FUZZY_NAME_THRESHOLD_NO_STREET,
    GEO_PROXIMITY_MILES,
)

logger = logging.getLogger(__name__)


def _normalize_for_dedup(text):
    """Lowercase, strip punctuation, collapse whitespace."""
    if not text:
        return ''
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9 ]', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def _haversine_miles(lat1, lon1, lat2, lon2):
    """Distance between two lat/lon points in miles."""
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def find_duplicate(session: Session, candidate):
    """Run the three-layer dedup check against existing candidates
    and return a match result.

    Returns:
        (match_candidate, match_layer) or (None, None) if no match.
        match_layer is one of: 'address_exact', 'fuzzy_name', 'geo_proximity'
    """
    norm_street = _normalize_for_dedup(candidate.street)
    norm_city = _normalize_for_dedup(candidate.city)
    norm_state = _normalize_for_dedup(candidate.state)
    norm_name = _normalize_for_dedup(candidate.name)

    # Layer 1: street + city + state exact match
    existing = (
        session.query(Candidates)
        .filter(
            Candidates.id != candidate.id,
            Candidates.verification_status.in_(
                ['geocode_match', 'human_approved', 'source_verified']
            ),
        )
        .all()
    )

    for other in existing:
        other_street = _normalize_for_dedup(other.street)
        other_city = _normalize_for_dedup(other.city)
        other_state = _normalize_for_dedup(other.state)

        if (norm_street and other_street and
                norm_street == other_street and
                norm_city == other_city and
                norm_state == other_state):
            logger.info("Layer 1 match: '%s' == '%s' at %s, %s",
                        candidate.name, other.name, other.city, other.state)
            return other, 'address_exact'

    # Layer 2: fuzzy name within same city + state
    #
    # When either side has no street address (e.g. wiki entries), use a
    # relaxed threshold and also consider unverified candidates so that
    # wiki-vs-wiki duplicates are caught.
    candidate_has_street = bool(norm_street)

    if candidate_has_street:
        layer2_pool = existing
    else:
        layer2_pool = (
            session.query(Candidates)
            .filter(
                Candidates.id != candidate.id,
                Candidates.verification_status.in_(
                    ['geocode_match', 'human_approved', 'source_verified',
                     'unverified']
                ),
            )
            .all()
        )

    for other in layer2_pool:
        other_city = _normalize_for_dedup(other.city)
        other_state = _normalize_for_dedup(other.state)

        if norm_city != other_city or norm_state != other_state:
            continue

        other_has_street = bool(_normalize_for_dedup(other.street))
        no_street = not candidate_has_street or not other_has_street
        threshold = FUZZY_NAME_THRESHOLD_NO_STREET if no_street else FUZZY_NAME_THRESHOLD

        other_name = _normalize_for_dedup(other.name)
        ratio = SequenceMatcher(None, norm_name, other_name).ratio()

        if ratio >= threshold:
            logger.info("Layer 2 match (%.2f, thr=%.2f): '%s' ~ '%s' in %s, %s",
                        ratio, threshold, candidate.name, other.name,
                        other.city, other.state)
            return other, 'fuzzy_name'

    # Layer 3: geographic proximity
    if candidate.geo_lat is not None and candidate.geo_lon is not None:
        for other in existing:
            if other.geo_lat is None or other.geo_lon is None:
                continue

            dist = _haversine_miles(
                candidate.geo_lat, candidate.geo_lon,
                other.geo_lat, other.geo_lon
            )

            if dist <= GEO_PROXIMITY_MILES:
                logger.info(
                    "Layer 3 match (%.2f mi): '%s' near '%s'",
                    dist, candidate.name, other.name
                )
                return other, 'geo_proximity'

    return None, None
