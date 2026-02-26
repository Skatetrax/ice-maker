import re
import logging
from datetime import datetime, timezone
from difflib import SequenceMatcher
from types import SimpleNamespace

import requests
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import Session, declarative_base

from pipeline.staging import (
    Sources, RawEntries, Candidates, RejectedEntries,
    LocationSources, Locations, build_engine, init_db,
)
from config import (
    FUZZY_NAME_THRESHOLD, FUZZY_NAME_THRESHOLD_NO_STREET,
)

_SkatetraxBase = declarative_base()


class _SkatetraxLocation(_SkatetraxBase):
    """Read-only mirror of the Skatetrax locations table.

    Used as a fallback when the public API is unavailable and
    SKATETRAX_DB_URL is configured.
    """

    __tablename__ = 'locations'

    rink_id = Column(String, primary_key=True)
    rink_name = Column(String)
    rink_address = Column(String)
    rink_city = Column(String)
    rink_state = Column(String)

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


def _fetch_skatetrax_api():
    """Fetch rinks from the Skatetrax public API.

    Returns a list of SimpleNamespace objects with the same attributes
    as _SkatetraxLocation (rink_id, rink_name, rink_address, rink_city,
    rink_state), or an empty list on failure.
    """
    from config import SKATETRAX_API_URL

    if not SKATETRAX_API_URL:
        return []

    try:
        resp = requests.get(SKATETRAX_API_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Skatetrax public API request failed: %s", e)
        return []

    locations = []
    for r in data:
        if r.get('rink_city', '-') == '-':
            continue
        locations.append(SimpleNamespace(
            rink_id=r['rink_id'],
            rink_name=r.get('rink_name', ''),
            rink_address=r.get('rink_address', ''),
            rink_city=r.get('rink_city', ''),
            rink_state=r.get('rink_state', ''),
        ))

    return locations


def _fetch_skatetrax_db():
    """Fallback: load rinks directly from the Skatetrax database.

    Only used when SKATETRAX_DB_URL is set and the public API is
    unavailable.  Returns an empty list otherwise.
    """
    from config import SKATETRAX_DB_URL

    if not SKATETRAX_DB_URL:
        return []

    try:
        engine = create_engine(SKATETRAX_DB_URL)
        with Session(engine) as session:
            locations = session.query(_SkatetraxLocation).all()
            session.expunge_all()
        return locations
    except Exception as e:
        logger.warning(
            "Skatetrax DB fallback failed: %s", e
        )
        return []


def _load_skatetrax_locations():
    """Load existing Skatetrax rinks for UUID alignment.

    Tries the public API first (no credentials needed, works from
    any environment).  Falls back to a direct DB query when
    SKATETRAX_DB_URL is set.  Returns an empty list if neither
    source is available -- promotion still works, it just mints
    fresh UUIDs.
    """
    locations = _fetch_skatetrax_api()
    if not locations:
        locations = _fetch_skatetrax_db()

    if locations:
        logger.info(
            "Loaded %d Skatetrax rinks for UUID alignment", len(locations),
        )
    else:
        logger.info(
            "No Skatetrax rinks available for UUID alignment -- "
            "new locations will receive fresh UUIDs"
        )

    return locations


def _find_skatetrax_match(skatetrax_locations, name, street, city, state):
    """Check Skatetrax for a rink matching this candidate.

    Uses the same two-layer matching as _find_matching_location:
    Layer 1 -- exact normalized street + city + state
    Layer 2 -- fuzzy name within the same city + state

    Returns the matching _SkatetraxLocation or None.
    """
    if not skatetrax_locations:
        return None

    norm_street = _normalize(street)
    norm_city = _normalize(city)
    norm_state = _normalize(state)
    norm_name = _normalize(name)

    for loc in skatetrax_locations:
        loc_street = _normalize(loc.rink_address)
        loc_city = _normalize(loc.rink_city)
        loc_state = _normalize(loc.rink_state)

        if (norm_street and loc_street and
                norm_street == loc_street and
                loc_city == norm_city and
                loc_state == norm_state):
            return loc

    for loc in skatetrax_locations:
        loc_city = _normalize(loc.rink_city)
        loc_state = _normalize(loc.rink_state)

        if norm_city != loc_city or norm_state != loc_state:
            continue

        loc_has_street = bool(_normalize(loc.rink_address))
        no_street = not norm_street or not loc_has_street
        threshold = (FUZZY_NAME_THRESHOLD_NO_STREET if no_street
                     else FUZZY_NAME_THRESHOLD)

        loc_name = _normalize(loc.rink_name)
        ratio = SequenceMatcher(None, norm_name, loc_name).ratio()

        if ratio >= threshold:
            return loc

    return None


def _normalize(text):
    """Lowercase, strip punctuation, collapse whitespace."""
    if not text:
        return ''
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9 ]', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def _find_matching_location(session, name, street, city, state):
    """Check the locations table for an existing match.

    Layer 1: exact normalized street + city + state
    Layer 2: fuzzy name in same city + state (relaxed threshold when
             either side has no street)

    Returns the matching Locations row or None.
    """
    norm_street = _normalize(street)
    norm_city = _normalize(city)
    norm_state = _normalize(state)
    norm_name = _normalize(name)

    locations = (
        session.query(Locations)
        .filter(Locations.rink_status.notin_(['merged', 'disabled']))
        .all()
    )

    for loc in locations:
        loc_street = _normalize(loc.rink_address)
        loc_city = _normalize(loc.rink_city)
        loc_state = _normalize(loc.rink_state)

        if (norm_street and loc_street and
                norm_street == loc_street and
                norm_city == loc_city and
                norm_state == loc_state):
            return loc

    for loc in locations:
        loc_city = _normalize(loc.rink_city)
        loc_state = _normalize(loc.rink_state)

        if norm_city != loc_city or norm_state != loc_state:
            continue

        loc_has_street = bool(_normalize(loc.rink_address))
        no_street = not norm_street or not loc_has_street
        threshold = (FUZZY_NAME_THRESHOLD_NO_STREET if no_street
                     else FUZZY_NAME_THRESHOLD)

        loc_name = _normalize(loc.rink_name)
        ratio = SequenceMatcher(None, norm_name, loc_name).ratio()

        if ratio >= threshold:
            return loc

    return None


def _source_name_for(session, candidate):
    """Resolve the human-readable source name for a candidate."""
    raw = session.get(RawEntries, candidate.raw_entry_id)
    if raw:
        src = session.get(Sources, raw.source_id)
        if src:
            return src.name
    return 'unknown'


def _add_location_source(session, location_id, candidate):
    """Record a source corroboration, avoiding duplicates."""
    raw = session.get(RawEntries, candidate.raw_entry_id)
    if not raw:
        return

    existing = (
        session.query(LocationSources)
        .filter_by(location_id=location_id, source_id=raw.source_id)
        .first()
    )

    if existing:
        existing.last_seen_at = datetime.now(timezone.utc)
        existing.is_present = True
    else:
        session.add(LocationSources(
            location_id=location_id,
            source_id=raw.source_id,
            candidate_id=candidate.id,
        ))


def promote_verified(session, skatetrax_locations=None):
    """Phase 1: Move geocode_match and source_verified candidates
    into the locations table.

    When *skatetrax_locations* is provided (a list of _SkatetraxLocation
    rows), the promoter checks Skatetrax for an existing rink before
    minting a new UUID.  This keeps ice-maker and Skatetrax aligned:
    rinks Skatetrax already knows about keep their original UUID, and
    only genuinely new rinks get a fresh one.

    Returns a stats dict.
    """
    if skatetrax_locations is None:
        skatetrax_locations = []

    stats = {
        'promoted_new': 0,
        'promoted_existing': 0,
        'skipped_no_zip': 0,
        'adopted_skatetrax_uuid': 0,
    }

    candidates = (
        session.query(Candidates)
        .filter(
            Candidates.verification_status.in_(
                ['geocode_match', 'source_verified']
            ),
            Candidates.location_id.is_(None),
        )
        .all()
    )

    logger.info("Phase 1: %d verified candidates to promote", len(candidates))

    for i, cand in enumerate(candidates, 1):
        if not cand.zip:
            stats['skipped_no_zip'] += 1
            continue

        match = _find_matching_location(
            session, cand.name, cand.street, cand.city, cand.state
        )

        if match:
            cand.location_id = match.rink_id
            _add_location_source(session, match.rink_id, cand)
            stats['promoted_existing'] += 1
        else:
            loc_kwargs = dict(
                rink_name=cand.name,
                rink_address=cand.street or '',
                rink_city=cand.city,
                rink_state=cand.state,
                rink_country=cand.country or 'US',
                rink_zip=cand.zip,
                data_source=_source_name_for(session, cand),
            )

            st_match = _find_skatetrax_match(
                skatetrax_locations, cand.name, cand.street,
                cand.city, cand.state,
            )

            if st_match:
                adopted_id = str(st_match.rink_id)
                existing = session.get(Locations, adopted_id)
                if existing:
                    cand.location_id = existing.rink_id
                    _add_location_source(session, existing.rink_id, cand)
                    stats['promoted_existing'] += 1
                    continue

                loc_kwargs['rink_id'] = adopted_id
                logger.info(
                    "Adopting Skatetrax UUID %s for '%s' in %s, %s",
                    adopted_id, cand.name, cand.city, cand.state,
                )
                stats['adopted_skatetrax_uuid'] += 1

            loc = Locations(**loc_kwargs)
            session.add(loc)
            session.flush()

            cand.location_id = loc.rink_id
            _add_location_source(session, loc.rink_id, cand)
            stats['promoted_new'] += 1

        if i % BATCH_SIZE == 0:
            session.commit()
            logger.info("Phase 1 progress: %d/%d", i, len(candidates))

    session.commit()
    logger.info(
        "Phase 1 done: %d new locations (%d adopted Skatetrax UUIDs), "
        "%d linked to existing, %d skipped (no zip)",
        stats['promoted_new'], stats['adopted_skatetrax_uuid'],
        stats['promoted_existing'], stats['skipped_no_zip'],
    )
    return stats


def link_duplicates(session):
    """Phase 2: Link duplicate candidates to the location their
    primary candidate was promoted to.

    Returns a stats dict.
    """
    stats = {'linked': 0, 'primary_not_promoted': 0, 'parse_failed': 0}

    duplicates = (
        session.query(Candidates)
        .filter(
            Candidates.verification_status == 'duplicate',
            Candidates.location_id.is_(None),
        )
        .all()
    )

    logger.info("Phase 2: %d duplicate candidates to link", len(duplicates))

    for i, dup in enumerate(duplicates, 1):
        rejection = (
            session.query(RejectedEntries)
            .filter_by(raw_entry_id=dup.raw_entry_id)
            .filter(RejectedEntries.rejection_reason.in_(
                ['duplicate_address_exact', 'suspected_duplicate']
            ))
            .first()
        )

        if not rejection or not rejection.raw_parse_error:
            stats['parse_failed'] += 1
            continue

        m = re.search(r'Matches candidate (\d+):', rejection.raw_parse_error)
        if not m:
            stats['parse_failed'] += 1
            continue

        primary_id = int(m.group(1))
        primary = session.get(Candidates, primary_id)

        if not primary or not primary.location_id:
            stats['primary_not_promoted'] += 1
            continue

        dup.location_id = primary.location_id
        _add_location_source(session, primary.location_id, dup)
        stats['linked'] += 1

        if i % BATCH_SIZE == 0:
            session.commit()
            logger.info("Phase 2 progress: %d/%d", i, len(duplicates))

    session.commit()
    logger.info(
        "Phase 2 done: %d linked, %d primary not yet promoted, "
        "%d couldn't parse match",
        stats['linked'], stats['primary_not_promoted'],
        stats['parse_failed'],
    )
    return stats


def link_wiki_entries(session):
    """Phase 3: Link unverified wiki entries (no street address) to
    existing promoted locations via fuzzy name + city + state.

    No new locations are created -- wiki data alone isn't enough.

    Returns a stats dict.
    """
    stats = {'linked': 0, 'no_match': 0}

    wiki_candidates = (
        session.query(Candidates)
        .filter(
            Candidates.verification_status == 'unverified',
            Candidates.location_id.is_(None),
        )
        .all()
    )

    streetless = [c for c in wiki_candidates if not (c.street or '').strip()]
    logger.info("Phase 3: %d unverified streetless candidates to link",
                len(streetless))

    for i, cand in enumerate(streetless, 1):
        match = _find_matching_location(
            session, cand.name, cand.street, cand.city, cand.state
        )

        if match:
            cand.location_id = match.rink_id
            _add_location_source(session, match.rink_id, cand)
            stats['linked'] += 1
        else:
            stats['no_match'] += 1

        if i % BATCH_SIZE == 0:
            session.commit()
            logger.info("Phase 3 progress: %d/%d", i, len(streetless))

    session.commit()
    logger.info(
        "Phase 3 done: %d wiki entries linked, %d unmatched",
        stats['linked'], stats['no_match'],
    )
    return stats


def run_promotion():
    """Execute all three promotion phases and return combined stats."""
    engine = build_engine()
    init_db(engine)

    skatetrax_locations = _load_skatetrax_locations()

    with Session(engine) as session:
        s1 = promote_verified(session, skatetrax_locations=skatetrax_locations)
        s2 = link_duplicates(session)
        s3 = link_wiki_entries(session)

    total_locations = 0
    with Session(engine) as session:
        total_locations = session.query(Locations).count()

    stats = {
        'phase1_new_locations': s1['promoted_new'],
        'phase1_adopted_skatetrax_uuid': s1['adopted_skatetrax_uuid'],
        'phase1_linked_existing': s1['promoted_existing'],
        'phase1_skipped_no_zip': s1['skipped_no_zip'],
        'phase2_duplicates_linked': s2['linked'],
        'phase2_primary_not_promoted': s2['primary_not_promoted'],
        'phase2_parse_failed': s2['parse_failed'],
        'phase3_wiki_linked': s3['linked'],
        'phase3_wiki_no_match': s3['no_match'],
        'total_locations': total_locations,
    }

    logger.info("Promotion complete: %s", stats)
    return stats
