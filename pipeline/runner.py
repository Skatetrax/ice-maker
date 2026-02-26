import importlib
import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from pipeline.staging import (
    Sources, RawEntries, Candidates, RejectedEntries,
    LocationSources, build_engine, init_db
)
from pipeline.fingerprint import check_and_insert_raw
from pipeline.geocoder import geocode_candidate
from pipeline.matcher import find_duplicate

logger = logging.getLogger(__name__)


def _load_parser(module_path):
    """Dynamically import a parser module."""
    return importlib.import_module(module_path)


def _parse_entry(raw_entry, formatter_module):
    """Run the formatter's address parsing on a raw entry.

    The existing formatters work on CSVs via pandas. Here we call the
    underlying normalization functions directly on individual entries
    to integrate with the pipeline without rewriting them.
    """
    from utils.common import reset_utf8, country_us

    name = reset_utf8(raw_entry.raw_name)
    name = country_us._expand_rec_ctrs(name)

    address = raw_entry.raw_address
    street, city, state = None, None, None

    try:
        import usaddress
        tagged = usaddress.tag(address)
        parts = tagged[0]

        addr_number = parts.get('AddressNumber', '')
        pre_dir = parts.get('StreetNamePreDirectional', '')
        street_name = parts.get('StreetName', '')
        street_type = parts.get('StreetNamePostType', '')
        post_dir = parts.get('StreetNamePostDirectional', '')
        occupancy = parts.get('OccupancyIdentifier', '')

        street_parts = [addr_number, pre_dir, street_name,
                        street_type, post_dir, occupancy]
        assembled = ' '.join(p for p in street_parts if p).strip()

        if assembled:
            street = country_us._remove_punctuation(assembled)
            street = country_us._lookup_words(street)

        city = parts.get('PlaceName', '')
        if city:
            city = country_us._remove_punctuation(city)

        state = parts.get('StateName', '')
        if state:
            state = country_us.us_state_to_abbrev.get(state, state)

    except Exception as e:
        return None, str(e)

    if not name or not street:
        return None, f"Missing required fields: name={name!r}, street={street!r}"

    return {
        'name': name.strip().title() if name else name,
        'street': street.upper() if street else street,
        'city': city.strip().title() if city else city,
        'state': state.strip().upper() if state else state,
    }, None


def _parse_wiki_entry(raw_entry, extra):
    """Parse a fandom_wiki entry using pre-extracted city/state.

    The wiki source has no street addresses, so usaddress parsing is
    skipped.  The city and state come from the wiki table structure
    and full state names are converted to 2-letter abbreviations.
    """
    from utils.common import reset_utf8, country_us

    name = reset_utf8(raw_entry.raw_name)
    if not name:
        return None, "Missing rink name"

    city = (extra.get('city') or '').strip()
    state_full = (extra.get('state') or '').strip()
    state = country_us.us_state_to_abbrev.get(state_full, state_full)

    if not city and not state:
        return None, f"Missing city and state for {name!r}"

    return {
        'name': name.strip().title(),
        'street': None,
        'city': city.title(),
        'state': state.upper(),
    }, None


def repair_geocode_failed():
    """Re-parse geocode_failed candidates with the fixed parser.

    Candidates whose street address was incorrectly parsed (missing
    house numbers) are re-parsed from their raw entries.  Successfully
    re-parsed candidates are reset to 'unverified' so --geocode-pending
    can pick them up.

    Returns a stats dict.
    """
    engine = build_engine()
    init_db(engine)

    stats = {'total': 0, 'repaired': 0, 'still_failed': 0, 'unchanged': 0}

    with Session(engine) as session:
        failed = (
            session.query(Candidates)
            .filter(Candidates.verification_status == 'geocode_failed')
            .all()
        )
        stats['total'] = len(failed)
        logger.info("Found %d geocode_failed candidates to repair", len(failed))

        for cand in failed:
            raw = session.get(RawEntries, cand.raw_entry_id)
            if not raw:
                stats['still_failed'] += 1
                continue

            parsed, error = _parse_entry(raw, None)
            if parsed is None:
                stats['still_failed'] += 1
                continue

            new_street = parsed['street']
            if new_street and new_street != cand.street:
                cand.street = new_street
                cand.city = parsed['city']
                cand.state = parsed['state']
                cand.verification_status = 'unverified'
                cand.geo_lat = None
                cand.geo_lon = None
                cand.geo_confidence = None
                cand.geo_matched_name = None
                cand.zip = None
                stats['repaired'] += 1
            else:
                stats['unchanged'] += 1

        session.commit()

    logger.info(
        "Repair complete: %d repaired (now unverified), %d unchanged, "
        "%d still failed",
        stats['repaired'], stats['unchanged'], stats['still_failed'],
    )
    return stats


def run_source(source_name, scrape_only=False, geocode=True, limit=None):
    """Execute the full pipeline for a single source.

    Args:
        source_name: Name of the source (must exist in sources table).
        scrape_only: If True, only scrape and fingerprint. Don't parse/geocode.
        geocode: If True, run geocoding on unverified candidates.
        limit: Max number of new entries to process (for testing).

    Returns:
        dict with counts of what happened.
    """
    engine = build_engine()
    init_db(engine)

    stats = {
        'scraped': 0, 'new': 0, 'skipped': 0,
        'parsed': 0, 'parse_failed': 0,
        'geocoded': 0, 'geocode_match': 0, 'geocode_mismatch': 0,
        'geocode_failed': 0, 'source_verified': 0,
        'dedup_exact': 0, 'dedup_fuzzy': 0, 'dedup_geo': 0,
    }

    with Session(engine) as session:
        source = session.query(Sources).filter_by(name=source_name).first()
        if not source:
            logger.error("Source '%s' not found in sources table", source_name)
            return stats
        if not source.enabled:
            logger.warning("Source '%s' is disabled, skipping", source_name)
            return stats

        # Step 1: Scrape
        logger.info("Loading parser: %s", source.parser_module)
        parser = _load_parser(source.parser_module)

        if source_name == 'sk8stuff':
            raw_data = parser.pull_sk8stuff()
            raw_data = [
                {'name': r['name'],
                 'address': f"{r['street']}, {r['city']}, {r['state']}"}
                for r in raw_data
            ]
        elif source_name == 'arena_guide':
            raw_data = parser.pull_arena_guide_content()
            raw_data = [
                {'name': r['name'], 'address': r['address']}
                for r in raw_data
            ]
        elif source_name == 'learntoskate':
            lts_data = parser.aggr_lts()
            raw_data = [
                {'name': r['name'],
                 'address': f"{r['street']}, {r['city']}, {r['state']}",
                 '_extra': {
                     'zip': r.get('zip', ''),
                     'lat': r.get('lat'),
                     'lng': r.get('lng'),
                 }}
                for r in lts_data
            ]
        elif source_name == 'fandom_wiki':
            wiki_data = parser.pull_fandom_wiki()
            raw_data = [
                {'name': r['name'],
                 'address': f"{r['city']}, {r['state']}",
                 '_extra': {
                     'city': r['city'],
                     'state': r['state'],
                     'county': r.get('county', ''),
                     'club': r.get('club', ''),
                     'notes': r.get('notes', ''),
                     'website': r.get('website'),
                     'is_defunct': r.get('is_defunct', False),
                 }}
                for r in wiki_data
            ]
        else:
            logger.error("No scrape handler for source '%s'", source_name)
            return stats

        stats['scraped'] = len(raw_data)
        logger.info("Scraped %d entries from %s", len(raw_data), source_name)

        # Step 2-3: Fingerprint and insert raw
        new_entries = []
        extra_by_entry_id = {}
        for row in raw_data:
            entry, is_new = check_and_insert_raw(
                session, source.id, row['name'], row['address']
            )
            if is_new:
                new_entries.append(entry)
                if '_extra' in row:
                    extra_by_entry_id[entry.id] = row['_extra']
                stats['new'] += 1
            else:
                stats['skipped'] += 1

            if limit and stats['new'] >= limit:
                break

        session.commit()
        logger.info("New: %d, Skipped (unchanged): %d",
                    stats['new'], stats['skipped'])

        if scrape_only:
            _update_source_meta(session, source, stats)
            session.commit()
            return stats

        # Step 4: Parse new entries
        for raw_entry in new_entries:
            extra = extra_by_entry_id.get(raw_entry.id, {})

            if source_name == 'fandom_wiki':
                parsed, error = _parse_wiki_entry(raw_entry, extra)
            else:
                parsed, error = _parse_entry(raw_entry, source.formatter_module)

            if parsed is None:
                raw_entry.parse_status = 'failed'
                session.add(RejectedEntries(
                    raw_entry_id=raw_entry.id,
                    rejection_reason='parse_failure',
                    raw_parse_error=error,
                ))
                stats['parse_failed'] += 1
                continue

            raw_entry.parse_status = 'parsed'
            candidate = Candidates(
                raw_entry_id=raw_entry.id,
                name=parsed['name'],
                street=parsed.get('street'),
                city=parsed['city'],
                state=parsed['state'],
            )

            if extra.get('zip'):
                candidate.zip = extra['zip']
            if extra.get('lat') is not None and extra.get('lng') is not None:
                candidate.geo_lat = extra['lat']
                candidate.geo_lon = extra['lng']

            session.add(candidate)
            session.flush()
            stats['parsed'] += 1

            # Step 5: Dedup check
            match, layer = find_duplicate(session, candidate)
            if match:
                reason = 'suspected_duplicate'
                if layer == 'address_exact':
                    stats['dedup_exact'] += 1
                    reason = 'duplicate_address_exact'
                elif layer == 'fuzzy_name':
                    stats['dedup_fuzzy'] += 1
                elif layer == 'geo_proximity':
                    stats['dedup_geo'] += 1

                session.add(RejectedEntries(
                    raw_entry_id=raw_entry.id,
                    rejection_reason=reason,
                    raw_parse_error=f"Matches candidate {match.id}: {match.name}",
                ))
                candidate.verification_status = 'duplicate'
                continue

            # Step 6: Geocode (or mark source_verified if source provided coords)
            has_source_coords = (
                candidate.geo_lat is not None and
                candidate.geo_lon is not None and
                candidate.zip
            )

            if has_source_coords:
                candidate.verification_status = 'source_verified'
                stats['source_verified'] += 1
                logger.debug("Source-verified (coords+zip from API): %s",
                             candidate.name)
            elif geocode:
                status = geocode_candidate(candidate)
                stats['geocoded'] += 1

                if status == 'geocode_match':
                    stats['geocode_match'] += 1
                elif status == 'geocode_mismatch':
                    stats['geocode_mismatch'] += 1
                    session.add(RejectedEntries(
                        raw_entry_id=raw_entry.id,
                        rejection_reason='geocode_mismatch',
                        raw_parse_error=(
                            f"Confidence {candidate.geo_confidence:.2f}, "
                            f"matched: {candidate.geo_matched_name}"
                        ),
                    ))
                elif status == 'geocode_failed':
                    stats['geocode_failed'] += 1

        session.commit()

        _update_source_meta(session, source, stats)
        session.commit()

    logger.info("Pipeline complete for %s: %s", source_name, stats)
    return stats


def geocode_pending(source_name=None):
    """Geocode existing unverified candidates without re-scraping.

    Only candidates with a street address are geocoded -- wiki entries
    (no street) are skipped since Nominatim needs a street for a
    meaningful result.

    Commits in batches of 50 so progress survives interruptions.
    Already-geocoded candidates won't be re-queried on a subsequent
    run because their status changes from 'unverified'.

    Args:
        source_name: Limit to candidates from this source (optional).

    Returns:
        dict with geocoding counts.
    """
    import time as _time

    engine = build_engine()
    init_db(engine)

    stats = {
        'total_pending': 0,
        'skipped_no_street': 0,
        'geocoded': 0,
        'geocode_match': 0,
        'geocode_mismatch': 0,
        'geocode_failed': 0,
    }

    BATCH_SIZE = 50

    with Session(engine) as session:
        query = (
            session.query(Candidates)
            .filter(Candidates.verification_status == 'unverified')
        )

        if source_name:
            source = session.query(Sources).filter_by(name=source_name).first()
            if not source:
                logger.error("Source '%s' not found", source_name)
                return stats
            query = (
                query
                .join(RawEntries, Candidates.raw_entry_id == RawEntries.id)
                .filter(RawEntries.source_id == source.id)
            )

        pending = query.all()
        stats['total_pending'] = len(pending)
        logger.info("Found %d unverified candidates%s",
                     len(pending),
                     f" for source '{source_name}'" if source_name else "")

        streetless = [c for c in pending if not (c.street or '').strip()]
        with_street = [c for c in pending if (c.street or '').strip()]
        stats['skipped_no_street'] = len(streetless)

        if streetless:
            logger.info("Skipping %d candidates with no street address "
                        "(wiki entries)", len(streetless))

        logger.info("Geocoding %d candidates with street addresses",
                     len(with_street))

        start = _time.time()

        for i, candidate in enumerate(with_street, 1):
            status = geocode_candidate(candidate)
            stats['geocoded'] += 1

            if status == 'geocode_match':
                stats['geocode_match'] += 1
            elif status == 'geocode_mismatch':
                stats['geocode_mismatch'] += 1
                session.add(RejectedEntries(
                    raw_entry_id=candidate.raw_entry_id,
                    rejection_reason='geocode_mismatch',
                    raw_parse_error=(
                        f"Confidence {candidate.geo_confidence:.2f}, "
                        f"matched: {candidate.geo_matched_name}"
                    ),
                ))
            elif status == 'geocode_failed':
                stats['geocode_failed'] += 1

            if i % BATCH_SIZE == 0:
                session.commit()
                elapsed = _time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (len(with_street) - i) / rate if rate > 0 else 0
                logger.info(
                    "Progress: %d/%d (%.0f%%) | %.1f/sec | ~%.0f min remaining",
                    i, len(with_street),
                    100 * i / len(with_street),
                    rate, remaining / 60,
                )

        session.commit()

        elapsed = _time.time() - start
        logger.info(
            "Geocoding complete: %d processed in %.1f min | "
            "match=%d, mismatch=%d, failed=%d",
            stats['geocoded'], elapsed / 60,
            stats['geocode_match'], stats['geocode_mismatch'],
            stats['geocode_failed'],
        )

    return stats


def _update_source_meta(session, source, stats):
    """Update the sources table with run metadata."""
    source.last_run_at = datetime.now(timezone.utc)
    source.last_run_entry_count = stats['scraped']

    if stats['parse_failed'] == 0 and stats['scraped'] > 0:
        source.last_run_status = 'success'
    elif stats['parsed'] > 0:
        source.last_run_status = 'partial'
    else:
        source.last_run_status = 'failed'
