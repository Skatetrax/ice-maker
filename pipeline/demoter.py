import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from pipeline.staging import (
    Locations, LocationSources, LocationAliases, Candidates,
    build_engine, init_db,
)

logger = logging.getLogger(__name__)

VALID_STATUSES = ('active', 'closed_permanently', 'seasonal', 'merged', 'disabled')


def _find_location(session, name=None, rink_id=None):
    """Resolve a location by rink_id or by exact/partial name match."""
    if rink_id:
        loc = session.query(Locations).filter_by(rink_id=rink_id).first()
        if loc:
            return loc
        logger.error("No location found with rink_id=%s", rink_id)
        return None

    if name:
        loc = (
            session.query(Locations)
            .filter(Locations.rink_name.ilike(name))
            .first()
        )
        if loc:
            return loc

        partial = (
            session.query(Locations)
            .filter(Locations.rink_name.ilike(f'%{name}%'))
            .all()
        )
        if len(partial) == 1:
            return partial[0]
        if len(partial) > 1:
            logger.error(
                "Ambiguous name '%s' matched %d locations:", name, len(partial)
            )
            for p in partial:
                logger.error(
                    "  %s | %s, %s | id=%s",
                    p.rink_name, p.rink_city, p.rink_state, p.rink_id,
                )
            return None

        logger.error("No location found matching name '%s'", name)
        return None

    logger.error("Must provide --name or --rink-id")
    return None


def demote_location(name=None, rink_id=None, status='disabled'):
    """Change a location's rink_status.

    Returns a dict describing what happened.
    """
    if status not in VALID_STATUSES:
        logger.error(
            "Invalid status '%s'. Valid: %s", status, VALID_STATUSES
        )
        return {'error': f'Invalid status: {status}'}

    engine = build_engine()
    init_db(engine)

    with Session(engine) as session:
        loc = _find_location(session, name=name, rink_id=rink_id)
        if not loc:
            return {'error': 'Location not found'}

        old_status = loc.rink_status
        loc.rink_status = status
        session.commit()

        logger.info(
            "Demoted '%s' (%s, %s): %s -> %s",
            loc.rink_name, loc.rink_city, loc.rink_state,
            old_status, status,
        )

        return {
            'rink_id': loc.rink_id,
            'rink_name': loc.rink_name,
            'city': loc.rink_city,
            'state': loc.rink_state,
            'old_status': old_status,
            'new_status': status,
        }


def merge_locations(from_id, into_id):
    """Merge one location into another.

    - Moves LocationSources from source to target
    - Creates a LocationAlias for the old name
    - Re-points candidates from source to target
    - Sets source rink_status to 'merged'

    Returns a dict describing what happened.
    """
    if from_id == into_id:
        return {'error': 'Cannot merge a location into itself'}

    engine = build_engine()
    init_db(engine)

    stats = {
        'sources_moved': 0,
        'sources_updated': 0,
        'candidates_repointed': 0,
        'alias_created': False,
    }

    with Session(engine) as session:
        source_loc = session.query(Locations).filter_by(rink_id=from_id).first()
        target_loc = session.query(Locations).filter_by(rink_id=into_id).first()

        if not source_loc:
            return {'error': f'Source location not found: {from_id}'}
        if not target_loc:
            return {'error': f'Target location not found: {into_id}'}

        source_links = (
            session.query(LocationSources)
            .filter_by(location_id=from_id)
            .all()
        )

        for link in source_links:
            existing_target_link = (
                session.query(LocationSources)
                .filter_by(location_id=into_id, source_id=link.source_id)
                .first()
            )

            if existing_target_link:
                if link.first_seen_at and (
                    not existing_target_link.first_seen_at or
                    link.first_seen_at < existing_target_link.first_seen_at
                ):
                    existing_target_link.first_seen_at = link.first_seen_at
                if link.last_seen_at and (
                    not existing_target_link.last_seen_at or
                    link.last_seen_at > existing_target_link.last_seen_at
                ):
                    existing_target_link.last_seen_at = link.last_seen_at
                session.delete(link)
                stats['sources_updated'] += 1
            else:
                link.location_id = into_id
                stats['sources_moved'] += 1

        if source_loc.rink_name != target_loc.rink_name:
            session.add(LocationAliases(
                location_id=into_id,
                alias_name=source_loc.rink_name,
                effective_until=datetime.now(timezone.utc),
                notes=f'Merged from {from_id}',
            ))
            stats['alias_created'] = True

        repointed = (
            session.query(Candidates)
            .filter_by(location_id=from_id)
            .all()
        )
        for cand in repointed:
            cand.location_id = into_id
            stats['candidates_repointed'] += 1

        source_loc.rink_status = 'merged'
        session.commit()

        logger.info(
            "Merged '%s' (%s) into '%s' (%s): %d sources moved, "
            "%d updated, %d candidates repointed",
            source_loc.rink_name, from_id,
            target_loc.rink_name, into_id,
            stats['sources_moved'], stats['sources_updated'],
            stats['candidates_repointed'],
        )

        return {
            'from': f'{source_loc.rink_name} ({source_loc.rink_city}, {source_loc.rink_state})',
            'into': f'{target_loc.rink_name} ({target_loc.rink_city}, {target_loc.rink_state})',
            **stats,
        }


def rename_location(name=None, rink_id=None, new_name=None):
    """Rename a location, saving the old name as an alias.

    Returns a dict describing what happened.
    """
    if not new_name:
        return {'error': 'Must provide a new name'}

    engine = build_engine()
    init_db(engine)

    with Session(engine) as session:
        loc = _find_location(session, name=name, rink_id=rink_id)
        if not loc:
            return {'error': 'Location not found'}

        old_name = loc.rink_name

        if old_name != new_name:
            session.add(LocationAliases(
                location_id=loc.rink_id,
                alias_name=old_name,
                effective_until=datetime.now(timezone.utc),
                notes=f'Renamed to {new_name}',
            ))

        loc.rink_name = new_name
        session.commit()

        logger.info(
            "Renamed '%s' -> '%s' (%s, %s)",
            old_name, new_name, loc.rink_city, loc.rink_state,
        )

        return {
            'rink_id': loc.rink_id,
            'old_name': old_name,
            'new_name': new_name,
            'city': loc.rink_city,
            'state': loc.rink_state,
            'alias_created': old_name != new_name,
        }


def search_locations(query, state=None):
    """Search locations by partial name match, optionally filtered by state."""
    engine = build_engine()
    init_db(engine)

    with Session(engine) as session:
        q = session.query(Locations).filter(
            Locations.rink_name.ilike(f'%{query}%')
        )
        if state:
            q = q.filter(Locations.rink_state == state.upper())

        results = q.order_by(Locations.rink_state, Locations.rink_city).all()

        for loc in results:
            src_count = (
                session.query(LocationSources)
                .filter_by(location_id=loc.rink_id)
                .count()
            )
            print(
                f"  {loc.rink_name:45s} | {loc.rink_city:20s} | "
                f"{loc.rink_state} | {loc.rink_status:10s} | "
                f"sources={src_count} | id={loc.rink_id}"
            )

        print(f"\n  {len(results)} location(s) found")
        return len(results)
