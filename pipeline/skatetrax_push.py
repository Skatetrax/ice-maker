"""Push ice-maker locations into the Skatetrax locations table.

This is the final step that makes ice-maker's directory usable by
Skatetrax.  It reads active, verified locations from ice-maker and
upserts them into Skatetrax's locations table via SKATETRAX_DB_URL.

Safety guarantees:
- Existing Skatetrax rinks are never deleted.
- Hand-curated fields (rink_name, rink_phone, rink_url, rink_tz) are
  never overwritten on existing entries.
- Name mismatches are recorded as aliases in ice-maker's
  location_aliases table for future "aka / formerly known as" use.
- Only active locations with a zip code are pushed.
- A dry-run mode previews changes without writing.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, DateTime, create_engine, inspect,
)
from sqlalchemy.orm import Session, declarative_base

from pipeline.staging import Locations, LocationAliases, build_engine, init_db
from config import SKATETRAX_DB_URL

logger = logging.getLogger(__name__)

_SkatetraxBase = declarative_base()


class SkatetraxLocation(_SkatetraxBase):
    """Writable mirror of the Skatetrax locations table."""

    __tablename__ = 'locations'

    rink_id = Column(String, primary_key=True)
    rink_name = Column(String, nullable=False)
    rink_address = Column(String)
    rink_city = Column(String, nullable=False)
    rink_state = Column(String, nullable=False)
    rink_country = Column(String, nullable=False)
    rink_zip = Column(String, nullable=False)
    rink_url = Column(String)
    rink_phone = Column(String)
    rink_tz = Column(String)
    data_source = Column(String, nullable=False)
    date_created = Column(DateTime)


def _record_alias(im_session, location_id, alias_name, data_source):
    """Write a name to location_aliases if it doesn't already exist."""
    exists = (
        im_session.query(LocationAliases)
        .filter(
            LocationAliases.location_id == location_id,
            LocationAliases.alias_name == alias_name,
        )
        .first()
    )
    if exists:
        return False

    im_session.add(LocationAliases(
        location_id=location_id,
        alias_name=alias_name,
        notes=f"auto: push name mismatch (source: {data_source})",
    ))
    return True


def push_locations(dry_run=False):
    """Push ice-maker's active locations into Skatetrax.

    For each ice-maker location:
    - If rink_id exists in Skatetrax: update address/city/state/zip but
      preserve the curated name and hand-curated fields (phone, url, tz).
      If the ice-maker name differs from the Skatetrax name, the
      ice-maker name is recorded as an alias.
    - If rink_id is new: insert the full row.

    Args:
        dry_run: If True, log what would happen without writing.

    Returns:
        dict with counts of what happened.
    """
    stats = {
        'icemaker_active': 0,
        'already_in_skatetrax': 0,
        'updated': 0,
        'inserted': 0,
        'aliases_created': 0,
        'skipped_no_zip': 0,
        'errors': 0,
    }

    if not SKATETRAX_DB_URL:
        logger.error(
            "SKATETRAX_DB_URL is not set. "
            "Cannot push locations without a target database."
        )
        return stats

    icemaker_engine = build_engine()
    init_db(icemaker_engine)

    try:
        skatetrax_engine = create_engine(
            SKATETRAX_DB_URL,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=300,
        )
        with skatetrax_engine.connect() as conn:
            conn.close()
    except Exception as e:
        logger.error("Cannot connect to Skatetrax DB: %s", e)
        return stats

    if not inspect(skatetrax_engine).has_table('locations'):
        logger.error(
            "Skatetrax DB has no 'locations' table. "
            "Is SKATETRAX_DB_URL pointing to the right database?"
        )
        return stats

    with Session(icemaker_engine) as im_session:
        icemaker_locs = (
            im_session.query(Locations)
            .filter(Locations.rink_status == 'active')
            .order_by(Locations.rink_state, Locations.rink_city)
            .all()
        )
        stats['icemaker_active'] = len(icemaker_locs)
        logger.info(
            "Found %d active locations in ice-maker to push", len(icemaker_locs)
        )

        im_data = []
        for loc in icemaker_locs:
            if not loc.rink_zip:
                stats['skipped_no_zip'] += 1
                continue
            im_data.append({
                'rink_id': loc.rink_id,
                'rink_name': loc.rink_name,
                'rink_address': loc.rink_address or '',
                'rink_city': loc.rink_city,
                'rink_state': loc.rink_state,
                'rink_country': loc.rink_country or 'US',
                'rink_zip': loc.rink_zip,
                'data_source': loc.data_source,
                'date_created': loc.date_created or datetime.now(timezone.utc),
            })

    logger.info(
        "Pushing %d locations (%d skipped, no zip)",
        len(im_data), stats['skipped_no_zip'],
    )

    with Session(skatetrax_engine) as st_session:
        existing = {}
        for row in st_session.query(
            SkatetraxLocation.rink_id, SkatetraxLocation.rink_name,
        ).all():
            existing[str(row[0])] = row[1]

        stats['already_in_skatetrax'] = len(existing)
        logger.info(
            "Skatetrax currently has %d locations", len(existing),
        )

        alias_queue = []

        for entry in im_data:
            rid = entry['rink_id']

            if rid in existing:
                st_name = existing[rid]
                im_name = entry['rink_name']
                name_differs = (
                    st_name and im_name
                    and st_name.strip().lower() != im_name.strip().lower()
                )

                if dry_run:
                    if name_differs:
                        logger.info(
                            "  [DRY-RUN] Would update (name kept): %s"
                            " -- ice-maker has \"%s\"",
                            st_name, im_name,
                        )
                    else:
                        logger.info(
                            "  [DRY-RUN] Would update: %s (%s, %s)",
                            st_name, entry['rink_city'],
                            entry['rink_state'],
                        )
                    stats['updated'] += 1
                    if name_differs:
                        stats['aliases_created'] += 1
                    continue

                st_row = st_session.get(SkatetraxLocation, rid)
                if st_row:
                    st_row.rink_address = entry['rink_address']
                    st_row.rink_city = entry['rink_city']
                    st_row.rink_state = entry['rink_state']
                    st_row.rink_country = entry['rink_country']
                    st_row.rink_zip = entry['rink_zip']
                    stats['updated'] += 1

                if name_differs:
                    alias_queue.append((
                        rid, im_name, entry['data_source'],
                    ))
            else:
                if dry_run:
                    logger.info(
                        "  [DRY-RUN] Would insert: %s (%s, %s) id=%s",
                        entry['rink_name'], entry['rink_city'],
                        entry['rink_state'], rid,
                    )
                    stats['inserted'] += 1
                    continue

                st_session.add(SkatetraxLocation(
                    rink_id=entry['rink_id'],
                    rink_name=entry['rink_name'],
                    rink_address=entry['rink_address'],
                    rink_city=entry['rink_city'],
                    rink_state=entry['rink_state'],
                    rink_country=entry['rink_country'],
                    rink_zip=entry['rink_zip'],
                    data_source=entry['data_source'],
                    date_created=entry['date_created'],
                ))
                stats['inserted'] += 1

        if not dry_run:
            st_session.commit()
            logger.info("Committed changes to Skatetrax DB")
        else:
            logger.info("Dry-run complete -- no changes written")

    if alias_queue and not dry_run:
        with Session(icemaker_engine) as im_session:
            for location_id, alias_name, data_source in alias_queue:
                if _record_alias(im_session, location_id,
                                 alias_name, data_source):
                    stats['aliases_created'] += 1
            im_session.commit()
            logger.info(
                "Recorded %d new aliases in ice-maker",
                stats['aliases_created'],
            )

    logger.info(
        "Push complete: %d updated, %d inserted, %d aliases, "
        "%d already present",
        stats['updated'], stats['inserted'],
        stats['aliases_created'], stats['already_in_skatetrax'],
    )
    return stats
