import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine, func, Column, String, Integer, DateTime
from sqlalchemy.orm import Session, declarative_base

from pipeline.staging import (
    Sources, Locations, LocationSources, build_engine, init_db,
)
from config import SKATETRAX_DB_URL

logger = logging.getLogger(__name__)

SkatetraxBase = declarative_base()


class IceTime(SkatetraxBase):
    """Read-only mirror of the Skatetrax ice_time table.

    Only the columns needed for rink confirmation are mapped.
    """

    __tablename__ = 'ice_time'

    ice_time_id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    rink_id = Column(String)


def sync_ice_time():
    """Query the Skatetrax DB for distinct rink_ids from ice_time
    and record them as LocationSources entries for the 'skatetrax'
    source.

    Each rink_id that appears in ice_time is proof that at least one
    skater has been there.  This is the highest-confidence signal
    available.

    Returns a stats dict.
    """
    stats = {
        'total_rinks_in_ice_time': 0,
        'confirmed': 0,
        'missing_from_directory': 0,
    }

    icemaker_engine = build_engine()
    init_db(icemaker_engine)

    try:
        skatetrax_engine = create_engine(SKATETRAX_DB_URL)
        skatetrax_engine.connect().close()
    except Exception as e:
        logger.error(
            "Cannot connect to Skatetrax DB at %s: %s",
            SKATETRAX_DB_URL, e,
        )
        logger.info(
            "Set SKATETRAX_DB_URL to a valid connection string "
            "to enable ice_time sync"
        )
        return stats

    with Session(skatetrax_engine) as st_session:
        rink_rows = (
            st_session.query(
                IceTime.rink_id,
                func.max(IceTime.date).label('last_skated'),
            )
            .group_by(IceTime.rink_id)
            .all()
        )

    stats['total_rinks_in_ice_time'] = len(rink_rows)
    logger.info("Found %d distinct rink_ids in ice_time", len(rink_rows))

    if not rink_rows:
        return stats

    with Session(icemaker_engine) as session:
        skatetrax_source = (
            session.query(Sources).filter_by(name='skatetrax').first()
        )
        if not skatetrax_source:
            logger.error("'skatetrax' source not found in sources table")
            return stats

        for rink_id, last_skated in rink_rows:
            rink_id_str = str(rink_id)

            location = (
                session.query(Locations)
                .filter_by(rink_id=rink_id_str)
                .first()
            )

            if not location:
                logger.debug(
                    "Rink %s exists in ice_time but not in directory",
                    rink_id_str,
                )
                stats['missing_from_directory'] += 1
                continue

            existing_link = (
                session.query(LocationSources)
                .filter_by(
                    location_id=rink_id_str,
                    source_id=skatetrax_source.id,
                )
                .first()
            )

            if existing_link:
                existing_link.last_seen_at = last_skated or datetime.now(timezone.utc)
                existing_link.is_present = True
            else:
                session.add(LocationSources(
                    location_id=rink_id_str,
                    source_id=skatetrax_source.id,
                    candidate_id=None,
                    first_seen_at=last_skated or datetime.now(timezone.utc),
                    last_seen_at=last_skated or datetime.now(timezone.utc),
                ))

            stats['confirmed'] += 1

        session.commit()

    logger.info(
        "ice_time sync complete: %d confirmed, %d missing from directory "
        "(out of %d total)",
        stats['confirmed'],
        stats['missing_from_directory'],
        stats['total_rinks_in_ice_time'],
    )
    return stats
