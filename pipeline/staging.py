import logging

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime, ForeignKey,
    UniqueConstraint, create_engine, text,
)
from sqlalchemy.orm import declarative_base, mapped_column, Mapped, Session
from uuid import uuid4, UUID as UUIDV4
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

Base = declarative_base()

_engine_cache = {}


def build_engine(url=None):
    """Create or return a cached engine with dialect-appropriate settings.

    Postgres connections get pooling tuned for a batch pipeline.
    SQLite connections get no pooling (used in tests and local dev).
    """
    if url is None:
        from config import DATABASE_URL
        url = DATABASE_URL

    if url in _engine_cache:
        return _engine_cache[url]

    kwargs = {}
    if url.startswith('postgresql'):
        kwargs.update(
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            pool_recycle=300,
        )

    engine = create_engine(url, **kwargs)
    _engine_cache[url] = engine
    return engine


class Sources(Base):
    """Registry of all data sources. Enables dynamic enable/disable
    and tracks reliability over time."""

    __tablename__ = 'sources'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    parser_module = Column(String, nullable=False)
    formatter_module = Column(String, nullable=True)
    enabled = Column(Boolean, default=True, nullable=False)
    last_run_at = Column(DateTime, nullable=True)
    last_run_status = Column(String, nullable=True)
    last_run_entry_count = Column(Integer, nullable=True)
    confidence_weight = Column(Float, default=1.0, nullable=False)
    notes = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class RawEntries(Base):
    """Every scraped row exactly as received, with a fingerprint
    for change detection."""

    __tablename__ = 'raw_entries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=False)
    raw_name = Column(String, nullable=False)
    raw_address = Column(String, nullable=False)
    raw_fingerprint = Column(String, nullable=False, unique=True, index=True)
    scrape_date = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    parse_status = Column(String, default='pending', nullable=False)


class Candidates(Base):
    """Parsed, normalized, geocode-verified entries ready for
    promotion to the locations table."""

    __tablename__ = 'candidates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_entry_id = Column(Integer, ForeignKey('raw_entries.id'), nullable=False)
    name = Column(String, nullable=False)
    street = Column(String, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    zip = Column(String, nullable=True)
    country = Column(String, default='US')
    geo_lat = Column(Float, nullable=True)
    geo_lon = Column(Float, nullable=True)
    geo_confidence = Column(Float, nullable=True)
    geo_matched_name = Column(String, nullable=True)
    verification_status = Column(String, default='unverified', nullable=False)
    location_id = Column(String, nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class RejectedEntries(Base):
    """Entries that failed parsing or verification, awaiting
    human review."""

    __tablename__ = 'rejected_entries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_entry_id = Column(Integer, ForeignKey('raw_entries.id'), nullable=False)
    rejection_reason = Column(String, nullable=False)
    raw_parse_error = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    reviewed = Column(Boolean, default=False)


class LocationSources(Base):
    """Junction table tracking which sources corroborate each
    promoted rink. Enables multi-source confidence scoring and
    disappearance detection."""

    __tablename__ = 'location_sources'
    __table_args__ = (
        UniqueConstraint('location_id', 'source_id', name='uq_location_source'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(String, nullable=False)
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=False)
    candidate_id = Column(Integer, ForeignKey('candidates.id'), nullable=True)
    first_seen_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_seen_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_present = Column(Boolean, default=True, nullable=False)


class LocationAliases(Base):
    """Tracks previous names for a rink. When a rink is renamed,
    the old name moves here so searches for the old name still
    resolve to the correct location UUID."""

    __tablename__ = 'location_aliases'

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(String, nullable=False)
    alias_name = Column(String, nullable=False)
    effective_from = Column(DateTime, nullable=True)
    effective_until = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    notes = Column(String, nullable=True)


class Locations(Base):
    """Rink directory table mirroring the Skatetrax locations schema.

    Uses String(36) for the UUID primary key so the same model works
    on both SQLite (tests/local dev) and PostgreSQL (production).
    """

    __tablename__ = 'locations'

    rink_id = Column(String(36), primary_key=True,
                     default=lambda: str(uuid4()))
    rink_name = Column(String(255), nullable=False)
    rink_address = Column(String(255), nullable=True)
    rink_city = Column(String(100), nullable=False)
    rink_state = Column(String(2), nullable=False)
    rink_country = Column(String(2), nullable=False, default='US')
    rink_zip = Column(String(10), nullable=False)
    rink_url = Column(String(255), nullable=True)
    rink_phone = Column(String(20), nullable=True)
    rink_tz = Column(String(60), nullable=True)
    rink_status = Column(String(30), nullable=False, default='active')
    data_source = Column(String(50), nullable=False)
    date_created = Column(DateTime,
                          default=lambda: datetime.now(timezone.utc))


SEED_SOURCES = [
    {
        'name': 'sk8stuff',
        'parser_module': 'parsers.sk8stuff',
        'formatter_module': 'formatters.sk8stuff',
        'notes': 'Single-page PHP table, all rinks in one request',
    },
    {
        'name': 'arena_guide',
        'parser_module': 'parsers.arena_guide',
        'formatter_module': 'formatters.arena_guide',
        'notes': 'CMS pagination, ~1773 posts, site owner permission granted',
    },
    {
        'name': 'learntoskate',
        'parser_module': 'parsers.learntoskate',
        'formatter_module': 'formatters.learntoskate',
        'notes': 'JSON API, returns programs not rinks directly',
    },
    {
        'name': 'fandom_wiki',
        'parser_module': 'parsers.fandom_wiki',
        'formatter_module': None,
        'notes': 'Curated wiki list, no street addresses but has defunct status, clubs, and websites',
    },
    {
        'name': 'skatetrax',
        'parser_module': 'pipeline.ice_time_sync',
        'formatter_module': None,
        'notes': 'Skatetrax ice_time table -- ultimate proof a rink exists',
        'confidence_weight': 2.0,
    },
]


_MIGRATIONS = [
    "ALTER TABLE locations ADD COLUMN rink_status VARCHAR(30) NOT NULL DEFAULT 'active'",
]


def init_db(engine):
    """Create all staging tables, seed sources, and run migrations."""
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        existing = session.query(Sources).count()
        if existing == 0:
            for src in SEED_SOURCES:
                session.add(Sources(**src))
            session.commit()

        for src_def in SEED_SOURCES:
            exists = session.query(Sources).filter_by(name=src_def['name']).first()
            if not exists:
                session.add(Sources(**src_def))
        session.commit()

    with engine.connect() as conn:
        for sql in _MIGRATIONS:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                conn.rollback()
