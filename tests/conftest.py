import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault('ICEMAKER_DB_URL', 'sqlite://')

from pipeline.staging import (
    Base, Sources, RawEntries, Candidates, RejectedEntries,
    LocationSources, LocationAliases, Locations, init_db,
)


@pytest.fixture(scope='session')
def db_engine():
    engine = create_engine('sqlite://')
    init_db(engine)
    return engine


@pytest.fixture
def db_session(db_engine):
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def make_source(db_session):
    _counter = [0]

    def _factory(**overrides):
        _counter[0] += 1
        defaults = {
            'name': f'test_source_{_counter[0]}',
            'parser_module': 'parsers.test',
            'formatter_module': None,
            'enabled': True,
            'confidence_weight': 1.0,
        }
        defaults.update(overrides)
        src = Sources(**defaults)
        db_session.add(src)
        db_session.flush()
        return src

    return _factory


@pytest.fixture
def make_raw_entry(db_session, make_source):
    _default_source = [None]

    def _factory(**overrides):
        if 'source_id' not in overrides:
            if _default_source[0] is None:
                _default_source[0] = make_source(name='default_source')
            overrides['source_id'] = _default_source[0].id

        defaults = {
            'raw_name': 'Test Rink',
            'raw_address': '100 Main St, Springfield, IL',
            'raw_fingerprint': str(uuid4()),
            'parse_status': 'parsed',
        }
        defaults.update(overrides)
        entry = RawEntries(**defaults)
        db_session.add(entry)
        db_session.flush()
        return entry

    return _factory


@pytest.fixture
def make_candidate(db_session, make_raw_entry):
    def _factory(**overrides):
        if 'raw_entry_id' not in overrides:
            raw = make_raw_entry()
            overrides['raw_entry_id'] = raw.id

        defaults = {
            'name': 'Test Rink',
            'street': '100 MAIN STREET',
            'city': 'Springfield',
            'state': 'IL',
            'zip': '62701',
            'country': 'US',
            'verification_status': 'geocode_match',
        }
        defaults.update(overrides)
        cand = Candidates(**defaults)
        db_session.add(cand)
        db_session.flush()
        return cand

    return _factory


@pytest.fixture
def make_location(db_session):
    def _factory(**overrides):
        defaults = {
            'rink_id': str(uuid4()),
            'rink_name': 'Test Rink',
            'rink_address': '100 Main Street',
            'rink_city': 'Springfield',
            'rink_state': 'IL',
            'rink_country': 'US',
            'rink_zip': '62701',
            'rink_status': 'active',
            'data_source': 'test_source',
        }
        defaults.update(overrides)
        loc = Locations(**defaults)
        db_session.add(loc)
        db_session.flush()
        return loc

    return _factory


def fixture_path(filename):
    return Path(__file__).parent / 'fixtures' / filename
