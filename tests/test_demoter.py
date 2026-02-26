"""Tests for pipeline/demoter.py -- demote, merge, rename, search."""

import pytest
from unittest.mock import patch
from pipeline.demoter import (
    _find_location, demote_location, merge_locations,
    rename_location, search_locations, VALID_STATUSES,
)
from pipeline.staging import (
    Locations, LocationSources, LocationAliases, Candidates,
    RawEntries, Sources, build_engine, init_db,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


@pytest.fixture
def demo_engine():
    engine = create_engine('sqlite://')
    init_db(engine)
    return engine


@pytest.fixture
def demo_session(demo_engine):
    with Session(demo_engine) as session:
        yield session


def _add_location(session, name, city='Springfield', state='IL', status='active'):
    loc = Locations(
        rink_name=name,
        rink_address='100 Main Street',
        rink_city=city,
        rink_state=state,
        rink_country='US',
        rink_zip='62701',
        rink_status=status,
        data_source='test',
    )
    session.add(loc)
    session.flush()
    return loc


class TestFindLocation:
    def test_find_by_rink_id(self, demo_session):
        loc = _add_location(demo_session, 'Test Rink')
        found = _find_location(demo_session, rink_id=loc.rink_id)
        assert found is not None
        assert found.rink_name == 'Test Rink'

    def test_find_by_exact_name(self, demo_session):
        _add_location(demo_session, 'Exact Match Rink')
        found = _find_location(demo_session, name='Exact Match Rink')
        assert found is not None

    def test_find_by_partial_name(self, demo_session):
        _add_location(demo_session, 'Polar Ice Raleigh')
        found = _find_location(demo_session, name='Polar')
        assert found is not None
        assert found.rink_name == 'Polar Ice Raleigh'

    def test_ambiguous_name_returns_none(self, demo_session):
        _add_location(demo_session, 'Ice House North')
        _add_location(demo_session, 'Ice House South')
        found = _find_location(demo_session, name='Ice House')
        assert found is None

    def test_no_match_returns_none(self, demo_session):
        found = _find_location(demo_session, name='Nonexistent')
        assert found is None

    def test_no_args_returns_none(self, demo_session):
        found = _find_location(demo_session)
        assert found is None


class TestDemoteLocation:
    def test_changes_status(self, demo_engine):
        with Session(demo_engine) as session:
            loc = _add_location(session, 'Demote Me')
            session.commit()
            rid = loc.rink_id

        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            result = demote_location(rink_id=rid, status='closed_permanently')
        assert result['new_status'] == 'closed_permanently'
        assert result['old_status'] == 'active'

    def test_invalid_status_rejected(self):
        result = demote_location(name='Any', status='bogus')
        assert 'error' in result

    def test_not_found(self, demo_engine):
        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            result = demote_location(name='Ghost Rink')
        assert 'error' in result


class TestMergeLocations:
    def test_merge_creates_alias(self, demo_engine):
        with Session(demo_engine) as session:
            loc_from = _add_location(session, 'Sugar Mountain A')
            loc_into = _add_location(session, 'Sugar Mountain Resort')
            session.commit()
            from_id = loc_from.rink_id
            into_id = loc_into.rink_id

        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            result = merge_locations(from_id, into_id)

        assert result['alias_created'] is True

        with Session(demo_engine) as session:
            alias = session.query(LocationAliases).filter_by(
                location_id=into_id
            ).first()
            assert alias is not None
            assert alias.alias_name == 'Sugar Mountain A'

            src = session.query(Locations).filter_by(rink_id=from_id).first()
            assert src.rink_status == 'merged'

    def test_merge_into_self_rejected(self, demo_engine):
        result = merge_locations('same-id', 'same-id')
        assert 'error' in result

    def test_merge_moves_sources(self, demo_engine):
        with Session(demo_engine) as session:
            loc_from = _add_location(session, 'From Rink')
            loc_into = _add_location(session, 'Into Rink')
            src = session.query(Sources).first()
            session.add(LocationSources(
                location_id=loc_from.rink_id,
                source_id=src.id,
            ))
            session.commit()
            from_id = loc_from.rink_id
            into_id = loc_into.rink_id

        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            result = merge_locations(from_id, into_id)

        assert result['sources_moved'] >= 1

    def test_merge_repoints_candidates(self, demo_engine):
        with Session(demo_engine) as session:
            loc_from = _add_location(session, 'Cand From')
            loc_into = _add_location(session, 'Cand Into')

            src = session.query(Sources).first()
            raw = RawEntries(
                source_id=src.id,
                raw_name='Test',
                raw_address='Test Addr',
                raw_fingerprint='fp_merge_cand',
                parse_status='parsed',
            )
            session.add(raw)
            session.flush()

            cand = Candidates(
                raw_entry_id=raw.id,
                name='Test Cand',
                location_id=loc_from.rink_id,
                verification_status='geocode_match',
            )
            session.add(cand)
            session.commit()
            from_id = loc_from.rink_id
            into_id = loc_into.rink_id
            cand_id = cand.id

        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            result = merge_locations(from_id, into_id)

        assert result['candidates_repointed'] == 1

        with Session(demo_engine) as session:
            c = session.get(Candidates, cand_id)
            assert c.location_id == into_id


class TestRenameLocation:
    def test_rename_saves_alias(self, demo_engine):
        with Session(demo_engine) as session:
            loc = _add_location(session, 'Old Name Rink')
            session.commit()
            rid = loc.rink_id

        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            result = rename_location(rink_id=rid, new_name='New Name Rink')

        assert result['old_name'] == 'Old Name Rink'
        assert result['new_name'] == 'New Name Rink'
        assert result['alias_created'] is True

        with Session(demo_engine) as session:
            loc = session.query(Locations).filter_by(rink_id=rid).first()
            assert loc.rink_name == 'New Name Rink'

    def test_same_name_no_alias(self, demo_engine):
        with Session(demo_engine) as session:
            loc = _add_location(session, 'Same Name')
            session.commit()
            rid = loc.rink_id

        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            result = rename_location(rink_id=rid, new_name='Same Name')

        assert result['alias_created'] is False

    def test_no_new_name_error(self):
        result = rename_location(name='Test', new_name=None)
        assert 'error' in result


class TestSearchLocations:
    def test_partial_match_returns_results(self, demo_engine):
        with Session(demo_engine) as session:
            _add_location(session, 'Polar Ice Raleigh', city='Raleigh', state='NC')
            _add_location(session, 'Polar Ice Cary', city='Cary', state='NC')
            session.commit()

        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            count = search_locations('Polar')
        assert count == 2

    def test_state_filter(self, demo_engine):
        with Session(demo_engine) as session:
            _add_location(session, 'Rink NC', city='Raleigh', state='NC')
            _add_location(session, 'Rink VA', city='Richmond', state='VA')
            session.commit()

        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            count = search_locations('Rink', state='NC')
        assert count == 1

    def test_no_match_returns_zero(self, demo_engine):
        with patch('pipeline.demoter.build_engine', return_value=demo_engine):
            count = search_locations('Nonexistent')
        assert count == 0
