"""Tests for pipeline/promoter.py -- all 3 promotion phases."""

import json
import pytest
import requests
import responses
from types import SimpleNamespace
from pipeline.promoter import (
    promote_verified, link_duplicates, link_wiki_entries,
    _find_skatetrax_match, _fetch_skatetrax_api,
)
from pipeline.staging import (
    Sources, RawEntries, Candidates, RejectedEntries,
    Locations, LocationSources,
)


@pytest.fixture
def source(db_session, make_source):
    return make_source(name='test_promoter_src', parser_module='parsers.test')


@pytest.fixture
def raw(db_session, source):
    entry = RawEntries(
        source_id=source.id,
        raw_name='Test Rink',
        raw_address='100 Main St, Springfield, IL',
        raw_fingerprint='fp_promoter_test',
        parse_status='parsed',
    )
    db_session.add(entry)
    db_session.flush()
    return entry


class TestPhase1PromoteVerified:
    def test_verified_candidate_creates_location(self, db_session, source, raw):
        cand = Candidates(
            raw_entry_id=raw.id,
            name='Springfield Ice',
            street='100 MAIN STREET',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session)
        assert stats['promoted_new'] == 1
        assert cand.location_id is not None

        loc = db_session.query(Locations).filter_by(rink_id=cand.location_id).first()
        assert loc is not None
        assert loc.rink_name == 'Springfield Ice'
        assert loc.rink_zip == '62701'

    def test_no_zip_skipped(self, db_session, raw):
        cand = Candidates(
            raw_entry_id=raw.id,
            name='No Zip Rink',
            street='100 MAIN STREET',
            city='Springfield',
            state='IL',
            zip=None,
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session)
        assert stats['skipped_no_zip'] == 1
        assert cand.location_id is None

    def test_duplicate_address_links_to_existing(self, db_session, source, raw):
        loc = Locations(
            rink_name='Springfield Ice',
            rink_address='100 MAIN STREET',
            rink_city='Springfield',
            rink_state='IL',
            rink_country='US',
            rink_zip='62701',
            data_source='test_promoter_src',
        )
        db_session.add(loc)
        db_session.flush()

        cand = Candidates(
            raw_entry_id=raw.id,
            name='Springfield Iceplex',
            street='100 MAIN STREET',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session)
        assert stats['promoted_existing'] >= 1
        assert cand.location_id == loc.rink_id

    def test_source_verified_also_promoted(self, db_session, raw):
        cand = Candidates(
            raw_entry_id=raw.id,
            name='LTS Rink',
            street='200 OAK AVENUE',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='source_verified',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session)
        assert stats['promoted_new'] == 1

    def test_idempotency(self, db_session, source, raw):
        cand = Candidates(
            raw_entry_id=raw.id,
            name='Idempotent Rink',
            street='300 ELM DRIVE',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats1 = promote_verified(db_session)
        assert stats1['promoted_new'] == 1

        stats2 = promote_verified(db_session)
        # Second run should find 0 candidates (location_id already set)
        assert stats2['promoted_new'] == 0
        assert stats2['promoted_existing'] == 0

    def test_merged_locations_skipped(self, db_session, source, raw):
        loc = Locations(
            rink_name='Merged Rink',
            rink_address='100 MAIN STREET',
            rink_city='Springfield',
            rink_state='IL',
            rink_country='US',
            rink_zip='62701',
            rink_status='merged',
            data_source='old_source',
        )
        db_session.add(loc)
        db_session.flush()

        cand = Candidates(
            raw_entry_id=raw.id,
            name='Merged Rink',
            street='100 MAIN STREET',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session)
        # Should create a NEW location, not link to the merged one
        assert stats['promoted_new'] == 1
        assert cand.location_id != loc.rink_id


class TestPhase2LinkDuplicates:
    def test_links_duplicate_to_primary(self, db_session, source, raw):
        primary_cand = Candidates(
            raw_entry_id=raw.id,
            name='Primary Rink',
            street='100 MAIN STREET',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='geocode_match',
        )
        db_session.add(primary_cand)
        db_session.flush()

        # Simulate promotion of primary
        loc = Locations(
            rink_name='Primary Rink',
            rink_address='100 MAIN STREET',
            rink_city='Springfield',
            rink_state='IL',
            rink_country='US',
            rink_zip='62701',
            data_source='test_promoter_src',
        )
        db_session.add(loc)
        db_session.flush()
        primary_cand.location_id = loc.rink_id

        raw2 = RawEntries(
            source_id=source.id,
            raw_name='Dup Rink',
            raw_address='100 Main St, Springfield, IL',
            raw_fingerprint='fp_dup_test',
            parse_status='parsed',
        )
        db_session.add(raw2)
        db_session.flush()

        dup_cand = Candidates(
            raw_entry_id=raw2.id,
            name='Duplicate Rink',
            street='100 MAIN STREET',
            city='Springfield',
            state='IL',
            verification_status='duplicate',
        )
        db_session.add(dup_cand)
        db_session.flush()

        db_session.add(RejectedEntries(
            raw_entry_id=raw2.id,
            rejection_reason='duplicate_address_exact',
            raw_parse_error=f'Matches candidate {primary_cand.id}: Primary Rink',
        ))
        db_session.flush()

        stats = link_duplicates(db_session)
        assert stats['linked'] == 1
        assert dup_cand.location_id == loc.rink_id

    def test_primary_not_promoted_skipped(self, db_session, source, raw):
        primary_cand = Candidates(
            raw_entry_id=raw.id,
            name='Unpromoted Primary',
            street='100 MAIN STREET',
            city='Springfield',
            state='IL',
            verification_status='geocode_match',
            location_id=None,
        )
        db_session.add(primary_cand)
        db_session.flush()

        raw2 = RawEntries(
            source_id=source.id,
            raw_name='Dup2',
            raw_address='100 Main St, Springfield, IL',
            raw_fingerprint='fp_dup_test2',
            parse_status='parsed',
        )
        db_session.add(raw2)
        db_session.flush()

        dup = Candidates(
            raw_entry_id=raw2.id,
            name='Dup2',
            verification_status='duplicate',
        )
        db_session.add(dup)
        db_session.flush()

        db_session.add(RejectedEntries(
            raw_entry_id=raw2.id,
            rejection_reason='duplicate_address_exact',
            raw_parse_error=f'Matches candidate {primary_cand.id}: Unpromoted',
        ))
        db_session.flush()

        stats = link_duplicates(db_session)
        assert stats['primary_not_promoted'] == 1


class TestPhase3LinkWikiEntries:
    def test_wiki_entry_links_to_existing_location(self, db_session, source, raw):
        loc = Locations(
            rink_name='Polar Iceplex',
            rink_address='100 MAIN STREET',
            rink_city='Raleigh',
            rink_state='NC',
            rink_country='US',
            rink_zip='27601',
            data_source='test_promoter_src',
        )
        db_session.add(loc)
        db_session.flush()

        raw2 = RawEntries(
            source_id=source.id,
            raw_name='Polar Iceplex',
            raw_address='Raleigh, North Carolina',
            raw_fingerprint='fp_wiki_test',
            parse_status='parsed',
        )
        db_session.add(raw2)
        db_session.flush()

        wiki_cand = Candidates(
            raw_entry_id=raw2.id,
            name='Polar Iceplex',
            street=None,
            city='Raleigh',
            state='NC',
            verification_status='unverified',
        )
        db_session.add(wiki_cand)
        db_session.flush()

        stats = link_wiki_entries(db_session)
        assert stats['linked'] == 1
        assert wiki_cand.location_id == loc.rink_id

    def test_no_match_stays_unlinked(self, db_session, source, raw):
        raw2 = RawEntries(
            source_id=source.id,
            raw_name='Unknown Wiki Rink',
            raw_address='Nowhere, NC',
            raw_fingerprint='fp_wiki_nomatch',
            parse_status='parsed',
        )
        db_session.add(raw2)
        db_session.flush()

        wiki_cand = Candidates(
            raw_entry_id=raw2.id,
            name='Unknown Wiki Rink',
            street=None,
            city='Nowhere',
            state='NC',
            verification_status='unverified',
        )
        db_session.add(wiki_cand)
        db_session.flush()

        stats = link_wiki_entries(db_session)
        assert stats['no_match'] == 1
        assert wiki_cand.location_id is None


def _fake_skatetrax_loc(rink_id, name, address, city, state):
    """Build a lightweight stand-in for _SkatetraxLocation."""
    return SimpleNamespace(
        rink_id=rink_id,
        rink_name=name,
        rink_address=address,
        rink_city=city,
        rink_state=state,
    )


class TestFindSkatetraxMatch:
    """Unit tests for _find_skatetrax_match (no DB needed)."""

    def test_exact_address_match(self):
        locs = [
            _fake_skatetrax_loc(
                'st-uuid-1', 'Cleland Ice Rink',
                '100 MAIN STREET', 'Springfield', 'IL',
            )
        ]
        result = _find_skatetrax_match(
            locs, 'Cleland Ice Rink',
            '100 MAIN STREET', 'Springfield', 'IL',
        )
        assert result is not None
        assert result.rink_id == 'st-uuid-1'

    def test_fuzzy_name_match(self):
        locs = [
            _fake_skatetrax_loc(
                'st-uuid-2', 'Springfield Ice Arena',
                None, 'Springfield', 'IL',
            )
        ]
        result = _find_skatetrax_match(
            locs, 'Springfield Ice',
            None, 'Springfield', 'IL',
        )
        assert result is not None
        assert result.rink_id == 'st-uuid-2'

    def test_no_match_returns_none(self):
        locs = [
            _fake_skatetrax_loc(
                'st-uuid-3', 'Polar Iceplex',
                '200 OAK AVENUE', 'Raleigh', 'NC',
            )
        ]
        result = _find_skatetrax_match(
            locs, 'Springfield Ice',
            '100 MAIN STREET', 'Springfield', 'IL',
        )
        assert result is None

    def test_empty_list_returns_none(self):
        assert _find_skatetrax_match([], 'Any', '1 St', 'City', 'ST') is None

    def test_none_list_returns_none(self):
        assert _find_skatetrax_match(None, 'Any', '1 St', 'City', 'ST') is None


class TestUuidAlignment:
    """Integration tests for Skatetrax UUID adoption during promotion."""

    def test_adopts_skatetrax_uuid(self, db_session, source, raw):
        """When Skatetrax knows a rink, the promoter uses its UUID."""
        skatetrax_uuid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
        st_locs = [
            _fake_skatetrax_loc(
                skatetrax_uuid, 'Springfield Ice',
                '100 MAIN STREET', 'Springfield', 'IL',
            )
        ]

        cand = Candidates(
            raw_entry_id=raw.id,
            name='Springfield Ice',
            street='100 MAIN STREET',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session, skatetrax_locations=st_locs)

        assert stats['adopted_skatetrax_uuid'] == 1
        assert stats['promoted_new'] == 1
        assert cand.location_id == skatetrax_uuid

        loc = db_session.query(Locations).filter_by(rink_id=skatetrax_uuid).first()
        assert loc is not None
        assert loc.rink_name == 'Springfield Ice'

    def test_mints_new_uuid_when_no_skatetrax_match(self, db_session, source, raw):
        """When Skatetrax doesn't know this rink, a fresh UUID is minted."""
        st_locs = [
            _fake_skatetrax_loc(
                'st-unrelated', 'Polar Iceplex',
                '200 OAK AVENUE', 'Raleigh', 'NC',
            )
        ]

        cand = Candidates(
            raw_entry_id=raw.id,
            name='Brand New Rink',
            street='999 ELM DRIVE',
            city='Anytown',
            state='OH',
            zip='44101',
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session, skatetrax_locations=st_locs)

        assert stats['adopted_skatetrax_uuid'] == 0
        assert stats['promoted_new'] == 1
        assert cand.location_id is not None
        assert cand.location_id != 'st-unrelated'

    def test_no_skatetrax_locations_mints_normally(self, db_session, source, raw):
        """With no Skatetrax list, promotion works exactly as before."""
        cand = Candidates(
            raw_entry_id=raw.id,
            name='Normal Rink',
            street='300 PINE ROAD',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session)

        assert stats['adopted_skatetrax_uuid'] == 0
        assert stats['promoted_new'] == 1
        assert cand.location_id is not None

    def test_adopted_uuid_already_in_locations_links_existing(
        self, db_session, source, raw,
    ):
        """If a Skatetrax UUID was already adopted in a prior run,
        the candidate links to the existing location rather than
        creating a duplicate."""
        skatetrax_uuid = 'bbbbbbbb-cccc-dddd-eeee-ffffffffffff'
        existing_loc = Locations(
            rink_id=skatetrax_uuid,
            rink_name='Already Here',
            rink_address='100 MAIN STREET',
            rink_city='Springfield',
            rink_state='IL',
            rink_country='US',
            rink_zip='62701',
            data_source='sk8stuff',
        )
        db_session.add(existing_loc)
        db_session.flush()

        st_locs = [
            _fake_skatetrax_loc(
                skatetrax_uuid, 'Already Here',
                '100 MAIN STREET', 'Springfield', 'IL',
            )
        ]

        raw2 = RawEntries(
            source_id=source.id,
            raw_name='Same Rink Again',
            raw_address='100 Main St, Springfield, IL',
            raw_fingerprint='fp_uuid_collision_test',
            parse_status='parsed',
        )
        db_session.add(raw2)
        db_session.flush()

        cand = Candidates(
            raw_entry_id=raw2.id,
            name='Already Here Rink',
            street='555 DIFFERENT STREET',
            city='Springfield',
            state='IL',
            zip='62701',
            verification_status='geocode_match',
        )
        db_session.add(cand)
        db_session.flush()

        stats = promote_verified(db_session, skatetrax_locations=st_locs)

        assert cand.location_id == skatetrax_uuid
        loc_count = (
            db_session.query(Locations)
            .filter_by(rink_id=skatetrax_uuid)
            .count()
        )
        assert loc_count == 1


_SAMPLE_API_RESPONSE = [
    {
        "rink_id": "f6255fc0-bb01-4f4a-a2a1-b991481dd1e1",
        "rink_name": "Cleland Ice Rink",
        "rink_address": "1606 Rock Merritt Avenue",
        "rink_city": "Fort Liberty",
        "rink_state": "NC",
        "rink_country": "US",
        "rink_zip": "28307",
        "data_source": "skatetrax",
    },
    {
        "rink_id": "b261166b-9e7c-4a96-ab06-ec630deb3321",
        "rink_name": "Off Ice",
        "rink_address": "-",
        "rink_city": "-",
        "rink_state": "-",
        "rink_country": "-",
        "rink_zip": "00000",
        "data_source": "skatetrax",
    },
]

API_URL = "https://api.skatetrax.com/api/v4/public/rinks"


class TestFetchSkatetraxApi:
    """Tests for the public API fetch used for UUID alignment."""

    @responses.activate
    def test_parses_api_response(self):
        responses.add(
            responses.GET, API_URL,
            json=_SAMPLE_API_RESPONSE, status=200,
        )
        locs = _fetch_skatetrax_api()
        assert len(locs) == 1
        assert locs[0].rink_id == "f6255fc0-bb01-4f4a-a2a1-b991481dd1e1"
        assert locs[0].rink_name == "Cleland Ice Rink"
        assert locs[0].rink_city == "Fort Liberty"

    @responses.activate
    def test_filters_out_placeholder_entries(self):
        responses.add(
            responses.GET, API_URL,
            json=_SAMPLE_API_RESPONSE, status=200,
        )
        locs = _fetch_skatetrax_api()
        names = [l.rink_name for l in locs]
        assert "Off Ice" not in names

    @responses.activate
    def test_api_error_returns_empty(self):
        responses.add(
            responses.GET, API_URL, status=500,
        )
        locs = _fetch_skatetrax_api()
        assert locs == []

    @responses.activate
    def test_api_timeout_returns_empty(self):
        responses.add(
            responses.GET, API_URL,
            body=requests.exceptions.Timeout("timeout"),
        )
        locs = _fetch_skatetrax_api()
        assert locs == []

    @responses.activate
    def test_api_result_usable_for_matching(self):
        """Verify the API-sourced objects work with _find_skatetrax_match."""
        responses.add(
            responses.GET, API_URL,
            json=_SAMPLE_API_RESPONSE, status=200,
        )
        locs = _fetch_skatetrax_api()
        match = _find_skatetrax_match(
            locs, 'Cleland Ice Rink',
            '1606 ROCK MERRITT AVENUE', 'Fort Liberty', 'NC',
        )
        assert match is not None
        assert match.rink_id == "f6255fc0-bb01-4f4a-a2a1-b991481dd1e1"
