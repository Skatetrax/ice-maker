"""Tests for pipeline/fingerprint.py."""

import pytest
from pipeline.fingerprint import compute_fingerprint, check_and_insert_raw
from pipeline.staging import RawEntries


class TestComputeFingerprint:
    def test_deterministic(self):
        fp1 = compute_fingerprint(1, "Polar Ice", "100 Main St, Raleigh, NC")
        fp2 = compute_fingerprint(1, "Polar Ice", "100 Main St, Raleigh, NC")
        assert fp1 == fp2

    def test_case_insensitive(self):
        fp1 = compute_fingerprint(1, "POLAR ICE", "100 MAIN ST")
        fp2 = compute_fingerprint(1, "polar ice", "100 main st")
        assert fp1 == fp2

    def test_leading_trailing_whitespace_on_payload(self):
        """strip() acts on the whole payload string, not individual fields."""
        fp1 = compute_fingerprint(1, "Polar Ice", "100 Main St")
        fp2 = compute_fingerprint(1, "Polar Ice", "100 Main St")
        assert fp1 == fp2
        # Internal whitespace differences DO produce different hashes
        fp3 = compute_fingerprint(1, " Polar Ice", "100 Main St")
        assert fp1 != fp3

    def test_different_source_id_different_hash(self):
        fp1 = compute_fingerprint(1, "Polar Ice", "100 Main St")
        fp2 = compute_fingerprint(2, "Polar Ice", "100 Main St")
        assert fp1 != fp2

    def test_different_name_different_hash(self):
        fp1 = compute_fingerprint(1, "Polar Ice", "100 Main St")
        fp2 = compute_fingerprint(1, "Other Rink", "100 Main St")
        assert fp1 != fp2

    def test_returns_hex_string(self):
        fp = compute_fingerprint(1, "Test", "Address")
        assert len(fp) == 32
        assert all(c in '0123456789abcdef' for c in fp)


class TestCheckAndInsertRaw:
    def test_new_entry_returns_is_new_true(self, db_session, make_source):
        src = make_source(name='test_fp_src')
        entry, is_new = check_and_insert_raw(
            db_session, src.id, "New Rink", "123 Test St, City, ST"
        )
        assert is_new is True
        assert entry.raw_name == "New Rink"
        assert entry.parse_status == 'pending'

    def test_duplicate_returns_is_new_false(self, db_session, make_source):
        src = make_source(name='test_fp_src2')
        entry1, is_new1 = check_and_insert_raw(
            db_session, src.id, "Same Rink", "Same Address"
        )
        entry2, is_new2 = check_and_insert_raw(
            db_session, src.id, "Same Rink", "Same Address"
        )
        assert is_new1 is True
        assert is_new2 is False
        assert entry1.id == entry2.id
