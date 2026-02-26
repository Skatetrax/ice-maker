import hashlib
import logging
from sqlalchemy.orm import Session
from pipeline.staging import RawEntries

logger = logging.getLogger(__name__)


def compute_fingerprint(source_id, raw_name, raw_address):
    """MD5 hash of source + name + address, lowercased and stripped."""
    payload = f"{source_id}|{raw_name}|{raw_address}".lower().strip()
    return hashlib.md5(payload.encode('utf-8')).hexdigest()


def check_and_insert_raw(session: Session, source_id, raw_name, raw_address):
    """Check if this entry already exists (by fingerprint).

    Returns:
        (RawEntries, is_new) -- the row and whether it was newly inserted.
    """
    fp = compute_fingerprint(source_id, raw_name, raw_address)

    existing = session.query(RawEntries).filter_by(raw_fingerprint=fp).first()

    if existing:
        logger.debug("Fingerprint exists, skipping: %s", raw_name)
        return existing, False

    entry = RawEntries(
        source_id=source_id,
        raw_name=raw_name,
        raw_address=raw_address,
        raw_fingerprint=fp,
        parse_status='pending'
    )
    session.add(entry)
    session.flush()

    return entry, True
