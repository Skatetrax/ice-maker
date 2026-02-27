#!/usr/bin/env python3
"""One-time bulk validation of ice-maker locations against Google Places API.

Requires a GOOGLE_PLACES_API_KEY environment variable (free-trial $300 credit
is more than enough for the full directory).

Usage
-----
    # Validate all active locations, write report CSV
    python validate_google.py --output validation_report.csv

    # Dry-run: print what would be queried without calling Google
    python validate_google.py --dry-run

    # Resume a previous run (skips rinks already in the output file)
    python validate_google.py --output validation_report.csv --resume

    # Limit to N rinks (useful for testing the key works)
    python validate_google.py --output validation_report.csv --limit 5

Pricing (Places API New, as of 2025)
-------------------------------------
    Text Search  – Advanced fields (phone, website): ~$0.040/req
    Time Zone API                                   : ~$0.005/req
    Total per rink                                  : ~$0.045
    Full directory (~2,100 rinks)                    : ~$95
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import requests
from sqlalchemy.orm import Session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TIMEZONE_URL = "https://maps.googleapis.com/maps/api/timezone/json"

FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.businessStatus",
    "places.formattedAddress",
    "places.types",
    "places.nationalPhoneNumber",
    "places.websiteUri",
    "places.location",
])

ICE_RELATED_TYPES = {
    "ice_skating_rink",
    "skating_rink",
    "sports_complex",
    "stadium",
    "arena",
    "sports_club",
    "fitness_center",
    "recreation_center",
}

REPORT_FIELDS = [
    "rink_id",
    "im_name",
    "im_address",
    "im_city",
    "im_state",
    "im_zip",
    "im_phone",
    "im_url",
    "im_tz",
    "google_place_id",
    "google_name",
    "google_address",
    "google_status",
    "google_types",
    "google_phone",
    "google_website",
    "google_lat",
    "google_lon",
    "google_tz",
    "name_similarity",
    "name_match",
    "flag_closed",
    "flag_not_ice",
    "flag_name_diff",
    "flag_no_result",
]


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _build_query(loc) -> str:
    """Build a search string from the location's name + address."""
    parts = [loc.rink_name]
    if loc.rink_address:
        parts.append(loc.rink_address)
    parts.append(loc.rink_city)
    parts.append(loc.rink_state)
    if loc.rink_zip:
        parts.append(loc.rink_zip)
    return ", ".join(parts)


def search_place(api_key: str, query: str, session: requests.Session):
    """Call Google Places Text Search and return the top result dict."""
    resp = session.post(
        PLACES_SEARCH_URL,
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        },
        json={"textQuery": query, "maxResultCount": 1},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    places = data.get("places", [])
    return places[0] if places else None


def get_timezone(api_key: str, lat: float, lng: float,
                 session: requests.Session) -> str | None:
    """Resolve lat/lng to an IANA timezone name via the Time Zone API."""
    ts = int(datetime.now(timezone.utc).timestamp())
    resp = session.get(
        TIMEZONE_URL,
        params={
            "location": f"{lat},{lng}",
            "timestamp": ts,
            "key": api_key,
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") == "OK":
        return data.get("timeZoneId")
    return None


def validate_location(api_key: str, loc, http: requests.Session) -> dict:
    """Validate a single location, returning a flat report row."""
    row = {
        "rink_id": loc.rink_id,
        "im_name": loc.rink_name,
        "im_address": loc.rink_address or "",
        "im_city": loc.rink_city,
        "im_state": loc.rink_state,
        "im_zip": loc.rink_zip or "",
        "im_phone": loc.rink_phone or "",
        "im_url": loc.rink_url or "",
        "im_tz": loc.rink_tz or "",
    }

    query = _build_query(loc)
    place = search_place(api_key, query, http)

    if place is None:
        row.update({
            "google_place_id": "",
            "google_name": "",
            "google_address": "",
            "google_status": "",
            "google_types": "",
            "google_phone": "",
            "google_website": "",
            "google_lat": "",
            "google_lon": "",
            "google_tz": "",
            "name_similarity": 0.0,
            "name_match": False,
            "flag_closed": False,
            "flag_not_ice": False,
            "flag_name_diff": False,
            "flag_no_result": True,
        })
        return row

    g_name = place.get("displayName", {}).get("text", "")
    g_status = place.get("businessStatus", "")
    g_types = place.get("types", [])
    g_phone = place.get("nationalPhoneNumber", "")
    g_website = place.get("websiteUri", "")
    g_addr = place.get("formattedAddress", "")
    g_loc = place.get("location", {})
    g_lat = g_loc.get("latitude", "")
    g_lng = g_loc.get("longitude", "")
    g_place_id = place.get("id", "")

    g_tz = ""
    if g_lat and g_lng:
        try:
            g_tz = get_timezone(api_key, g_lat, g_lng, http) or ""
        except Exception as exc:
            log.warning("Timezone lookup failed for %s: %s", loc.rink_id, exc)

    sim = _similarity(loc.rink_name, g_name) if g_name else 0.0
    is_ice = bool(ICE_RELATED_TYPES & set(g_types))

    row.update({
        "google_place_id": g_place_id,
        "google_name": g_name,
        "google_address": g_addr,
        "google_status": g_status,
        "google_types": "|".join(g_types),
        "google_phone": g_phone,
        "google_website": g_website,
        "google_lat": g_lat,
        "google_lon": g_lng,
        "google_tz": g_tz,
        "name_similarity": round(sim, 3),
        "name_match": sim >= 0.8,
        "flag_closed": g_status in (
            "CLOSED_PERMANENTLY", "CLOSED_TEMPORARILY"
        ),
        "flag_not_ice": not is_ice,
        "flag_name_diff": sim < 0.6,
        "flag_no_result": False,
    })
    return row


def load_locations(engine):
    from pipeline.staging import Locations
    with Session(engine) as sess:
        return (
            sess.query(Locations)
            .filter(Locations.rink_status == "active")
            .order_by(Locations.rink_state, Locations.rink_city)
            .all()
        )


def load_resume_ids(path: Path) -> set:
    """Return rink_ids already present in a partial output CSV."""
    ids = set()
    if not path.exists():
        return ids
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids.add(row["rink_id"])
    return ids


def main():
    parser = argparse.ArgumentParser(
        description="Validate ice-maker locations against Google Places API",
    )
    parser.add_argument(
        "--output", "-o", default="validation_report.csv",
        help="Path for the output CSV (default: validation_report.csv)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print queries that would be sent without calling Google",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Skip rinks already present in the output file",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Only validate the first N locations (0 = all)",
    )
    parser.add_argument(
        "--delay", type=float, default=0.25,
        help="Seconds between API calls (default 0.25)",
    )
    args = parser.parse_args()

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not api_key and not args.dry_run:
        print(
            "ERROR: GOOGLE_PLACES_API_KEY is not set.\n"
            "  Export it before running, e.g.:\n"
            "    export GOOGLE_PLACES_API_KEY=AIza...\n",
            file=sys.stderr,
        )
        sys.exit(1)

    from pipeline.staging import build_engine, init_db
    engine = build_engine()
    init_db(engine)

    locations = load_locations(engine)
    log.info("Loaded %d active locations from ice-maker DB", len(locations))

    skip_ids = set()
    if args.resume:
        skip_ids = load_resume_ids(Path(args.output))
        log.info("Resuming -- %d rinks already done, skipping", len(skip_ids))

    todo = [loc for loc in locations if loc.rink_id not in skip_ids]
    if args.limit:
        todo = todo[:args.limit]

    if args.dry_run:
        for loc in todo:
            print(f"  [{loc.rink_id[:8]}] {_build_query(loc)}")
        print(f"\nWould query {len(todo)} locations.")
        cost_est = len(todo) * 0.045
        print(f"Estimated cost: ${cost_est:.2f}")
        return

    log.info("Validating %d locations (delay=%.2fs)", len(todo), args.delay)
    cost_est = len(todo) * 0.045
    log.info("Estimated cost: $%.2f", cost_est)

    out_path = Path(args.output)
    write_header = not out_path.exists() or not args.resume
    http = requests.Session()

    with open(out_path, "a" if args.resume else "w",
              newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_FIELDS)
        if write_header:
            writer.writeheader()

        done = 0
        flags = {"closed": 0, "not_ice": 0, "name_diff": 0, "no_result": 0}

        for loc in todo:
            try:
                row = validate_location(api_key, loc, http)
                writer.writerow(row)
                f.flush()

                if row["flag_closed"]:
                    flags["closed"] += 1
                if row["flag_not_ice"]:
                    flags["not_ice"] += 1
                if row["flag_name_diff"]:
                    flags["name_diff"] += 1
                if row["flag_no_result"]:
                    flags["no_result"] += 1

                done += 1
                if done % 50 == 0:
                    log.info(
                        "Progress: %d/%d  (closed=%d  not_ice=%d  "
                        "name_diff=%d  no_result=%d)",
                        done, len(todo),
                        flags["closed"], flags["not_ice"],
                        flags["name_diff"], flags["no_result"],
                    )

            except Exception:
                log.exception("Failed on %s (%s)", loc.rink_id, loc.rink_name)

            time.sleep(args.delay)

    print(f"\n=== Validation Complete ===")
    print(f"  Locations checked : {done}")
    print(f"  Flagged closed    : {flags['closed']}")
    print(f"  Flagged not-ice   : {flags['not_ice']}")
    print(f"  Flagged name diff : {flags['name_diff']}")
    print(f"  No Google result  : {flags['no_result']}")
    print(f"  Report written to : {out_path}")


if __name__ == "__main__":
    main()
