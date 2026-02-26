import argparse
import csv
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(name)s  %(levelname)s  %(message)s'
)

logger = logging.getLogger(__name__)

from pipeline.runner import run_source, geocode_pending, repair_geocode_failed
from pipeline.promoter import run_promotion
from pipeline.ice_time_sync import sync_ice_time
from pipeline.skatetrax_push import push_locations


def export_csv(path):
    """Export the locations table to CSV."""
    from sqlalchemy import func
    from sqlalchemy.orm import Session
    from pipeline.staging import Locations, LocationSources, build_engine, init_db

    engine = build_engine()
    init_db(engine)

    with Session(engine) as session:
        rows = (
            session.query(
                Locations,
                func.count(LocationSources.id).label('source_count'),
            )
            .outerjoin(
                LocationSources,
                Locations.rink_id == LocationSources.location_id,
            )
            .group_by(Locations.rink_id)
            .order_by(Locations.rink_state, Locations.rink_city)
            .all()
        )

        fieldnames = [
            'rink_id', 'rink_name', 'rink_address', 'rink_city',
            'rink_state', 'rink_zip', 'rink_status', 'data_source',
            'source_count',
        ]

        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for loc, source_count in rows:
                writer.writerow({
                    'rink_id': loc.rink_id,
                    'rink_name': loc.rink_name,
                    'rink_address': loc.rink_address or '',
                    'rink_city': loc.rink_city,
                    'rink_state': loc.rink_state,
                    'rink_zip': loc.rink_zip,
                    'rink_status': loc.rink_status,
                    'data_source': loc.data_source,
                    'source_count': source_count,
                })

        logger.info("Exported %d locations to %s", len(rows), path)
        return len(rows)


def _run_all(args):
    """Run every enabled source, then geocode pending, then promote."""
    from sqlalchemy.orm import Session
    from pipeline.staging import Sources, build_engine, init_db

    engine = build_engine()
    init_db(engine)

    with Session(engine) as session:
        enabled = (
            session.query(Sources)
            .filter_by(enabled=True)
            .order_by(Sources.id)
            .all()
        )
        source_names = [s.name for s in enabled
                        if s.name != 'skatetrax']

    combined = {
        'sources_run': [],
        'total_scraped': 0,
        'total_new': 0,
        'total_parsed': 0,
    }

    for name in source_names:
        logger.info("=== Running source: %s ===", name)
        s = run_source(
            source_name=name,
            scrape_only=args.scrape_only,
            geocode=not args.no_geocode,
            limit=args.limit,
        )
        combined['sources_run'].append(name)
        combined['total_scraped'] += s.get('scraped', 0)
        combined['total_new'] += s.get('new', 0)
        combined['total_parsed'] += s.get('parsed', 0)

    logger.info("=== Geocoding remaining pending candidates ===")
    geo_stats = geocode_pending()
    combined['geocode_match'] = geo_stats.get('geocode_match', 0)
    combined['geocode_mismatch'] = geo_stats.get('geocode_mismatch', 0)
    combined['geocode_failed'] = geo_stats.get('geocode_failed', 0)

    logger.info("=== Promoting verified candidates ===")
    promo_stats = run_promotion()
    combined['locations_new'] = promo_stats.get('phase1_new_locations', 0)
    combined['locations_linked'] = promo_stats.get('phase1_linked_existing', 0)
    combined['total_locations'] = promo_stats.get('total_locations', 0)

    return combined


parser = argparse.ArgumentParser(description='Run the ice-maker pipeline')

mode = parser.add_mutually_exclusive_group()
mode.add_argument('--source', type=str,
                  help='Run full pipeline for a source: sk8stuff, arena_guide, '
                       'learntoskate, fandom_wiki, or "all"')
mode.add_argument('--geocode-pending', action='store_true',
                  help='Geocode existing unverified candidates (no scraping)')
mode.add_argument('--promote', action='store_true',
                  help='Promote verified candidates to the locations table')
mode.add_argument('--sync-ice-time', action='store_true',
                  help='Confirm rinks via Skatetrax ice_time table '
                       '(requires SKATETRAX_DB_URL)')
mode.add_argument('--repair-failed', action='store_true',
                  help='Re-parse geocode_failed candidates with fixed parser')
mode.add_argument('--push-to-skatetrax', action='store_true',
                  help='Push active locations into the Skatetrax DB '
                       '(requires SKATETRAX_DB_URL)')

parser.add_argument('--export-csv', type=str, metavar='PATH',
                    help='Export locations table to CSV at the given path '
                         '(can be combined with other operations)')
parser.add_argument('--geocode-source', type=str, default=None,
                    help='Limit --geocode-pending to a specific source')
parser.add_argument('--dry-run', action='store_true',
                    help='Preview changes without writing (for --push-to-skatetrax)')
parser.add_argument('--scrape-only', action='store_true',
                    help='Only scrape and fingerprint, skip parse/geocode')
parser.add_argument('--no-geocode', action='store_true',
                    help='Skip geocoding step')
parser.add_argument('--limit', type=int, default=None,
                    help='Max new entries to process (for testing)')

args = parser.parse_args()

has_mode = (
    args.source or args.geocode_pending or args.promote
    or args.sync_ice_time or args.repair_failed
    or args.push_to_skatetrax
)

if not has_mode and not args.export_csv:
    parser.error(
        'Provide a pipeline operation (--source, --geocode-pending, '
        '--promote, --sync-ice-time, --repair-failed) '
        'and/or --export-csv PATH'
    )

stats = {}

if has_mode:
    if args.geocode_pending:
        stats = geocode_pending(source_name=args.geocode_source)
    elif args.promote:
        stats = run_promotion()
    elif args.sync_ice_time:
        stats = sync_ice_time()
    elif args.repair_failed:
        stats = repair_geocode_failed()
    elif args.push_to_skatetrax:
        stats = push_locations(dry_run=args.dry_run)
    elif args.source == 'all':
        stats = _run_all(args)
    else:
        stats = run_source(
            source_name=args.source,
            scrape_only=args.scrape_only,
            geocode=not args.no_geocode,
            limit=args.limit,
        )

    print("\n=== Pipeline Results ===")
    for key, value in stats.items():
        print(f"  {key}: {value}")

if args.export_csv:
    count = export_csv(args.export_csv)
    print(f"\n=== Exported {count} locations to {args.export_csv} ===")
